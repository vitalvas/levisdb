// The engine assembles the storage core: an in-memory write buffer that
// flushes to on-disk SSTables, and the read path that merges them. Compaction
// and the WAL live above it.

package levisdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// tableMeta is a live on-disk table and its tier depth. handle is the pooled
// descriptor the reader uses; path lets it be removed on compaction.
type tableMeta struct {
	num    uint32
	depth  int // tier depth; 0 is freshest
	handle *openFile
	path   string
	reader *tableReader
	size   int64
	// minKey/maxKey bound the USER keys in this table (inclusive). Recorded in
	// the manifest so a read can skip a table whose range excludes the lookup key
	// before probing its bloom. nil bounds mean "unknown" (treated as covering
	// everything, so the table is never wrongly skipped).
	minKey []byte
	maxKey []byte
	// entries/tombstones count the table's entries and how many are deletes, so
	// the picker can trigger compaction on tombstone density. Recorded in the
	// manifest; zero entries means unknown (older tables), treated as no pressure.
	entries    int
	tombstones int

	lifeMu       sync.Mutex
	refs         int
	deleteOnZero bool
}

// mayContain reports whether key could fall within this table's recorded key
// range. Unknown (nil) bounds conservatively return true.
func (t *tableMeta) mayContain(key []byte) bool {
	if t.minKey != nil && bytes.Compare(key, t.minKey) < 0 {
		return false
	}
	if t.maxKey != nil && bytes.Compare(key, t.maxKey) > 0 {
		return false
	}
	return true
}

// overlapsRange reports whether this table's key range intersects [start, end].
// Either bound may be nil (unbounded on that side). Unknown table bounds
// conservatively return true.
func (t *tableMeta) overlapsRange(start, end []byte) bool {
	if end != nil && t.minKey != nil && bytes.Compare(t.minKey, end) > 0 {
		return false
	}
	if start != nil && t.maxKey != nil && bytes.Compare(t.maxKey, start) < 0 {
		return false
	}
	return true
}

func (t *tableMeta) acquire() bool {
	t.lifeMu.Lock()
	defer t.lifeMu.Unlock()
	if t.refs == 0 {
		return false
	}
	t.refs++
	return true
}

func (t *tableMeta) releaseRef() error { return t.release(false) }

func (t *tableMeta) releaseOwner(remove bool) error { return t.release(remove) }

func (t *tableMeta) release(remove bool) error {
	t.lifeMu.Lock()
	if remove {
		t.deleteOnZero = true
	}
	if t.refs > 0 {
		t.refs--
	}
	final := t.refs == 0
	deleteFile := t.deleteOnZero
	t.lifeMu.Unlock()
	if !final {
		return nil
	}
	err := t.handle.close()
	if deleteFile {
		if rerr := removeFileDurable(t.path); err == nil {
			err = rerr
		}
	}
	return err
}

// FileAllocator hands out globally-unique, monotonic file numbers.
type fileAllocator interface {
	Next() uint32
}

// Config holds the engine tuning derived from the database options.
type engineConfigT struct {
	MemtableSize int64
	BloomBits    int
	BlockSize    int
	// MaxLevels is the tier count (1..8); the deepest tier (MaxLevels - 1) merges
	// in place. Zero falls back to the maxTierDepth default for direct engine users
	// that leave it unset.
	MaxLevels      uint8
	FreshCodecName string
	LevelCodecs    []string
	Cache          *blockCacheT // shared block cache; nil disables caching
	FDs            *fdPool      // shared open-descriptor pool; nil keeps files open
	// Commit durably records a table-set replacement, invokes install while the
	// manifest transaction is serialized, and returns only after the edit is
	// durable. Tests and recovery leave it nil and install directly.
	Commit func(inputs, outputs []*tableMeta, install func()) error

	// LazyTableMetadata defers parsing a restored table's bloom and index to its
	// first read instead of at open, so a store with very many tables opens without
	// reading every table's metadata. Only consulted for tables opened during restore.
	LazyTableMetadata bool

	// ValueThreshold enables value separation when > 0: a flushed value at least this
	// many bytes is written to a blob file and the table stores a pointer. Zero (the
	// default) keeps every value inline, so the whole blob machinery stays dormant.
	ValueThreshold int64
	// BlobFileSize is the blob-file roll size; only used when ValueThreshold > 0.
	BlobFileSize int64
	// blobPath returns the on-disk path for a blob file number. Nil when value
	// separation is disabled.
	blobPath func(num uint32) (string, error)
	// CommitBlobs durably records newly written blob files in the manifest before a
	// flush/compaction install makes the referencing tables visible. Nil during
	// recovery and for direct engine users, which install without a manifest.
	CommitBlobs func(added []blobFileMeta) error
}

// valueSeparationOn reports whether this engine separates large values into blob
// files. All blob code paths gate on this so a zero ValueThreshold is fully inert.
func (s *engineT) valueSeparationOn() bool {
	return s.cfg.ValueThreshold > 0 && s.cfg.blobPath != nil
}

// Engine is the storage core. Writes go to the active memtable; when it fills
// it becomes immutable and is flushed to a fresh L0 table. Reads consult the
// active memtable, then the flushing memtable, then every overlapping table by
// visible sequence.
type engineT struct {
	cfg   engineConfigT
	alloc fileAllocator
	// tablePath returns the on-disk path for a table number.
	tablePath func(num uint32) (string, error)

	mu  sync.RWMutex
	mem *memtableT
	imm *memtableT // immutable, being flushed; nil when none
	// recoveryMems are sealed, in-memory tables created only while replaying a
	// crash WAL in read-only mode. They bound each skiplist arena without writing
	// recovery SSTables into a database that promised not to mutate.
	recoveryMems []*memtableT
	tables       []*tableMeta // file-creation order; compaction means this is not version order
	// index is a per-level sorted view of tables for O(log n) point-lookup candidate
	// selection; rebuilt from tables under mu at every mutation. Never nil after the
	// first setTables; guarded by mu like tables.
	index *levelIndex
	seed  int64

	// blobHandles caches an open fd-pool handle per blob file number for the read
	// path, so resolving a blob pointer does not reopen the file each time. Guarded by
	// blobMu. Empty and unused when value separation is off.
	blobMu      sync.Mutex
	blobHandles map[uint32]*blobReader

	// flushMu serializes Flush and Compact so at most one runs at a time; the
	// finer mu guards the fields those operations read and swap.
	flushMu sync.Mutex
	// lastCompaction records the most recent Compact outcome for logging. It is
	// written under flushMu and read by the caller right after Compact returns on
	// the same goroutine, so it needs no separate guard.
	lastCompaction compactionResult

	// Read-amplification counters, cumulative since open. pointReads is the number
	// of point lookups (Get and Has); tableProbes is the number of tables whose
	// bloom filter was consulted across all those lookups (after the cheap key-range
	// gate). Their ratio is the structural read fan-out, which predicts per-lookup
	// seek cost independently of the device or cache state. bloomFalsePositives
	// counts probes where the bloom admitted the key (so a block was read) but the
	// table did not hold it -- wasted read I/O, the signal for bloom/cache tuning.
	pointReads          atomic.Int64
	tableProbes         atomic.Int64
	bloomFalsePositives atomic.Int64
	// userBytesWritten is the cumulative key+value bytes applied by writes, the
	// write-amplification denominator (disk bytes written / this). Space
	// amplification is reported as on-disk table bytes (Stats.TablesSize) against the
	// live logical volume the caller tracks, since the engine does not keep an exact
	// running live-byte total across version-collapsing compactions.
	userBytesWritten atomic.Int64
}

// engineSeed fixes the memtable skiplist RNG so memtable layout is
// deterministic across runs.
const engineSeed int64 = 1

// newEngine creates an empty engine.
func newEngine(cfg engineConfigT, alloc fileAllocator, tablePath func(uint32) (string, error)) *engineT {
	if cfg.FDs == nil {
		// Default to an unbounded pool so callers that do not share one (tests)
		// still get working table handles.
		cfg.FDs = newFDPool(-1)
	}
	return &engineT{
		cfg:       cfg,
		alloc:     alloc,
		tablePath: tablePath,
		mem:       newMemtable(engineSeed),
		seed:      engineSeed,
		// Non-nil so a read before the first setTables (empty DB) has a valid, empty
		// index to visit rather than a nil deref.
		index: buildLevelIndex(nil),
	}
}

// setTables replaces the live table set and rebuilds the level index in lockstep.
// The caller must hold s.mu for writing. Every mutation of s.tables goes through
// here so the index never drifts from the authoritative slice.
func (s *engineT) setTables(tables []*tableMeta) {
	s.tables = tables
	s.index = buildLevelIndex(tables)
}

// rebuildIndex rebuilds the level index from the current tables. restoreTables
// appends many tables raw (to avoid an O(n^2) rebuild per table) and calls this
// once at the end. The caller must hold s.mu for writing.
func (s *engineT) rebuildIndex() {
	s.index = buildLevelIndex(s.tables)
}

// Put buffers a set into the active memtable at seq.
func (s *engineT) Put(seq uint64, key, value []byte) {
	s.mu.Lock()
	s.mem.Put(seq, key, value)
	s.mu.Unlock()
}

// putTTL buffers a set with an absolute Unix-nanosecond expiration.
func (s *engineT) putTTL(seq uint64, key, value []byte, expiresAt int64) {
	s.mu.Lock()
	s.mem.putTTL(seq, key, value, expiresAt)
	s.mu.Unlock()
}

// Delete buffers a tombstone into the active memtable at seq.
func (s *engineT) del(seq uint64, key []byte) {
	s.mu.Lock()
	s.mem.del(seq, key)
	s.mu.Unlock()
}

// delRange buffers a range tombstone for [start, end) into the active memtable.
func (s *engineT) delRange(seq uint64, start, end []byte) {
	s.mu.Lock()
	s.mem.delRange(seq, start, end)
	s.mu.Unlock()
}

// MemEmpty reports whether the active memtable holds no entries.
func (s *engineT) memEmpty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.empty()
}

// memSize returns the active memtable's approximate encoded size, for flush
// logging.
func (s *engineT) memSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.Size()
}

// memCount returns the active memtable's buffered entry count, for flush logging.
func (s *engineT) memCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.Len()
}

// NeedFlush reports whether the active memtable has reached the flush
// threshold and no flush is already in progress.
func (s *engineT) needFlush() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.imm == nil && s.mem.Size() >= s.cfg.MemtableSize
}

// depth0Count returns the number of fresh-tier (depth 0) tables, the backlog
// signal write backpressure throttles on.
func (s *engineT) depth0Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, t := range s.tables {
		if t.depth == 0 {
			n++
		}
	}
	return n
}

// maxPopulatedDepth returns the deepest tier that currently holds a table, or 0
// when the engine is empty. A per-tier full compaction cascades down to this tier
// and collapses it in place, so it never rewrites data below where it already
// lives.
func (s *engineT) maxPopulatedDepth() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	deepest := 0
	for _, t := range s.tables {
		if t.depth > deepest {
			deepest = t.depth
		}
	}
	return deepest
}

// tierTableCount returns the number of live tables at the given tier depth. Used
// by the flush/compaction logging to report input/output sizes.
func (s *engineT) tierTableCount(depth int) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, t := range s.tables {
		if t.depth == depth {
			n++
		}
	}
	return n
}

// tierTableStats returns the count and total on-disk bytes of live tables at the
// given tier depth, for compaction logging.
func (s *engineT) tierTableStats(depth int) (count int, bytes int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, t := range s.tables {
		if t.depth == depth {
			count++
			bytes += t.size
		}
	}
	return count, bytes
}

// setLastCompaction records a compaction outcome. Called from Compact under
// flushMu.
func (s *engineT) setLastCompaction(res compactionResult) { s.lastCompaction = res }

// takeLastCompaction returns the most recent compaction outcome, for the caller
// to log after Compact returns.
func (s *engineT) takeLastCompaction() compactionResult { return s.lastCompaction }

// newestTable returns the number, entry count, and size of the most recently
// created live table, for flush logging (the table just written). It returns
// zeros when no table exists.
func (s *engineT) newestTable() (num uint32, entries int, size int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.tables) == 0 {
		return 0, 0, 0
	}
	t := s.tables[len(s.tables)-1]
	return t.num, t.entries, t.size
}

// levelSummary renders the live tables as a per-tier histogram so a log reader
// sees the whole ladder after an operation. Each tier is labelled "L<depth>:
// <count>" (e.g. "L0:1 L1:2 L2:2") rather than LevelDB's bare positional
// "files[ 1 2 2 ]", so the numbers are self-describing. Every tier from 0 to the
// deepest with a live table is reported, so a gap reads as 0.
func (s *engineT) levelSummary() string {
	s.mu.RLock()
	counts := make(map[int]int)
	maxDepth := 0
	for _, t := range s.tables {
		counts[t.depth]++
		if t.depth > maxDepth {
			maxDepth = t.depth
		}
	}
	s.mu.RUnlock()
	var b strings.Builder
	for d := 0; d <= maxDepth; d++ {
		if d > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "L%d:%d", d, counts[d])
	}
	return b.String()
}

// Get resolves key at snapshot seq, returning the value or that it is absent or
// deleted. It reads memtable, then the flushing memtable, then tables.
func (s *engineT) get(seq uint64, key []byte) (value []byte, found, deleted bool, err error) {
	return s.getAtTime(seq, key, time.Now().Unix())
}

// versionPick tracks the newest visible version of a key seen so far while a
// lookup scans the memtable, immutable memtable, recovery memtables, and tables.
type versionPick struct {
	value   []byte
	seq     uint64
	found   bool
	deleted bool
	// expiresAt is the winning version's absolute TTL deadline (Unix seconds), 0 for
	// a non-TTL value. Carried so Deduplication can compare it exactly without a
	// second lookup.
	expiresAt int64
	// blobPtr is non-nil when the winning version's value lives in a blob file; value
	// is then nil until the caller resolves the pointer. Only table candidates set it
	// (memtable values are always inline).
	blobPtr *blobPointer
}

// set records the first version found (no prior best to compare against).
func (p *versionPick) set(v []byte, seq uint64, deleted bool, expiresAt int64) {
	p.value, p.seq, p.found, p.deleted, p.expiresAt, p.blobPtr = v, seq, true, deleted, expiresAt, nil
}

// setBlob records a first version whose value is out-of-line in a blob file.
func (p *versionPick) setBlob(ptr *blobPointer, seq uint64, expiresAt int64) {
	p.value, p.seq, p.found, p.deleted, p.expiresAt, p.blobPtr = nil, seq, true, false, expiresAt, ptr
}

// merge folds one candidate version into the best. A higher sequence wins; the
// same sequence with a different kind or value is an impossible-under-unique-seqs
// anomaly and returns a conflict error tagged with source. Ties with an identical
// version are ignored (the same record read from more than one place).
func (p *versionPick) merge(v []byte, seq uint64, deleted bool, expiresAt int64, source string) error {
	switch {
	case !p.found || seq > p.seq:
		p.set(v, seq, deleted, expiresAt)
	case seq == p.seq && (deleted != p.deleted || (!deleted && !bytes.Equal(v, p.value))):
		return fmt.Errorf("%s: conflicting versions for key at sequence %d", source, seq)
	}
	return nil
}

// mergeBlob folds a table candidate whose value is out-of-line. A higher sequence
// wins and records the pointer. Unique sequences mean a blob version never ties a
// same-seq inline version, so no value comparison is needed.
func (p *versionPick) mergeBlob(ptr *blobPointer, seq uint64, expiresAt int64) {
	if !p.found || seq > p.seq {
		p.setBlob(ptr, seq, expiresAt)
	}
}

// pickVersion scans the memtable, flushing memtable, recovery memtables, and
// every overlapping table, folding each candidate into the newest visible version
// at seq (expiry evaluated against now). The caller holds s.mu.
func (s *engineT) pickVersion(seq uint64, key []byte, now int64) (versionPick, error) {
	var best versionPick
	if v, vseq, f, d, exp := s.mem.getVersionAt(seq, key, now); f {
		best.set(v, vseq, d, exp)
	}
	if s.imm != nil {
		if v, vseq, f, d, exp := s.imm.getVersionAt(seq, key, now); f {
			if err := best.merge(v, vseq, d, exp, "memtable"); err != nil {
				return versionPick{}, err
			}
		}
	}
	for _, recovered := range s.recoveryMems {
		if v, vseq, f, d, exp := recovered.getVersionAt(seq, key, now); f {
			if err := best.merge(v, vseq, d, exp, "recovery memtable"); err != nil {
				return versionPick{}, err
			}
		}
	}
	// Table creation order is not version order: compacting an old tier creates
	// a new file after newer shallow-tier tables. Inspect every overlapping table
	// and select the greatest visible sequence rather than returning the newest
	// file's first match.
	// The level index yields only the tables whose recorded key range can contain
	// key (all of L0, plus the single disjoint candidate per sorted level), so this
	// is O(log n) per level rather than a scan of every table.
	probes, bloomFP := int64(0), int64(0)
	var mergeErr error
	visitErr := s.index.candidates(key, func(table *tableMeta) error {
		probes++
		r, gerr := table.reader.lookupAtTime(key, seq, now, false)
		if gerr != nil {
			return gerr
		}
		if r.bloomPassed && !r.found {
			bloomFP++ // bloom admitted the key, the block read found nothing: wasted I/O
		}
		if !r.found {
			return nil
		}
		if r.blobPtr != nil {
			best.mergeBlob(r.blobPtr, r.versionSeq, r.expiresAt)
			return nil
		}
		if err := best.merge(r.value, r.versionSeq, r.deleted, r.expiresAt, "table"); err != nil {
			mergeErr = err
			return err
		}
		return nil
	})
	s.pointReads.Add(1)
	s.tableProbes.Add(probes)
	s.bloomFalsePositives.Add(bloomFP)
	if mergeErr != nil {
		return versionPick{}, mergeErr
	}
	if visitErr != nil {
		return versionPick{}, visitErr
	}
	return best, nil
}

// getWithExpiry resolves key at seq and also reports the stored absolute TTL
// deadline (0 for a non-TTL value). It backs Get (which surfaces ExpiresAt) and
// Deduplication. A range tombstone newer than the point version shadows the key,
// exactly as getAtTime does, so a range-deleted key reads as absent.
func (s *engineT) getWithExpiry(seq uint64, key []byte) (value []byte, expiresAt int64, found bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	best, err := s.pickVersion(seq, key, time.Now().Unix())
	if err != nil {
		return nil, 0, false, err
	}
	if !best.found || best.deleted {
		return nil, 0, false, nil
	}
	// A range tombstone visible at seq and newer than the point version deletes the
	// key even when the point version itself is live.
	rdSeq, rderr := s.maxCoveringRangeDelSeq(key, seq)
	if rderr != nil {
		return nil, 0, false, rderr
	}
	if rdSeq > best.seq {
		return nil, 0, false, nil
	}
	// An out-of-line value is fetched from its blob file now that it is the chosen
	// version; the returned bytes are a fresh owned copy.
	if best.blobPtr != nil {
		v, berr := s.resolveBlob(*best.blobPtr)
		if berr != nil {
			return nil, 0, false, berr
		}
		return v, best.expiresAt, true, nil
	}
	// Copy out: best.value may alias a cached block or memtable buffer the caller
	// must not retain or mutate.
	return append([]byte(nil), best.value...), best.expiresAt, true, nil
}

func (s *engineT) getAtTime(seq uint64, key []byte, now int64) (value []byte, found, deleted bool, err error) {
	// Hold the read lock for the whole lookup so a concurrent compaction cannot
	// close and remove a table file mid-read; the compaction swap runs under
	// the write lock.
	s.mu.RLock()
	defer s.mu.RUnlock()

	best, err := s.pickVersion(seq, key, now)
	if err != nil {
		return nil, false, false, err
	}
	bestValue, bestSeq, bestFound, bestDeleted := best.value, best.seq, best.found, best.deleted
	// A range tombstone visible at seq and newer than the best point version
	// deletes the key, even when the point version itself is live. Apply it after
	// the point lookup so it can shadow a value in any source.
	if bestFound && !bestDeleted {
		rdSeq, rderr := s.maxCoveringRangeDelSeq(key, seq)
		if rderr != nil {
			return nil, false, false, rderr
		}
		if rdSeq > bestSeq {
			bestDeleted = true
			bestValue = nil
		}
	}
	if bestFound {
		// Resolve an out-of-line value from its blob file once chosen and not shadowed
		// by a range tombstone. A deleted result has no value to fetch.
		if !bestDeleted && best.blobPtr != nil {
			v, berr := s.resolveBlob(*best.blobPtr)
			if berr != nil {
				return nil, false, false, berr
			}
			return v, true, false, nil
		}
		return append([]byte(nil), bestValue...), true, bestDeleted, nil
	}
	return nil, false, false, nil
}

// maxCoveringRangeDelSeq returns the highest sequence of any range tombstone that
// covers key and is visible at readSeq (seq <= readSeq), across the memtable, the
// flushing memtable, the recovery memtables, and every table. Zero means none.
// Caller holds s.mu.
func (s *engineT) maxCoveringRangeDelSeq(key []byte, readSeq uint64) (uint64, error) {
	var best uint64
	consider := func(rts []rangeTombstone) {
		for i := range rts {
			if rts[i].seq <= readSeq && rts[i].seq > best && rts[i].contains(key) {
				best = rts[i].seq
			}
		}
	}
	consider(s.mem.rangeTombstones())
	if s.imm != nil {
		consider(s.imm.rangeTombstones())
	}
	for _, recovered := range s.recoveryMems {
		consider(recovered.rangeTombstones())
	}
	for _, table := range s.tables {
		if !table.mayContain(key) {
			continue
		}
		rts, err := table.reader.rangeTombstones()
		if err != nil {
			return 0, err
		}
		consider(rts)
	}
	return best, nil
}

// has reports whether key exists at snapshot seq, and whether the newest
// visible version is a tombstone, without copying the value.
func (s *engineT) has(seq uint64, key []byte) (found, deleted bool, err error) {
	return s.hasAtTime(seq, key, time.Now().Unix())
}

func (s *engineT) hasAtTime(seq uint64, key []byte, now int64) (found, deleted bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var bestSeq uint64
	var bestFound, bestDeleted bool
	if _, vseq, f, d, _ := s.mem.getVersionAt(seq, key, now); f {
		bestSeq, bestFound, bestDeleted = vseq, true, d
	}
	if s.imm != nil {
		if _, vseq, f, d, _ := s.imm.getVersionAt(seq, key, now); f {
			switch {
			case !bestFound || vseq > bestSeq:
				bestSeq, bestFound, bestDeleted = vseq, true, d
			case vseq == bestSeq && d != bestDeleted:
				return false, false, fmt.Errorf("memtable: conflicting kinds for key at sequence %d", vseq)
			}
		}
	}
	for _, recovered := range s.recoveryMems {
		if _, vseq, f, d, _ := recovered.getVersionAt(seq, key, now); f {
			switch {
			case !bestFound || vseq > bestSeq:
				bestSeq, bestFound, bestDeleted = vseq, true, d
			case vseq == bestSeq && d != bestDeleted:
				return false, false, fmt.Errorf("recovery memtable: conflicting kinds for key at sequence %d", vseq)
			}
		}
	}
	// Only the tables whose key range can contain key, via the level index.
	probes, bloomFP := int64(0), int64(0)
	var conflictErr error
	visitErr := s.index.candidates(key, func(table *tableMeta) error {
		probes++
		vseq, f, d, bloomPassed, gerr := table.reader.hasVersionAtTime(key, seq, now)
		if gerr != nil {
			return gerr
		}
		if bloomPassed && !f {
			bloomFP++
		}
		if !f {
			return nil
		}
		if !bestFound || vseq > bestSeq {
			bestSeq, bestFound, bestDeleted = vseq, true, d
			return nil
		}
		if vseq == bestSeq && d != bestDeleted {
			conflictErr = fmt.Errorf("table: conflicting kinds for key at sequence %d", vseq)
			return conflictErr
		}
		return nil
	})
	s.pointReads.Add(1)
	s.tableProbes.Add(probes)
	s.bloomFalsePositives.Add(bloomFP)
	if conflictErr != nil {
		return false, false, conflictErr
	}
	if visitErr != nil {
		return false, false, visitErr
	}
	// A range tombstone newer than the best point version deletes the key.
	if bestFound && !bestDeleted {
		rdSeq, rderr := s.maxCoveringRangeDelSeq(key, seq)
		if rderr != nil {
			return false, false, rderr
		}
		if rdSeq > bestSeq {
			bestDeleted = true
		}
	}
	return bestFound, bestDeleted, nil
}

// tableFault reports a table that failed integrity verification.
type tableFault struct {
	num uint32
	err error
}

// verify reads and validates every block of every live table, returning a fault
// per table that failed its CRC/codec/metadata checks (empty when all intact).
// Each table is ref-held during its scan so a concurrent compaction cannot free
// the file mid-read; the engine lock is released for the I/O so verification does
// not block reads or writes.
func (s *engineT) verify() []tableFault {
	s.mu.RLock()
	held := make([]*tableMeta, 0, len(s.tables))
	for _, t := range s.tables {
		if t.acquire() {
			held = append(held, t)
		}
	}
	s.mu.RUnlock()

	var faults []tableFault
	for _, t := range held {
		if err := t.reader.verify(); err != nil {
			faults = append(faults, tableFault{num: t.num, err: err})
		}
		_ = t.releaseRef()
	}
	return faults
}

// rotate seals the active memtable as immutable and installs a fresh active
// one, so writes continue while the sealed table is flushed. It reports whether
// there is anything to flush.
func (s *engineT) rotate() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.imm != nil || s.mem.empty() {
		return s.imm != nil
	}
	s.imm = s.mem
	s.mem = newMemtable(s.seed)
	return true
}

// sealReadOnlyRecoveryMemtable moves the active recovery memtable into an
// immutable in-memory list and installs a fresh one. Read-only WAL replay uses
// this instead of Flush so it can bound arena growth without creating files.
func (s *engineT) sealReadOnlyRecoveryMemtable() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mem.empty() {
		return
	}
	s.recoveryMems = append(s.recoveryMems, s.mem)
	s.mem = newMemtable(s.seed)
}

// Flush seals the active memtable (if needed) and writes the immutable one to a
// fresh L0 SSTable. It is called by the flush worker; only one flush runs at a
// time.
func (s *engineT) Flush() error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	if !s.rotate() {
		return nil
	}
	s.mu.RLock()
	imm := s.imm
	s.mu.RUnlock()

	num := s.alloc.Next()
	if num == 0 {
		return ErrFileNumberExhausted
	}
	path, err := s.tablePath(num)
	if err != nil {
		return err
	}
	// When value separation is on, large values in this flush are diverted to blob
	// files via bw; smaller values stay inline. bw is nil when the feature is off.
	var bw *blobWriter
	if s.valueSeparationOn() {
		bw = newBlobWriter(s.alloc, s.cfg.blobPath, s.cfg.BlobFileSize)
	}
	meta, err := s.writeTable(tableWriteJob{
		num:  num,
		path: path,
		it:   imm.newIterator(),
		rts:  imm.rangeTombstones(),
		bw:   bw,
	})
	if err != nil {
		if bw != nil {
			bw.abandon()
		}
		return err
	}
	// Seal the blob files this flush produced and record them durably in the manifest
	// BEFORE the table that points into them is committed, so a crash never leaves a
	// table pointer to an unrecorded or non-durable blob.
	var blobs []blobFileMeta
	if bw != nil {
		blobs, err = bw.close()
		if err != nil {
			_ = meta.releaseOwner(false)
			return err
		}
		if len(blobs) > 0 && s.cfg.CommitBlobs != nil {
			if err := s.cfg.CommitBlobs(blobs); err != nil {
				// Blob files are synced; leave them for orphan cleanup rather than
				// deleting, so recovery never races a half-removed referenced blob.
				_ = meta.releaseOwner(false)
				return err
			}
		}
	}

	install := func() {
		s.mu.Lock()
		s.setTables(append(s.tables, meta))
		s.imm = nil
		s.mu.Unlock()
	}
	if s.cfg.Commit != nil {
		if err := s.cfg.Commit(nil, []*tableMeta{meta}, install); err != nil {
			// A manifest append error is ambiguous: the complete edit may be
			// readable even when its final Sync reported failure. Preserve the
			// output so recovery never sees a manifest reference to a deleted file;
			// if the edit did not land, writable-open orphan cleanup removes it.
			_ = meta.releaseOwner(false)
			return err
		}
		return nil
	}
	install()
	return nil
}

// maybeSeparateValue diverts a large value to a blob file, returning the internal
// key and value to actually write into the table. When value separation is off (bw
// nil) or the value is too small or not a plain/TTL set, it returns the inputs
// unchanged. Otherwise it appends the raw value to the blob file and returns a
// rewritten internal key of kind ikeyKindBlobRef whose value is the encoded pointer.
// A TTL value's raw bytes (expiry prefix stripped) go to the blob; the deadline is
// carried in the pointer, so a read reports it without fetching the blob.
func (s *engineT) maybeSeparateValue(bw *blobWriter, ik, value []byte) ([]byte, []byte, error) {
	if bw == nil {
		return ik, value, nil
	}
	seq, kind := ikeySeqKind(ik)
	var raw []byte
	var expiresAt int64
	switch kind {
	case ikeyKindSet:
		raw = value
	case ikeyKindSetTTL:
		decoded, exp, err := decodeExpiringValue(value)
		if err != nil {
			return nil, nil, err
		}
		raw, expiresAt = decoded, exp
	default:
		return ik, value, nil // tombstone or already-separated: never diverted
	}
	if int64(len(raw)) < s.cfg.ValueThreshold {
		return ik, value, nil // small value stays inline
	}
	ptr, err := bw.append(raw, expiresAt)
	if err != nil {
		return nil, nil, err
	}
	user := ikeyUserKey(ik)
	newIK := ikeyEncode(nil, user, seq, ikeyKindBlobRef)
	return newIK, encodeBlobPointer(nil, ptr), nil
}

// tableWriteJob bundles the inputs for writeTable (keeps its signature within the
// parameter limit). bw is nil unless value separation is on.
type tableWriteJob struct {
	num   uint32
	depth int
	path  string
	it    *memtableIterator
	rts   []rangeTombstone
	bw    *blobWriter
}

// writeTable writes all entries from a memtable iterator into a new table file
// at the given tier depth, persists any range tombstones, and opens it for
// reading.
func (s *engineT) writeTable(j tableWriteJob) (*tableMeta, error) {
	num, depth, path, it, rts, bw := j.num, j.depth, j.path, j.it, j.rts, j.bw
	// O_EXCL: the number came from the allocator, so this file must not pre-exist.
	// If it does, a reused file number is about to overwrite live data; fail loudly
	// instead of silently truncating it.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = f.Close()
			_ = removeFileDurable(path)
		}
	}()
	// A flush writes a fresh L0 table, never the bottom tier, so it uses the
	// per-level override for this depth or the fresh fallback.
	codecName := resolveLevelCodec(s.cfg.LevelCodecs, depth, s.cfg.FreshCodecName, "", false)
	c, err := codecFromName(codecName)
	if err != nil {
		return nil, err
	}
	w := newTableWriter(f, tableWriterConfig{
		codec:     c,
		bloomBits: s.cfg.BloomBits,
		blockSize: s.cfg.BlockSize,
	})
	w.setRangeTombstones(rts)
	for it.Next() {
		ik, value, aerr := s.maybeSeparateValue(bw, it.internalKey(), it.Value())
		if aerr != nil {
			return nil, aerr
		}
		if err := w.Add(ik, value); err != nil {
			return nil, err
		}
	}
	size, err := w.finish()
	if err != nil {
		return nil, err
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}
	// Close the write descriptor; reads go through the pooled read handle so
	// the descriptor is bounded and reopened on demand.
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	// The recorded key bounds must span both point entries and range tombstones,
	// so a read for a key that only a range tombstone covers does not skip this
	// table via mayContain. A range tombstone's end is exclusive, but using it as
	// an inclusive upper bound is safe (over-inclusive never wrongly skips).
	minKey, maxKey := widenBoundsForRangeDels(w.minUserKey(), w.maxUserKey(), rts)
	meta, err := s.openTableMeta(tableSpec{
		num:        num,
		depth:      depth,
		path:       path,
		size:       size,
		minKey:     minKey,
		maxKey:     maxKey,
		entries:    w.entryCount(),
		tombstones: w.tombstoneCount(),
	})
	if err != nil {
		return nil, err
	}
	keep = true
	return meta, nil
}

// widenBoundsForRangeDels expands [minKey, maxKey] to also cover every range
// tombstone's [start, end], so a table's recorded bounds include keys that only
// a range tombstone touches. nil point bounds (a range-del-only table) take the
// tombstone span directly.
func widenBoundsForRangeDels(minKey, maxKey []byte, rts []rangeTombstone) ([]byte, []byte) {
	for i := range rts {
		if minKey == nil || bytes.Compare(rts[i].start, minKey) < 0 {
			minKey = rts[i].start
		}
		if maxKey == nil || bytes.Compare(rts[i].end, maxKey) > 0 {
			maxKey = rts[i].end
		}
	}
	// Return owned copies so later reuse of the tombstone buffers cannot mutate the
	// recorded bounds.
	if minKey != nil {
		minKey = append([]byte(nil), minKey...)
	}
	if maxKey != nil {
		maxKey = append([]byte(nil), maxKey...)
	}
	return minKey, maxKey
}

// tableSpec identifies a table file to open and its recorded metadata.
type tableSpec struct {
	num        uint32
	depth      int
	path       string
	size       int64
	minKey     []byte // recorded user-key bounds; nil when unknown
	maxKey     []byte
	entries    int // total entries; 0 means unknown
	tombstones int // tombstone entries, for the density trigger
	// restoreLazy marks a table being restored on open, which may defer its metadata
	// parse when LazyTableMetadata is set. Fresh flush/compaction outputs leave it
	// false so they always validate eagerly.
	restoreLazy bool
}

// openTableMeta creates a pooled read handle for a table file and its reader.
func (s *engineT) openTableMeta(spec tableSpec) (*tableMeta, error) {
	handle := s.cfg.FDs.newHandle(spec.path)
	// Fresh flush/compaction outputs always validate eagerly (a just-written corrupt
	// table must fail immediately). Only a restored table may open lazily, and only
	// when LazyTableMetadata is set, deferring its bloom/index parse to first read.
	newReader := newCachedTableReader
	if spec.restoreLazy && s.cfg.LazyTableMetadata {
		newReader = newCachedTableReaderLazy
	}
	r, err := newReader(handle, spec.size, s.cfg.Cache, spec.num)
	if err != nil {
		handle.close()
		return nil, err
	}
	// Drop the reader's parsed index/filter when its descriptor is evicted, so
	// resident metadata is bounded by the open-file count, not the live-table
	// count. A later read rebuilds it on demand.
	handle.rw.Lock()
	handle.onEvict = r.dropMeta
	if handle.file == nil {
		r.dropMeta() // eviction may have finished before the callback was installed
	}
	handle.rw.Unlock()
	return &tableMeta{
		num:        spec.num,
		depth:      spec.depth,
		handle:     handle,
		path:       spec.path,
		reader:     r,
		size:       spec.size,
		minKey:     spec.minKey,
		maxKey:     spec.maxKey,
		entries:    spec.entries,
		tombstones: spec.tombstones,
		refs:       1,
	}, nil
}

// resolveBlob reads the value a blob pointer addresses, caching one fd-pool handle
// per blob file number. For a TTL value the blob holds the raw user bytes; callers
// read the deadline from the pointer's expiresAt, so no TTL re-decoding is needed.
func (s *engineT) resolveBlob(p blobPointer) ([]byte, error) {
	s.blobMu.Lock()
	if s.blobHandles == nil {
		s.blobHandles = map[uint32]*blobReader{}
	}
	br := s.blobHandles[p.num]
	if br == nil {
		path, err := s.cfg.blobPath(p.num)
		if err != nil {
			s.blobMu.Unlock()
			return nil, err
		}
		br = newBlobReader(s.cfg.FDs.newHandle(path))
		s.blobHandles[p.num] = br
	}
	s.blobMu.Unlock()
	return br.read(p)
}

// Close releases all open table files.
func (s *engineT) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for _, t := range s.tables {
		if err := t.releaseOwner(false); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.setTables(nil)
	return firstErr
}
