// The engine assembles the storage core: an in-memory write buffer that
// flushes to on-disk SSTables, and the read path that merges them. Compaction
// and the WAL live above it.

package levisdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
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
	MemtableSize   int64
	BloomBits      int
	BlockSize      int
	FreshCodecName string
	LevelCodecs    []string
	EntropySkip    bool         // enable the per-block entropy pre-check when writing tables
	Cache          *blockCacheT // shared block cache; nil disables caching
	FDs            *fdPool      // shared open-descriptor pool; nil keeps files open
	// Commit durably records a table-set replacement, invokes install while the
	// manifest transaction is serialized, and returns only after the edit is
	// durable. Tests and recovery leave it nil and install directly.
	Commit func(inputs, outputs []*tableMeta, install func()) error
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
	seed         int64

	// flushMu serializes Flush and Compact so at most one runs at a time; the
	// finer mu guards the fields those operations read and swap.
	flushMu sync.Mutex
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
	}
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

// Get resolves key at snapshot seq, returning the value or that it is absent or
// deleted. It reads memtable, then the flushing memtable, then tables.
func (s *engineT) get(seq uint64, key []byte) (value []byte, found, deleted bool, err error) {
	return s.getAtTime(seq, key, time.Now().UnixNano())
}

// versionPick tracks the newest visible version of a key seen so far while a
// lookup scans the memtable, immutable memtable, recovery memtables, and tables.
type versionPick struct {
	value   []byte
	seq     uint64
	found   bool
	deleted bool
}

// set records the first version found (no prior best to compare against).
func (p *versionPick) set(v []byte, seq uint64, deleted bool) {
	p.value, p.seq, p.found, p.deleted = v, seq, true, deleted
}

// merge folds one candidate version into the best. A higher sequence wins; the
// same sequence with a different kind or value is an impossible-under-unique-seqs
// anomaly and returns a conflict error tagged with source. Ties with an identical
// version are ignored (the same record read from more than one place).
func (p *versionPick) merge(v []byte, seq uint64, deleted bool, source string) error {
	switch {
	case !p.found || seq > p.seq:
		p.set(v, seq, deleted)
	case seq == p.seq && (deleted != p.deleted || (!deleted && !bytes.Equal(v, p.value))):
		return fmt.Errorf("%s: conflicting versions for key at sequence %d", source, seq)
	}
	return nil
}

func (s *engineT) getAtTime(seq uint64, key []byte, now int64) (value []byte, found, deleted bool, err error) {
	// Hold the read lock for the whole lookup so a concurrent compaction cannot
	// close and remove a table file mid-read; the compaction swap runs under
	// the write lock.
	s.mu.RLock()
	defer s.mu.RUnlock()

	var best versionPick
	if v, vseq, f, d := s.mem.getVersionAt(seq, key, now); f {
		best.set(v, vseq, d)
	}
	if s.imm != nil {
		if v, vseq, f, d := s.imm.getVersionAt(seq, key, now); f {
			if err := best.merge(v, vseq, d, "memtable"); err != nil {
				return nil, false, false, err
			}
		}
	}
	for _, recovered := range s.recoveryMems {
		if v, vseq, f, d := recovered.getVersionAt(seq, key, now); f {
			if err := best.merge(v, vseq, d, "recovery memtable"); err != nil {
				return nil, false, false, err
			}
		}
	}
	// Table creation order is not version order: compacting an old tier creates
	// a new file after newer shallow-tier tables. Inspect every overlapping table
	// and select the greatest visible sequence rather than returning the newest
	// file's first match.
	for _, table := range s.tables {
		// Skip a table whose recorded key range cannot hold key, avoiding a bloom
		// probe and a possible false-positive block seek on the target disk.
		if !table.mayContain(key) {
			continue
		}
		v, vseq, f, d, gerr := table.reader.lookupAtTime(key, seq, now, false)
		if gerr != nil {
			return nil, false, false, gerr
		}
		if !f {
			continue
		}
		if err := best.merge(v, vseq, d, "table"); err != nil {
			return nil, false, false, err
		}
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
	return s.hasAtTime(seq, key, time.Now().UnixNano())
}

func (s *engineT) hasAtTime(seq uint64, key []byte, now int64) (found, deleted bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var bestSeq uint64
	var bestFound, bestDeleted bool
	if _, vseq, f, d := s.mem.getVersionAt(seq, key, now); f {
		bestSeq, bestFound, bestDeleted = vseq, true, d
	}
	if s.imm != nil {
		if _, vseq, f, d := s.imm.getVersionAt(seq, key, now); f {
			switch {
			case !bestFound || vseq > bestSeq:
				bestSeq, bestFound, bestDeleted = vseq, true, d
			case vseq == bestSeq && d != bestDeleted:
				return false, false, fmt.Errorf("memtable: conflicting kinds for key at sequence %d", vseq)
			}
		}
	}
	for _, recovered := range s.recoveryMems {
		if _, vseq, f, d := recovered.getVersionAt(seq, key, now); f {
			switch {
			case !bestFound || vseq > bestSeq:
				bestSeq, bestFound, bestDeleted = vseq, true, d
			case vseq == bestSeq && d != bestDeleted:
				return false, false, fmt.Errorf("recovery memtable: conflicting kinds for key at sequence %d", vseq)
			}
		}
	}
	for _, table := range s.tables {
		if !table.mayContain(key) {
			continue
		}
		vseq, f, d, gerr := table.reader.hasVersionAtTime(key, seq, now)
		if gerr != nil {
			return false, false, gerr
		}
		if !f {
			continue
		}
		if !bestFound || vseq > bestSeq {
			bestSeq, bestFound, bestDeleted = vseq, true, d
			continue
		}
		if vseq == bestSeq && d != bestDeleted {
			return false, false, fmt.Errorf("table: conflicting kinds for key at sequence %d", vseq)
		}
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
	meta, err := s.writeTable(num, 0, path, imm.newIterator(), imm.rangeTombstones())
	if err != nil {
		return err
	}

	install := func() {
		s.mu.Lock()
		s.tables = append(s.tables, meta)
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

// writeTable writes all entries from a memtable iterator into a new table file
// at the given tier depth, persists any range tombstones, and opens it for
// reading.
func (s *engineT) writeTable(num uint32, depth int, path string, it *memtableIterator, rts []rangeTombstone) (*tableMeta, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
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
		codec:       c,
		bloomBits:   s.cfg.BloomBits,
		blockSize:   s.cfg.BlockSize,
		entropySkip: s.cfg.EntropySkip,
	})
	w.setRangeTombstones(rts)
	for it.Next() {
		if err := w.Add(it.internalKey(), it.Value()); err != nil {
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
}

// openTableMeta creates a pooled read handle for a table file and its reader.
func (s *engineT) openTableMeta(spec tableSpec) (*tableMeta, error) {
	handle := s.cfg.FDs.newHandle(spec.path)
	r, err := newCachedTableReader(handle, spec.size, s.cfg.Cache, spec.num)
	if err != nil {
		handle.close()
		return nil, err
	}
	// Drop the reader's parsed index/filter when its descriptor is evicted, so
	// resident metadata is bounded by the open-file count, not the live-table
	// count. A later read rebuilds it on demand.
	handle.onEvict = r.dropMeta
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
	s.tables = nil
	return firstErr
}
