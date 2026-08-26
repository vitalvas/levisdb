// The shard engine assembles per-shard storage: an in-memory write buffer
// that flushes to on-disk SSTables, and the read path that merges them. Shards
// are the unit of the token-ring partitioning; compaction and the shared WAL
// live above them.

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

// Config holds the per-shard tuning derived from the database options.
type shardConfigT struct {
	Index          int // shard index
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

// Shard is one partition's storage. Writes go to the active memtable; when it
// fills it becomes immutable and is flushed to a fresh L0 table. Reads consult
// the active memtable, then the flushing memtable, then every overlapping table
// by visible sequence.
type shardT struct {
	cfg   shardConfigT
	alloc fileAllocator
	// tablePath returns the on-disk path for a table number in this shard.
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

	// flushMu serializes Flush and Compact so at most one runs per shard; the
	// finer mu guards the fields those operations read and swap.
	flushMu sync.Mutex
}

// NewShard creates an empty shard. seed varies memtable RNGs deterministically.
func newShard(cfg shardConfigT, alloc fileAllocator, tablePath func(uint32) (string, error), seed int64) *shardT {
	if cfg.FDs == nil {
		// Default to an unbounded pool so callers that do not share one (tests)
		// still get working table handles.
		cfg.FDs = newFDPool(-1)
	}
	return &shardT{
		cfg:       cfg,
		alloc:     alloc,
		tablePath: tablePath,
		mem:       newMemtable(seed),
		seed:      seed,
	}
}

// Put buffers a set into the active memtable at seq.
func (s *shardT) Put(seq uint64, key, value []byte) {
	s.mu.Lock()
	s.mem.Put(seq, key, value)
	s.mu.Unlock()
}

// putTTL buffers a set with an absolute Unix-nanosecond expiration.
func (s *shardT) putTTL(seq uint64, key, value []byte, expiresAt int64) {
	s.mu.Lock()
	s.mem.putTTL(seq, key, value, expiresAt)
	s.mu.Unlock()
}

// Delete buffers a tombstone into the active memtable at seq.
func (s *shardT) del(seq uint64, key []byte) {
	s.mu.Lock()
	s.mem.del(seq, key)
	s.mu.Unlock()
}

// MemEmpty reports whether the active memtable holds no entries.
func (s *shardT) memEmpty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.empty()
}

// NeedFlush reports whether the active memtable has reached the flush
// threshold and no flush is already in progress.
func (s *shardT) needFlush() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.imm == nil && s.mem.Size() >= s.cfg.MemtableSize
}

// depth0Count returns the number of fresh-tier (depth 0) tables, the backlog
// signal write backpressure throttles on.
func (s *shardT) depth0Count() int {
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

// Get resolves key at snapshot seq, returning the value or that it is absent or
// deleted. It reads memtable, then the flushing memtable, then tables.
func (s *shardT) get(seq uint64, key []byte) (value []byte, found, deleted bool, err error) {
	return s.getAtTime(seq, key, time.Now().UnixNano())
}

func (s *shardT) getAtTime(seq uint64, key []byte, now int64) (value []byte, found, deleted bool, err error) {
	// Hold the read lock for the whole lookup so a concurrent compaction cannot
	// close and remove a table file mid-read; the compaction swap runs under
	// the write lock.
	s.mu.RLock()
	defer s.mu.RUnlock()

	var bestValue []byte
	var bestSeq uint64
	var bestFound, bestDeleted bool
	if v, vseq, f, d := s.mem.getVersionAt(seq, key, now); f {
		bestValue, bestSeq, bestFound, bestDeleted = v, vseq, true, d
	}
	if s.imm != nil {
		if v, vseq, f, d := s.imm.getVersionAt(seq, key, now); f {
			switch {
			case !bestFound || vseq > bestSeq:
				bestValue, bestSeq, bestFound, bestDeleted = v, vseq, true, d
			case vseq == bestSeq && (d != bestDeleted || (!d && !bytes.Equal(v, bestValue))):
				return nil, false, false, fmt.Errorf("memtable: conflicting versions for key at sequence %d", vseq)
			}
		}
	}
	for _, recovered := range s.recoveryMems {
		if v, vseq, f, d := recovered.getVersionAt(seq, key, now); f {
			switch {
			case !bestFound || vseq > bestSeq:
				bestValue, bestSeq, bestFound, bestDeleted = v, vseq, true, d
			case vseq == bestSeq && (d != bestDeleted || (!d && !bytes.Equal(v, bestValue))):
				return nil, false, false, fmt.Errorf("recovery memtable: conflicting versions for key at sequence %d", vseq)
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
		if !bestFound || vseq > bestSeq {
			bestValue, bestSeq, bestFound, bestDeleted = v, vseq, true, d
			continue
		}
		if vseq == bestSeq && (d != bestDeleted || (!d && !bytes.Equal(v, bestValue))) {
			return nil, false, false, fmt.Errorf("table: conflicting versions for key at sequence %d", vseq)
		}
	}
	if bestFound {
		return append([]byte(nil), bestValue...), true, bestDeleted, nil
	}
	return nil, false, false, nil
}

// has reports whether key exists at snapshot seq, and whether the newest
// visible version is a tombstone, without copying the value.
func (s *shardT) has(seq uint64, key []byte) (found, deleted bool, err error) {
	return s.hasAtTime(seq, key, time.Now().UnixNano())
}

func (s *shardT) hasAtTime(seq uint64, key []byte, now int64) (found, deleted bool, err error) {
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
	return bestFound, bestDeleted, nil
}

// rotate seals the active memtable as immutable and installs a fresh active
// one, so writes continue while the sealed table is flushed. It reports whether
// there is anything to flush.
func (s *shardT) rotate() bool {
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
func (s *shardT) sealReadOnlyRecoveryMemtable() {
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
// time per shard.
func (s *shardT) Flush() error {
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
	meta, err := s.writeTable(num, 0, path, imm.newIterator())
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
// at the given tier depth and opens it for reading.
func (s *shardT) writeTable(num uint32, depth int, path string, it *memtableIterator) (*tableMeta, error) {
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
	meta, err := s.openTableMeta(tableSpec{
		num:        num,
		depth:      depth,
		path:       path,
		size:       size,
		minKey:     w.minUserKey(),
		maxKey:     w.maxUserKey(),
		entries:    w.entryCount(),
		tombstones: w.tombstoneCount(),
	})
	if err != nil {
		return nil, err
	}
	keep = true
	return meta, nil
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
func (s *shardT) openTableMeta(spec tableSpec) (*tableMeta, error) {
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
func (s *shardT) Close() error {
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
