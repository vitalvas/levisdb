package levisdb

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// DB is an open levisdb database.
type DB struct {
	opts        Options
	part        Partitioner
	store       *storageT
	alloc       *allocatorT
	man         *manifestWriter
	manNum      uint32 // file number of the live manifest, for rotation cleanup
	wal         *walT
	walFile     *os.File
	walNum      uint32
	retiredLogs []uint32
	// startupOutputs are tables produced by WAL recovery but not yet protected
	// by a CURRENT-selected manifest. A failed Open removes only these tables,
	// never tables restored from the previous manifest.
	startupOutputs []*tableMeta
	shards         []*shardT

	readSeq atomic.Uint64 // highest committed sequence, for snapshot reads
	// filterSafeSeq is the highest sequence whose WAL segment has been retired
	// after every shard was flushed. A compaction filter may physically discard
	// only versions at or below this point, or crash replay could resurrect them.
	filterSafeSeq atomic.Uint64

	sched *scheduler   // serializes flush + compaction off the write path
	snaps *snapshots   // live read snapshots, for compaction retention
	cache *blockCacheT // shared block cache across all shards
	fds   *fdPool      // shared bounded open-table-descriptor pool

	metrics dbMetrics // cumulative counters since open

	// walSyncStop stops the NoSync background fsync loop; walSyncWG awaits it.
	walSyncStop chan struct{}
	walSyncWG   sync.WaitGroup

	closeMu      sync.Mutex // serializes Close/crash teardown
	manMu        sync.Mutex // guards manifest appends from concurrent scheduler workers
	checkpointMu sync.Mutex
	bgMu         sync.Mutex
	bgErr        error

	mu     sync.RWMutex
	closed bool
}

func (db *DB) setBackgroundError(err error) {
	if err == nil {
		return
	}
	db.bgMu.Lock()
	if db.bgErr == nil {
		db.bgErr = fmt.Errorf("levisdb: persistent database failure: %w", err)
	}
	db.bgMu.Unlock()
}

func (db *DB) backgroundError() error {
	db.bgMu.Lock()
	defer db.bgMu.Unlock()
	return db.bgErr
}

// Batch is an ordered set of writes applied atomically by Write.
type Batch struct {
	ops []batchOp
}

type batchOp struct {
	kind  EntryKind
	key   []byte
	value []byte
	ttl   time.Duration
}

// Iterator iterates over key/value pairs in key order.
type Iterator interface {
	// Next advances to the next key; it reports whether one exists.
	Next() bool
	// Key returns the current key. The slice is valid until the next call.
	Key() []byte
	// Value returns the current value. The slice is valid until the next call.
	Value() []byte
	// Error returns any error accumulated during iteration.
	Error() error
	// Close releases the iterator.
	Close() error
}

// Open opens or creates a database at opts.Dir.
func Open(opts Options) (*DB, error) {
	opts.fillDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}

	var store *storageT
	var err error
	if opts.ReadOnly {
		store, err = openStorageReadOnly(opts.Dir)
	} else {
		store, err = openStorage(opts.Dir)
	}
	if err != nil {
		return nil, err
	}

	cacheSize := opts.BlockCacheSize
	if opts.DisableBlockCache {
		cacheSize = 0
	}
	db := &DB{
		opts:  opts,
		part:  opts.resolvePartitioner(),
		store: store,
		snaps: newSnapshots(),
		cache: newBlockCache(cacheSize),
		fds:   newFDPool(opts.MaxOpenFiles),
	}

	if err := db.load(); err != nil {
		db.closeAfterOpenError()
		return nil, err
	}
	db.startWALSyncLoop()
	registerMetrics(db)
	return db, nil
}

// startWALSyncLoop runs a background fsync of the WAL under NoSync to bound the
// crash-loss window. It is a no-op for read-only, durable (sync), or disabled
// (negative interval) configurations.
func (db *DB) startWALSyncLoop() {
	if db.opts.ReadOnly || !db.opts.NoSync || db.opts.WALSyncInterval <= 0 || db.wal == nil {
		return
	}
	stop := make(chan struct{})
	db.walSyncStop = stop
	db.walSyncWG.Add(1)
	// Capture stop and the interval locally so the goroutine never reads db fields
	// that stopWALSyncLoop mutates concurrently.
	interval := db.opts.WALSyncInterval
	go func() {
		defer db.walSyncWG.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if _, err := db.wal.syncNow(); err != nil && err != os.ErrClosed {
					db.setBackgroundError(err)
					return
				}
			}
		}
	}()
}

// stopWALSyncLoop signals the background syncer and waits for it to exit. Safe to
// call more than once and when the loop never started.
func (db *DB) stopWALSyncLoop() {
	if db.walSyncStop != nil {
		close(db.walSyncStop)
		db.walSyncStop = nil
	}
	db.walSyncWG.Wait()
}

// closeAfterOpenError releases resources created during a partial Open without
// flushing recovered state or changing the manifest selected by CURRENT.
func (db *DB) closeAfterOpenError() {
	if db.sched != nil {
		db.sched.Close()
	}
	if db.walFile != nil {
		_ = db.walFile.Close()
		if db.walNum != 0 {
			_ = db.store.removeLog(db.walNum)
		}
	}
	if db.man != nil {
		_ = db.man.Close()
	}
	if len(db.startupOutputs) > 0 {
		unmanifested := make(map[*tableMeta]bool, len(db.startupOutputs))
		for _, table := range db.startupOutputs {
			unmanifested[table] = true
		}
		for _, shard := range db.shards {
			if shard == nil {
				continue
			}
			shard.mu.Lock()
			kept := shard.tables[:0]
			for _, table := range shard.tables {
				if !unmanifested[table] {
					kept = append(kept, table)
				}
			}
			shard.tables = kept
			shard.mu.Unlock()
		}
		for _, table := range db.startupOutputs {
			_ = table.releaseOwner(true)
		}
		db.startupOutputs = nil
	}
	for _, s := range db.shards {
		if s != nil {
			_ = s.Close()
		}
	}
	_ = db.store.Close()
}

// load recovers existing state or initializes a fresh database, verifying the
// partitioner identity and shard count against any persisted manifest.
func (db *DB) load() error {
	manNum, ok, err := db.store.readCurrent()
	if err != nil {
		return err
	}

	var startSeq uint64
	var state *manifestState
	if ok {
		state, err = db.replayManifest(manNum)
		if err != nil {
			return err
		}
		if state.Partitioner != db.part.Name() || state.ShardCount != db.opts.ShardCount {
			return ErrPartitionerMismatch
		}
		startSeq = state.LastSeq
	}

	// Existing WAL segments (from a crash) and the current manifest number also
	// come from the shared counter; start the allocator above all of them so no
	// file number is reused.
	logs, err := db.store.listLogs()
	if err != nil {
		return err
	}
	start := db.highestFileNum(state)
	for _, n := range logs {
		if n > start {
			start = n
		}
	}
	if ok && manNum > start {
		start = manNum
	}
	db.alloc = newAllocator(start)

	db.shards = make([]*shardT, db.opts.ShardCount)
	for i := range db.shards {
		db.shards[i] = newShard(db.shardConfig(i), db.alloc, db.tablePathFn(i), int64(i+1))
	}
	if state != nil {
		if err := db.restoreTables(state); err != nil {
			return err
		}
	}
	originalTables := db.liveTableSet()

	// Replay any leftover WAL segments into the shard memtables. This recovers
	// writes committed since the last flush that a crash left only in the WAL.
	recoveredSeq, err := db.recoverWALMode(logs, startSeq, !db.opts.ReadOnly)
	if !db.opts.ReadOnly {
		db.startupOutputs = db.tablesAddedSince(originalTables)
	}
	if err != nil {
		return err
	}
	if recoveredSeq > startSeq {
		startSeq = recoveredSeq
	}
	if db.opts.ReadOnly {
		db.readSeq.Store(startSeq)
		return nil
	}

	// Open a fresh WAL segment and manifest for this session.
	if err := db.openWAL(startSeq); err != nil {
		return err
	}
	if err := db.openManifest(); err != nil {
		return err
	}
	db.startupOutputs = nil // CURRENT now protects every recovery output
	// CURRENT now durably names a baseline containing every table produced by
	// recovery. Only at this point is it safe to retire the old WAL segments.
	for _, num := range logs {
		if err := db.store.removeLog(num); err != nil {
			return err
		}
	}
	db.filterSafeSeq.Store(startSeq)
	// Remove stale manifests: the one replayed above plus any orphans from a
	// crashed rotation are obsolete now that CURRENT points at the fresh
	// baseline. This keeps manifest files from accumulating across restarts.
	db.cleanupManifests()
	// A crash can land after a compaction output is synced but before its
	// manifest edit, or after the edit but before obsolete inputs are removed.
	// CURRENT now protects the complete live set, so every other canonical table
	// name is provably orphaned and can be reclaimed. Read-only opens return
	// above and never perform this cleanup.
	db.cleanupTables()

	db.sched = newScheduler(db.opts.CompactionConcurrency, db.flushShard)
	return nil
}

func (db *DB) liveTableSet() map[*tableMeta]bool {
	set := make(map[*tableMeta]bool)
	for _, shard := range db.shards {
		shard.mu.RLock()
		for _, table := range shard.tables {
			set[table] = true
		}
		shard.mu.RUnlock()
	}
	return set
}

func (db *DB) tablesAddedSince(original map[*tableMeta]bool) []*tableMeta {
	var added []*tableMeta
	for _, shard := range db.shards {
		shard.mu.RLock()
		for _, table := range shard.tables {
			if !original[table] {
				added = append(added, table)
			}
		}
		shard.mu.RUnlock()
	}
	return added
}

// cleanupManifests removes every manifest file except the live one.
func (db *DB) cleanupManifests() {
	nums, err := db.store.listManifests()
	if err != nil {
		return
	}
	for _, n := range nums {
		if n != db.manNum {
			_ = db.store.removeManifest(n)
		}
	}
}

// cleanupTables removes canonical .sst files that are absent from the live
// table set selected by CURRENT. Failures are best-effort: an orphan must not
// make otherwise-valid data unavailable, and a later writable open retries.
func (db *DB) cleanupTables() {
	for shardIdx, shard := range db.shards {
		live := make(map[uint32]bool)
		shard.mu.RLock()
		for _, table := range shard.tables {
			live[table.num] = true
		}
		shard.mu.RUnlock()

		nums, err := db.store.listTables(shardIdx)
		if err != nil {
			continue
		}
		for _, num := range nums {
			if live[num] {
				continue
			}
			path, err := db.store.tablePath(shardIdx, num)
			if err == nil {
				_ = removeFileDurable(path)
			}
		}
	}
}

func (db *DB) shardConfig(i int) shardConfigT {
	return shardConfigT{
		Index:          i,
		MemtableSize:   db.opts.MemtableSize,
		BloomBits:      db.opts.BloomBits,
		BlockSize:      db.opts.BlockSize,
		FreshCodecName: db.opts.FreshCodec,
		LevelCodecs:    db.opts.LevelCodecs,
		Cache:          db.cache,
		FDs:            db.fds,
		Commit: func(inputs, outputs []*tableMeta, install func()) error {
			return db.commitTableChange(i, inputs, outputs, install)
		},
	}
}

func (db *DB) tablePathFn(shard int) func(uint32) (string, error) {
	return func(num uint32) (string, error) {
		return db.store.tablePath(shard, num)
	}
}

// maxCompactionBytes maps the option to the compaction config: a negative value
// (caller disabled the cap) becomes 0, which the picker treats as unbounded.
func maxCompactionBytes(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}

func (db *DB) compactionConfig() compactionConfigT {
	return compactionConfigT{
		TierRatio:          db.opts.TierRatio,
		BloomBits:          db.opts.BloomBits,
		BlockSize:          db.opts.BlockSize,
		FreshCodecName:     db.opts.FreshCodec,
		BottomCodecName:    db.opts.BottomCodec,
		LevelCodecs:        db.opts.LevelCodecs,
		FileSizeBase:       db.opts.FileSizeBase,
		FileSizeMultiplier: db.opts.FileSizeMultiplier,
		FileSizeMax:        db.opts.FileSizeMax,
		ExpireBefore:       db.snaps.oldestIteratorTime(time.Now().UnixNano()),
		Filter:             db.opts.CompactionFilter,
		MaxCompactionBytes: maxCompactionBytes(db.opts.MaxCompactionBytes),
	}
}

// compactionRunConfig atomically decides whether this compaction may apply the
// user filter. If a read view is already pinned, the compaction still proceeds
// but leaves filtering for a later run. A successful reservation blocks new
// reads until release is called.
func (db *DB) compactionRunConfig(forceCheckpoint bool) (retain uint64, cc compactionConfigT, release func(), err error) {
	cc = db.compactionConfig()
	if cc.Filter != nil && db.snaps.beginCompactionFilter() {
		safe := db.filterSafeSeq.Load()
		if forceCheckpoint {
			var prepareErr error
			safe, prepareErr = db.prepareCompactionFilter()
			if prepareErr != nil {
				db.snaps.endCompactionFilter()
				return 0, compactionConfigT{}, func() {}, prepareErr
			}
		}
		cc.FilterThrough = safe
		return db.readSeq.Load(), cc, db.snaps.endCompactionFilter, nil
	}
	cc.Filter = nil
	return db.snaps.oldest(db.readSeq.Load()), cc, func() {}, nil
}

// openWAL creates a fresh WAL segment for this session.
func (db *DB) openWAL(startSeq uint64) error {
	num := db.alloc.Next()
	if num == 0 {
		return ErrFileNumberExhausted
	}
	f, err := os.OpenFile(db.store.logPath(num), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(db.store.logPath(num))); err != nil {
		_ = f.Close()
		_ = db.store.removeLog(num)
		return err
	}
	db.walFile = f
	db.walNum = num
	db.wal = newWAL(f, db.walBridge(), startSeq, !db.opts.NoSync)
	db.wal.apply = db.applyCommitted
	db.readSeq.Store(startSeq)
	return nil
}

// openManifest creates a fresh manifest for this session and records the
// current live table set as its baseline.
func (db *DB) openManifest() error {
	w, num, err := db.newManifestWithBaseline()
	if err != nil {
		return err
	}
	if err := db.store.setCurrent(num); err != nil {
		if currentWasInstalled(err) {
			// CURRENT already names this complete baseline. Keep both it and any
			// recovery outputs it references; the old WALs remain because load
			// stops here, so either pre- or post-rename state can recover safely.
			db.man = w
			db.manNum = num
			db.startupOutputs = nil
			return err
		}
		_ = w.Close()
		_ = db.store.removeManifest(num)
		return err
	}
	db.man = w
	db.manNum = num
	return nil
}

// newManifestWithBaseline creates a new manifest file and writes a baseline
// edit capturing every live table plus the committed-sequence watermark, so a
// reopen can reconstruct the full state from this one manifest without the
// history that preceded it. It does not touch CURRENT.
func (db *DB) newManifestWithBaseline() (*manifestWriter, uint32, error) {
	num := db.alloc.Next()
	if num == 0 {
		return nil, 0, ErrFileNumberExhausted
	}
	w, err := createManifest(db.store.manifestPath(num), db.part.Name(), db.opts.ShardCount)
	if err != nil {
		return nil, 0, err
	}

	edit := manifestEdit{HasLastSeq: true, LastSeq: db.readSeq.Load()}
	for i, s := range db.shards {
		for _, t := range s.Tables() {
			edit.Added = append(edit.Added, manifestTableInfo{
				Shard:      i,
				Num:        t.Num,
				Depth:      t.Depth,
				Size:       t.Size,
				MinKey:     t.MinKey,
				MaxKey:     t.MaxKey,
				Entries:    t.Entries,
				Tombstones: t.Tombstones,
			})
		}
	}
	if err := w.append(&edit); err != nil {
		_ = w.Close()
		_ = db.store.removeManifest(num)
		return nil, 0, err
	}
	return w, num, nil
}

// maybeRotateManifest replaces the live manifest with a fresh baseline once it
// has accumulated many edits, bounding both the manifest file size and the
// replay cost on the next open. The new manifest is fully written and CURRENT
// swapped before the old is removed, so a crash mid-rotation always leaves
// CURRENT pointing at a complete manifest. Caller holds db.manMu.
func (db *DB) maybeRotateManifest() {
	if db.man.editCount() < manifestRotateEdits {
		return
	}
	w, num, err := db.newManifestWithBaseline()
	if err != nil {
		return // keep appending to the old manifest; rotation is best-effort
	}
	if err := db.store.setCurrent(num); err != nil {
		if !currentWasInstalled(err) {
			_ = w.Close()
			_ = db.store.removeManifest(num)
			return
		}
		// The rename is visible but not known durable. Adopt the new manifest
		// for the live process, preserve the old one as a crash fallback, and
		// poison writes so Close retains the WAL for either recovery path.
		old := db.man
		db.man = w
		db.manNum = num
		_ = old.Close()
		db.setBackgroundError(err)
		return
	}
	old, oldNum := db.man, db.manNum
	db.man = w
	db.manNum = num
	old.Close()
	_ = db.store.removeManifest(oldNum)
}

// manifestRotateEdits is the edit threshold that triggers a manifest rotation.
// It is a var so tests can lower it; production keeps the default.
var manifestRotateEdits = 4096

// walSegmentBytes bounds a live WAL segment. Rotation checkpoints every shard
// before retiring old segments; it is a var so tests can exercise rotation.
var walSegmentBytes int64 = 64 << 20

// Close flushes and releases the database. It is safe to call more than once.
func (db *DB) Close() error {
	db.closeMu.Lock()
	defer db.closeMu.Unlock()

	// Mark the DB closed while holding both lifecycle locks, then release the
	// checkpoint lock before draining scheduler workers. A filtering worker may
	// itself be checkpointing; holding checkpointMu while waiting for it would
	// deadlock shutdown.
	db.checkpointMu.Lock()
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		db.checkpointMu.Unlock()
		return nil
	}
	db.closed = true
	db.mu.Unlock()
	db.checkpointMu.Unlock()
	deregisterMetrics(db)
	// Stop the background WAL syncer before closing the WAL so it cannot fsync a
	// closed file. It holds no db locks, so this is safe here.
	db.stopWALSyncLoop()
	if db.sched != nil {
		db.sched.Close()
	}

	db.checkpointMu.Lock()
	defer db.checkpointMu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.opts.ReadOnly {
		var firstErr error
		for _, s := range db.shards {
			if err := s.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if err := db.store.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}

	firstErr := db.backgroundError()
	setErr := func(e error) {
		if e != nil && firstErr == nil {
			firstErr = e
		}
	}
	// Flush every shard on clean shutdown so buffered writes persist to tables.
	// WAL replay (M10) covers the crash case; this covers the graceful one.
	for _, s := range db.shards {
		if err := s.Flush(); err != nil {
			setErr(err)
			continue
		}
		// A background flush may already have sealed an immutable memtable. The
		// first pass drains it; the second captures the active memtable too.
		if err := s.Flush(); err != nil {
			setErr(err)
		}
	}
	// A manifest rotation can fail at the final directory sync after its
	// rename. That failure is recorded asynchronously by commitTableChange even
	// though the table installation itself succeeded. Re-read it before deciding
	// whether WAL removal is safe; firstErr was captured before shutdown flushes.
	setErr(db.backgroundError())
	// Close the fully-synced manifest before retiring its recovery WAL. Some
	// filesystems report deferred writeback errors from Close; in that case the
	// WAL must remain available even though every earlier Sync appeared to pass.
	if db.man != nil {
		setErr(db.man.Close())
	}
	if db.wal != nil {
		setErr(db.wal.Close())
		if firstErr == nil {
			for _, num := range append(db.retiredLogs, db.walNum) {
				if num != 0 {
					setErr(db.store.removeLog(num))
				}
			}
		}
	}
	for _, s := range db.shards {
		setErr(s.Close())
	}
	setErr(db.store.Close())
	return firstErr
}

// crash simulates a process crash for tests: it stops the scheduler and
// releases the directory lock and file handles WITHOUT flushing memtables or
// removing the WAL, leaving exactly the on-disk state a real crash would.
func (db *DB) crash() {
	db.closeMu.Lock()
	defer db.closeMu.Unlock()

	db.checkpointMu.Lock()
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		db.checkpointMu.Unlock()
		return
	}
	db.closed = true
	db.mu.Unlock()
	db.checkpointMu.Unlock()
	deregisterMetrics(db)
	db.stopWALSyncLoop()
	if db.sched != nil {
		db.sched.Close()
	}

	db.checkpointMu.Lock()
	defer db.checkpointMu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.walFile != nil {
		db.walFile.Sync()
		db.walFile.Close()
	}
	if db.man != nil {
		db.man.Close()
	}
	for _, s := range db.shards {
		s.Close()
	}
	db.store.Close()
}

// Put applies opts as one set operation.
func (db *DB) Put(opts PutOptions) error {
	var b Batch
	b.Put(opts)
	return db.Write(&b)
}

// Delete removes key.
func (db *DB) Delete(key []byte) error {
	var b Batch
	b.Delete(key)
	return db.Write(&b)
}

// Get returns the value for key at the latest committed sequence, or
// ErrNotFound.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	if len(key) == 0 {
		db.mu.RUnlock()
		return nil, ErrEmptyKey
	}
	// Keep the captured version alive until the shard lookup has acquired its
	// read lock. Otherwise a concurrent write and compaction can reclaim this
	// version in the gap between loading readSeq and reading the table set.
	seq := db.snaps.acquireCurrent(&db.readSeq)
	db.mu.RUnlock()
	defer db.snaps.release(seq)
	return db.getAt(seq, key)
}

// getAt resolves key at a specific snapshot sequence.
func (db *DB) getAt(seq uint64, key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	idx, err := db.shardForKey(key)
	if err != nil {
		return nil, err
	}
	v, found, deleted, err := db.shards[idx].get(seq, key)
	if err != nil {
		return nil, err
	}
	if !found || deleted {
		return nil, ErrNotFound
	}
	return v, nil
}

// Has reports whether key exists at the latest committed sequence. It is
// cheaper than Get: it is bloom-gated and never copies the value.
func (db *DB) Has(key []byte) (bool, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return false, ErrClosed
	}
	if len(key) == 0 {
		db.mu.RUnlock()
		return false, ErrEmptyKey
	}
	seq := db.snaps.acquireCurrent(&db.readSeq)
	db.mu.RUnlock()
	defer db.snaps.release(seq)
	return db.hasAt(seq, key)
}

// hasAt resolves membership at a specific snapshot sequence.
func (db *DB) hasAt(seq uint64, key []byte) (bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return false, ErrClosed
	}
	if len(key) == 0 {
		return false, ErrEmptyKey
	}
	idx, err := db.shardForKey(key)
	if err != nil {
		return false, err
	}
	found, deleted, err := db.shards[idx].has(seq, key)
	if err != nil {
		return false, err
	}
	return found && !deleted, nil
}

// shardForKey validates custom partitioners at the API boundary. Built-ins
// always return an in-range index, but a broken custom implementation should
// produce an ordinary error rather than an index-out-of-range panic on reads.
func (db *DB) shardForKey(key []byte) (int, error) {
	shard := db.part.Shard(key, db.opts.ShardCount)
	if shard < 0 || shard >= db.opts.ShardCount {
		return 0, fmt.Errorf("levisdb: partitioner returned shard %d outside [0,%d)", shard, db.opts.ShardCount)
	}
	return shard, nil
}

// backpressure poll/slowdown tuning. The slowdown delay matches LevelDB's
// ~1ms-per-write nudge; the hard-stop path polls at the same cadence and
// re-signals the scheduler so a blocked writer always makes progress once
// compaction drains the shard.
const (
	writeSlowdownDelay = time.Millisecond
	writeStopPoll      = time.Millisecond
)

// Write applies a batch atomically.
func (db *DB) Write(b *Batch) error {
	// Resolve each op's shard once, so the partitioner is invoked a single time
	// per key across both backpressure and the write itself.
	shards, err := db.resolveBatchShards(b)
	if err != nil {
		return err
	}
	db.throttleWrite(shards)
	db.mu.RLock()
	err = db.writeBatch(b, shards)
	db.mu.RUnlock()
	if err != nil {
		return err
	}
	db.maybeCheckpoint()
	return nil
}

// resolveBatchShards maps each op to its shard once. nil for an empty batch.
func (db *DB) resolveBatchShards(b *Batch) ([]int, error) {
	if b == nil || len(b.ops) == 0 {
		return nil, nil
	}
	shards := make([]int, len(b.ops))
	for i, op := range b.ops {
		if len(op.key) == 0 {
			return nil, ErrEmptyKey
		}
		shard, err := db.shardForKey(op.key)
		if err != nil {
			return nil, err
		}
		shards[i] = shard
	}
	return shards, nil
}

// throttleWrite applies per-shard write backpressure before the batch is
// logged, so a burst cannot outrun the serialized compactor and grow the fresh
// tier without bound. It never holds db.mu, so background flush/compaction keeps
// running while a writer waits. shards are the pre-resolved op shards.
func (db *DB) throttleWrite(shards []int) {
	if db.opts.ReadOnly || len(shards) == 0 {
		return
	}
	slow, stop := db.opts.L0SlowdownTables, db.opts.L0StopTables
	if slow <= 0 && stop <= 0 {
		return
	}
	stalled := false
	// Hard stop: block while any target shard is at or above the stop threshold,
	// re-signalling it so the scheduler drains it, until it falls below stop.
	if stop > 0 {
		for {
			shard, count := db.maxDepth0(shards)
			if count < stop {
				break
			}
			if !stalled {
				db.metrics.writeStalls.Add(1)
				stalled = true
			}
			db.mu.RLock()
			closed := db.closed
			db.mu.RUnlock()
			// Stop waiting if the DB closed or a background compaction/flush failure
			// is latched: the shard will never drain, so let writeBatch surface the
			// error instead of spinning here forever.
			if closed || db.backgroundError() != nil {
				return
			}
			db.sched.Signal(shard)
			time.Sleep(writeStopPoll)
		}
	}
	// Soft slowdown: a single brief delay when a target shard is over the mark
	// (skip if the hard path already stalled this write).
	if !stalled && slow > 0 {
		if _, count := db.maxDepth0(shards); count >= slow {
			db.metrics.writeStalls.Add(1)
			time.Sleep(writeSlowdownDelay)
		}
	}
}

// maxDepth0 returns the shard with the most fresh-tier tables among shards, and
// that count. shards may repeat; each is checked once by the caller's loop.
func (db *DB) maxDepth0(shards []int) (maxShard, maxCount int) {
	maxShard, maxCount = -1, -1
	for _, shard := range shards {
		if c := db.shards[shard].depth0Count(); c > maxCount {
			maxShard, maxCount = shard, c
		}
	}
	return maxShard, maxCount
}

func (db *DB) writeBatch(b *Batch, shards []int) error {
	if db.closed {
		return ErrClosed
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}
	if err := db.backgroundError(); err != nil {
		return err
	}
	if b == nil || b.Len() == 0 {
		return nil
	}

	entries := make([]walEntry, len(b.ops))
	now := time.Now()
	// The whole batch is applied to shard memtables before any flush check, so
	// bound the cumulative bytes landing in one shard as well as each entry. Both
	// limits keep the skiplist arena (uint32 offsets) well below 2^32.
	var perShard map[int]int64
	for i, op := range b.ops {
		if len(op.key) == 0 {
			return ErrEmptyKey
		}
		// Reject an entry that would approach the uint32 offset limits and
		// silently corrupt the skiplist arena or a data block. A TTL value carries
		// an extra 8-byte expiry prefix, so count it toward the limit.
		entrySize := len(op.key) + len(op.value)
		if op.ttl > 0 {
			entrySize += expiryPrefixLen
		}
		if entrySize > maxEntrySize {
			return ErrEntryTooLarge
		}
		if perShard == nil {
			perShard = make(map[int]int64, len(db.shards))
		}
		perShard[shards[i]] += int64(entrySize)
		if perShard[shards[i]] > int64(maxEntrySize) {
			return ErrBatchTooLarge
		}
		expiresAt, err := ttlExpiresAt(now, op.ttl)
		if err != nil {
			return err
		}
		kind := walKind(op.kind)
		if kind == walKindPut && expiresAt != 0 {
			kind = walKindPutTTL
		}
		entries[i] = walEntry{
			Shard:     shards[i],
			Kind:      kind,
			Key:       op.key,
			Value:     op.value,
			ExpiresAt: expiresAt,
		}
	}

	if err := db.wal.append(entries); err != nil {
		db.setBackgroundError(err)
		return err
	}
	return nil
}

func (db *DB) maybeCheckpoint() {
	if walSegmentBytes <= 0 || db.opts.ReadOnly {
		return
	}
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return
	}
	size, err := db.wal.size()
	if err != nil {
		db.setBackgroundError(err)
		return
	}
	if size < walSegmentBytes {
		return
	}
	if err := db.checkpointWAL(); err != nil {
		if err != ErrClosed {
			db.setBackgroundError(err)
		}
	}
}

func (db *DB) checkpointWAL() error {
	_, err := db.checkpointWALMode(false)
	return err
}

// prepareCompactionFilter retires the recovery WAL through a stable sequence
// before table compaction physically removes any value at or below it. Without
// this checkpoint, replaying the still-live WAL after a crash could resurrect a
// value that the filter removed from its table.
func (db *DB) prepareCompactionFilter() (uint64, error) {
	safe := db.filterSafeSeq.Load()
	if db.readSeq.Load() <= safe {
		return safe, nil
	}
	return db.checkpointWALMode(true)
}

func (db *DB) checkpointWALMode(force bool) (uint64, error) {
	db.checkpointMu.Lock()
	defer db.checkpointMu.Unlock()

	if db.closed {
		return db.filterSafeSeq.Load(), ErrClosed
	}
	if db.opts.ReadOnly {
		return db.filterSafeSeq.Load(), ErrReadOnly
	}
	size, err := db.wal.size()
	if err != nil {
		return db.filterSafeSeq.Load(), err
	}
	if !force && size < walSegmentBytes {
		return db.filterSafeSeq.Load(), nil
	}
	if force && db.readSeq.Load() <= db.filterSafeSeq.Load() {
		return db.filterSafeSeq.Load(), nil
	}
	num := db.alloc.Next()
	if num == 0 {
		return db.filterSafeSeq.Load(), ErrFileNumberExhausted
	}
	f, err := os.OpenFile(db.store.logPath(num), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return db.filterSafeSeq.Load(), err
	}
	if err := syncDir(filepath.Dir(db.store.logPath(num))); err != nil {
		_ = f.Close()
		_ = db.store.removeLog(num)
		return db.filterSafeSeq.Load(), err
	}
	oldNum := db.walNum
	oldFile, cutoff, err := db.wal.rotate(f)
	if err != nil {
		_ = f.Close()
		_ = removeFileDurable(db.store.logPath(num))
		return db.filterSafeSeq.Load(), err
	}
	db.walFile = f
	db.walNum = num
	db.retiredLogs = append(db.retiredLogs, oldNum)
	if err := oldFile.Close(); err != nil {
		return db.filterSafeSeq.Load(), err
	}

	// Two passes cover a shard that already had an immutable memtable when the
	// checkpoint began: the first drains it, the second captures the active table
	// that contains every write from the retired segment.
	for _, s := range db.shards {
		if err := s.Flush(); err != nil {
			return db.filterSafeSeq.Load(), err
		}
		if err := s.Flush(); err != nil {
			return db.filterSafeSeq.Load(), err
		}
	}
	if err := db.backgroundError(); err != nil {
		return db.filterSafeSeq.Load(), err
	}
	for len(db.retiredLogs) > 0 {
		if err := db.store.removeLog(db.retiredLogs[0]); err != nil {
			return db.filterSafeSeq.Load(), err
		}
		db.retiredLogs = db.retiredLogs[1:]
	}
	db.filterSafeSeq.Store(cutoff)
	return cutoff, nil
}

// applyCommitted installs one durable WAL batch and publishes its sequence only
// after every mutation is present. walT invokes it serially in commit order.
func (db *DB) applyCommitted(entries []walEntry) {
	var maxSeq uint64
	for i := range entries {
		e := &entries[i]
		s := db.shards[e.Shard]
		switch e.Kind {
		case walKindDelete:
			s.del(e.Seq, e.Key)
		case walKindPutTTL:
			s.putTTL(e.Seq, e.Key, e.Value, e.ExpiresAt)
		default:
			s.Put(e.Seq, e.Key, e.Value)
		}
		if e.Seq > maxSeq {
			maxSeq = e.Seq
		}
	}
	if maxSeq > 0 {
		db.readSeq.Store(maxSeq)
	}

	// Signal any shard that reached its flush threshold; the scheduler drains
	// it off this goroutine so writes are not blocked by flush or compaction.
	for i, s := range db.shards {
		if s.needFlush() {
			db.sched.Signal(i)
		}
	}
}

// flushShard flushes one shard and runs any ready compaction, recording each
// change in the manifest. It is the scheduler's per-shard work unit, so at most
// CompactionConcurrency shards run it at once.
func (db *DB) flushShard(i int) {
	s := db.shards[i]

	// A writer can fill the new active memtable while an older immutable is
	// being flushed. Such writes cannot signal another flush until the immutable
	// is installed, so re-check here and drain every over-limit active table.
	// Without this loop, the last oversized memtable in a burst could remain in
	// memory indefinitely when no later write arrived to signal it again.
	for {
		if err := s.Flush(); err != nil {
			db.setBackgroundError(err)
			return
		}
		if !s.needFlush() {
			break
		}
	}

	// Drain all ready tiers so a burst of flushes does not leave the shard
	// permanently over the tier ratio, and reclaim delete-heavy tiers early.
	for {
		// A tombstone can only be dropped when the retained sequence covers it.
		// While a live snapshot pins the oldest retained sequence below the
		// committed one, a tombstone-triggered compaction reclaims nothing and
		// merely relocates the delete-heavy tier one level deeper, where it
		// re-fires without bound (unbounded-depth churn). Gate the trigger off in
		// that state; count-based compaction still runs. This preview is cheap and
		// does not reserve a compaction filter slot.
		tombstoneRatio := db.opts.TombstoneCompactionRatio
		if db.snaps.oldest(db.readSeq.Load()) < db.readSeq.Load() {
			tombstoneRatio = 0
		}
		depth := s.pickCompaction(db.opts.TierRatio, tombstoneRatio)
		if depth < 0 {
			break
		}
		// Retain versions a live snapshot might still read; with no snapshots,
		// oldest returns the current committed seq so only the newest survives.
		retain, cc, release, err := db.compactionRunConfig(true)
		if err != nil {
			if err != ErrClosed {
				db.setBackgroundError(err)
			}
			break
		}
		err = s.Compact(depth, retain, cc)
		release()
		if err != nil {
			db.setBackgroundError(err)
			break
		}
	}
}

// CompactRange forces a full compaction of every shard, collapsing all tiers
// so tombstones and dead versions are reclaimed without waiting for the tier
// threshold. The start/end bounds are advisory: the Partitioner interface does
// not expose a range-to-shard mapping, so all shards are compacted regardless.
// Versions a live snapshot may still read are retained. It blocks until done
// and is serialized against background compaction per shard.
func (db *DB) CompactRange(start, end []byte) error {
	db.mu.RLock()
	err := db.validateManualCompaction()
	db.mu.RUnlock()
	if err != nil {
		return err
	}
	if db.opts.CompactionFilter != nil {
		if _, err := db.prepareCompactionFilter(); err != nil {
			return err
		}
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if err := db.validateManualCompaction(); err != nil {
		return err
	}
	_ = start
	_ = end
	for _, s := range db.shards {
		if err := db.compactShardFully(s); err != nil {
			return err
		}
	}
	return nil
}

// CompactShard forces a full compaction of a single shard by index, collapsing
// all its tiers so tombstones and dead versions are reclaimed. It is the
// per-shard form of CompactRange, useful for a known hot or bloated shard.
func (db *DB) CompactShard(shard int) error {
	db.mu.RLock()
	err := db.validateManualCompaction()
	shardCount := len(db.shards)
	db.mu.RUnlock()
	if err != nil {
		return err
	}
	if shard < 0 || shard >= shardCount {
		return fmt.Errorf("levisdb: shard %d out of range [0,%d)", shard, shardCount)
	}
	if db.opts.CompactionFilter != nil {
		if _, err := db.prepareCompactionFilter(); err != nil {
			return err
		}
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if err := db.validateManualCompaction(); err != nil {
		return err
	}
	return db.compactShardFully(db.shards[shard])
}

// CompactShardRange forces a full compaction of a single shard. The start/end
// bounds are advisory (the whole shard is compacted) and exist to mirror
// CompactRange for callers that scope work by both shard and key range.
func (db *DB) CompactShardRange(shard int, start, end []byte) error {
	_ = start
	_ = end
	return db.CompactShard(shard)
}

// validateManualCompaction checks state while the caller holds db.mu for the
// full operation, preventing Close from releasing tables or the manifest under
// an in-flight manual compaction.
func (db *DB) validateManualCompaction() error {
	if db.closed {
		return ErrClosed
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}
	if err := db.backgroundError(); err != nil {
		return err
	}
	return nil
}

// compactShardFully flushes the shard then merges all its tables into sized
// bottom-tier outputs, reclaiming tombstones and dead versions.
func (db *DB) compactShardFully(s *shardT) error {
	if err := s.Flush(); err != nil {
		return err
	}

	retain, cc, release, err := db.compactionRunConfig(false)
	if err != nil {
		return err
	}
	err = s.CompactAll(retain, cc)
	release()
	if err != nil {
		return err
	}
	return nil
}

// NewIterator returns an iterator over the whole keyspace at the latest
// committed sequence.
func (db *DB) NewIterator() (Iterator, error) {
	return db.NewRangeIterator(nil, nil)
}

// NewRangeIterator returns an iterator over [start, end) at the latest
// committed sequence. A nil bound is unbounded on that side.
func (db *DB) NewRangeIterator(start, end []byte) (Iterator, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	seq := db.snaps.acquireCurrent(&db.readSeq)
	return db.newRangeIterator(seq, start, end)
}
