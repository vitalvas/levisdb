package levisdb

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// DB is an open levisdb database.
type DB struct {
	opts   Options
	store  *storageT
	alloc  *allocatorT
	man    *manifestWriter
	manNum uint32 // file number of the live manifest, for rotation cleanup
	// wal is the single write-ahead log and the segment bookkeeping the
	// checkpoint uses to rotate and retire it.
	wal *dbWAL
	// startupOutputs are tables produced by WAL recovery but not yet protected
	// by a CURRENT-selected manifest. A failed Open removes only these tables,
	// never tables restored from the previous manifest.
	startupOutputs []*tableMeta
	eng            *engineT

	// walSeq is the global monotonic sequence assigned to every mutation as it is
	// logged. readSeq is the highest committed sequence, published only after a
	// batch applies, so snapshot reads never observe a half-applied batch.
	walSeq  atomic.Uint64
	readSeq atomic.Uint64 // highest committed sequence, for snapshot reads
	// filterSafeSeq is the highest sequence whose WAL segment has been retired
	// after a flush. A compaction filter may physically discard only versions at
	// or below this point, or crash replay could resurrect them.
	filterSafeSeq atomic.Uint64

	sched *scheduler   // serializes flush + compaction off the write path
	snaps *snapshots   // live read snapshots, for compaction retention
	cache *blockCacheT // block cache
	fds   *fdPool      // bounded open-table-descriptor pool

	metrics dbMetrics // cumulative counters since open

	// log is the root storage-engine logger (Options.Logger tagged with
	// component=levisdb, or a discarding logger when none was set). Subsystems
	// derive child loggers from it (see newRootLogger).
	log *slog.Logger

	// walSyncStop stops the NoSync background fsync loop; walSyncWG awaits it.
	walSyncStop chan struct{}
	walSyncWG   sync.WaitGroup

	// checkpointWG tracks the at-most-one background WAL checkpoint launched from
	// the write path (maybeCheckpoint), so Close and crash can wait for it to
	// finish before they touch the WAL fields it mutates. checkpointRunning is a
	// single-flight guard: writes trigger a checkpoint often, but only one runs at
	// a time and later triggers are dropped while it is in flight.
	checkpointWG      sync.WaitGroup
	checkpointRunning atomic.Bool

	closeMu      sync.Mutex // serializes Close/crash teardown
	manMu        sync.Mutex // guards manifest appends from concurrent scheduler workers
	checkpointMu sync.Mutex
	bgMu         sync.Mutex
	bgErr        error

	// seqMu orders sequence assignment with WAL enqueue: a batch reserves its
	// contiguous seq range and enqueues it while holding this, so records are
	// written and recovered in ascending seq order. The WAL fsync/commit runs
	// outside it so a slow commit does not serialize the next batch's enqueue.
	seqMu sync.Mutex

	mu     sync.RWMutex
	closed bool
}

// dbWAL is the write-ahead log and the segment bookkeeping the checkpoint uses
// to rotate and retire it.
type dbWAL struct {
	wal     *walT
	file    *os.File
	num     uint32   // live segment file number
	retired []uint32 // rotated-out segments awaiting removal after their flush
	// reapedThroughSeq is the highest sequence whose retained segment the reaper
	// has deleted. GetUpdatesSince cannot serve a sequence at or below it, so a
	// request there returns ErrRetentionExpired. Only used when WAL retention is
	// enabled; retained segments themselves live on disk, not in memory. Atomic
	// because the reaper writes it under db.checkpointMu while GetUpdatesSince
	// reads it under db.mu - different locks, so a plain field would race.
	reapedThroughSeq atomic.Uint64
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
	// rangeDel marks a range delete: key is the inclusive start and value is the
	// exclusive end. kind is ignored for a range delete.
	rangeDel bool
}

// Iterator iterates over key/value pairs in key order.
type Iterator interface {
	// Next advances to the next key; it reports whether one exists.
	Next() bool
	// Key returns the current key. The slice is valid only until the next call to
	// Next or Close; copy it to retain.
	Key() []byte
	// Value returns the current value. The slice is valid only until the next call
	// to Next or Close; copy it to retain.
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
		store: store,
		snaps: newSnapshots(),
		cache: newBlockCache(cacheSize),
		fds:   newFDPool(opts.MaxOpenFiles),
		log:   newRootLogger(opts.Logger),
	}
	db.log.Info("opening database",
		"op", "open", "dir", opts.Dir, "read_only", opts.ReadOnly)

	if err := db.load(); err != nil {
		db.log.Error("open failed", "op", "open", "err", err)
		db.closeAfterOpenError()
		return nil, err
	}
	db.startWALSyncLoop()
	registerMetrics(db)
	db.log.Info("database opened", "op", "open", "recovered_seq", db.readSeq.Load())
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
				if _, err := db.wal.wal.syncNow(); err != nil && err != os.ErrClosed {
					db.setBackgroundError(err)
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
	if db.wal != nil {
		_ = db.wal.file.Close()
		if db.wal.num != 0 {
			_ = db.store.removeLog(db.wal.num)
		}
	}
	if db.man != nil {
		_ = db.man.Close()
	}
	if len(db.startupOutputs) > 0 && db.eng != nil {
		unmanifested := make(map[*tableMeta]bool, len(db.startupOutputs))
		for _, table := range db.startupOutputs {
			unmanifested[table] = true
		}
		db.eng.mu.Lock()
		kept := db.eng.tables[:0]
		for _, table := range db.eng.tables {
			if !unmanifested[table] {
				kept = append(kept, table)
			}
		}
		db.eng.tables = kept
		db.eng.mu.Unlock()
		for _, table := range db.startupOutputs {
			_ = table.releaseOwner(true)
		}
		db.startupOutputs = nil
	}
	if db.eng != nil {
		_ = db.eng.Close()
	}
	_ = db.store.Close()
}

// load recovers existing state or initializes a fresh database.
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
		startSeq = state.LastSeq
	}

	// Existing WAL segments (from a crash) and the current manifest number come
	// from the shared counter; start the allocator above all of them so no file
	// number is reused.
	logs, lerr := db.store.listLogs()
	if lerr != nil {
		return lerr
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

	db.eng = newEngine(db.engineConfig(), db.alloc, db.tablePathFn())
	if state != nil {
		if err := db.restoreTables(state); err != nil {
			return err
		}
	}
	originalTables := db.liveTableSet()

	// Replay leftover WAL segments into the memtable. This recovers writes
	// committed since the last flush that a crash left only in the WAL.
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
	// Remove stale manifests and orphaned tables now that CURRENT points at the
	// fresh baseline. Read-only opens return above and never perform this.
	db.cleanupManifests()
	db.cleanupTables()

	db.sched = newScheduler(db.flushEngine)
	return nil
}

func (db *DB) liveTableSet() map[*tableMeta]bool {
	set := make(map[*tableMeta]bool)
	db.eng.mu.RLock()
	for _, table := range db.eng.tables {
		set[table] = true
	}
	db.eng.mu.RUnlock()
	return set
}

func (db *DB) tablesAddedSince(original map[*tableMeta]bool) []*tableMeta {
	var added []*tableMeta
	db.eng.mu.RLock()
	for _, table := range db.eng.tables {
		if !original[table] {
			added = append(added, table)
		}
	}
	db.eng.mu.RUnlock()
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
	live := make(map[uint32]bool)
	db.eng.mu.RLock()
	for _, table := range db.eng.tables {
		live[table.num] = true
	}
	db.eng.mu.RUnlock()

	nums, err := db.store.listTables()
	if err != nil {
		return
	}
	for _, num := range nums {
		if live[num] {
			continue
		}
		path, err := db.store.tablePath(num)
		if err == nil {
			_ = removeFileDurable(path)
		}
	}
}

func (db *DB) engineConfig() engineConfigT {
	return engineConfigT{
		MemtableSize:   db.opts.MemtableSize,
		BloomBits:      db.opts.BloomBits,
		BlockSize:      db.opts.BlockSize,
		FreshCodecName: db.opts.FreshCodec,
		LevelCodecs:    db.opts.LevelCodecs,
		Cache:          db.cache,
		FDs:            db.fds,
		Commit: func(inputs, outputs []*tableMeta, install func()) error {
			return db.commitTableChange(inputs, outputs, install)
		},
	}
}

func (db *DB) tablePathFn() func(uint32) (string, error) {
	return func(num uint32) (string, error) {
		return db.store.tablePath(num)
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
		TierByteTrigger:    db.opts.TierByteTrigger,
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
		OverlapSelection:   !db.opts.DisableOverlapSelection,
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

// openWAL creates a fresh WAL segment for this session. The global sequence and
// readSeq are seeded from startSeq (the highest recovered seq).
func (db *DB) openWAL(startSeq uint64) error {
	db.walSeq.Store(startSeq)
	db.readSeq.Store(startSeq)
	num := db.alloc.Next()
	if num == 0 {
		return ErrFileNumberExhausted
	}
	path, err := db.store.logPath(num)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		_ = f.Close()
		_ = db.store.removeLog(num)
		return err
	}
	w := newWAL(f, db.walBridge(), walConfig{sync: !db.opts.NoSync})
	w.apply = db.applyCommitted
	db.wal = &dbWAL{wal: w, file: f, num: num}
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
			// recovery outputs it references; the old WAL remains because load
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
	w, err := createManifest(db.store.manifestPath(num))
	if err != nil {
		return nil, 0, err
	}

	edit := manifestEdit{HasLastSeq: true, LastSeq: db.readSeq.Load()}
	for _, t := range db.eng.Tables() {
		edit.Added = append(edit.Added, manifestTableInfo(t))
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

// walSegmentBytes bounds a live WAL segment. Rotation checkpoints (flushes)
// before retiring the old segment; it is a var so tests can exercise rotation.
// At 256 MiB the barrier checkpoint fires rarely enough that a bulk writer is
// not stalled by it; the WAL-size cap still bounds the live WAL at
// walCheckpointStallMultiple x this.
var walSegmentBytes int64 = 256 << 20

// walCheckpointStallMultiple caps how far the live WAL may outgrow
// walSegmentBytes before a writer stops firing checkpoints asynchronously and
// instead blocks until the in-flight checkpoint drains. Checkpointing is normally
// async so writes do not stall on the flush barrier, but a sustained writer can
// outrun a single background checkpoint and grow the WAL without bound (and with
// it, crash-recovery cost). Once the segment reaches this multiple of
// walSegmentBytes, the writer waits, bounding the live WAL at roughly this size.
var walCheckpointStallMultiple int64 = 2

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
	db.log.Info("closing database", "op", "close", "last_seq", db.readSeq.Load())
	deregisterMetrics(db)
	// Wait for any background WAL checkpoint to finish before touching the WAL
	// fields it mutates. closed is now set, so no new checkpoint will start (it
	// returns ErrClosed), and this drains the at-most-one already in flight.
	db.checkpointWG.Wait()
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
		if err := db.eng.Close(); err != nil {
			firstErr = err
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
	// Flush on clean shutdown so buffered writes persist to tables. WAL replay
	// covers the crash case; this covers the graceful one. A background flush may
	// already have sealed an immutable memtable, so the first pass drains it and
	// the second captures the active memtable too.
	if err := db.eng.Flush(); err != nil {
		setErr(err)
	} else if err := db.eng.Flush(); err != nil {
		setErr(err)
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
		setErr(db.wal.wal.Close())
	}
	if firstErr == nil && db.wal != nil {
		// Flushed above; delete every WAL segment on disk. This covers the live and
		// retired segments and any left on disk for replication retention (all now
		// captured in tables). Retention is a live-process feature; a consumer
		// re-bootstraps across a restart. Listing the directory is authoritative, so
		// no orphan .log file is left behind.
		nums, listErr := db.store.listLogs()
		if listErr != nil {
			// Fall back to the tracked numbers if the listing fails. Build a fresh
			// slice so db.wal.retired is not mutated by the append.
			nums = make([]uint32, 0, len(db.wal.retired)+1)
			nums = append(nums, db.wal.retired...)
			nums = append(nums, db.wal.num)
		}
		for _, num := range nums {
			if num != 0 {
				setErr(db.store.removeLog(num))
			}
		}
	}
	setErr(db.eng.Close())
	setErr(db.store.Close())
	if firstErr != nil {
		db.log.Error("database closed with error", "op", "close", "err", firstErr)
	} else {
		db.log.Info("database closed", "op", "close")
	}
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
	// Drain any in-flight background checkpoint before touching WAL fields, as in
	// Close: a real crash cannot corrupt these, but the test harness must not race
	// the checkpoint goroutine against walFile.Close.
	db.checkpointWG.Wait()
	db.stopWALSyncLoop()
	if db.sched != nil {
		db.sched.Close()
	}

	db.checkpointMu.Lock()
	defer db.checkpointMu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	// Close the WAL WITHOUT fsync: a real crash never fsyncs, and the records
	// already reached the OS page cache in writeGroup, which survives process
	// exit. No segment is removed, so recovery replays them.
	if db.wal != nil && db.wal.file != nil {
		db.wal.file.Close()
	}
	if db.man != nil {
		db.man.Close()
	}
	if db.eng != nil {
		db.eng.Close()
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

// DeleteRange removes every key in the half-open range [start, end): every key k
// with start <= k < end becomes invisible as of this call. It is one record
// regardless of how many keys the range spans, so deleting a large or prefix
// range is O(1) rather than one tombstone per key. end must be strictly greater
// than start and non-empty, else ErrInvalidRange. Keys written after this call
// with a key in the range are unaffected (the delete applies only to the sequence
// at which it commits).
func (db *DB) DeleteRange(start, end []byte) error {
	var b Batch
	b.DeleteRange(start, end)
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
	// Keep the captured version alive until the lookup has acquired its read
	// lock. Otherwise a concurrent write and compaction can reclaim this version
	// in the gap between loading readSeq and reading the table set.
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
	v, found, deleted, err := db.eng.get(seq, key)
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
	found, deleted, err := db.eng.has(seq, key)
	if err != nil {
		return false, err
	}
	return found && !deleted, nil
}

// LatestSeq returns the highest committed sequence number: every mutation with a
// sequence at or below it is durable and visible to reads. It is the resume point
// a WALObserver consumer records, and the sequence a replica has caught up to. A
// closed database returns 0. Unlike Snapshot().Seq() it pins nothing, so it is a
// cheap way to read the current write position.
func (db *DB) LatestSeq() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return 0
	}
	return db.readSeq.Load()
}

// backpressure poll/slowdown tuning. The slowdown delay matches LevelDB's
// ~1ms-per-write nudge; the hard-stop path polls at the same cadence and
// re-signals the scheduler so a blocked writer always makes progress once
// compaction drains the engine.
const (
	writeSlowdownDelay = time.Millisecond
	writeStopPoll      = time.Millisecond
)

// Write applies a batch atomically.
func (db *DB) Write(b *Batch) error {
	if b == nil || len(b.ops) == 0 {
		return nil
	}
	// Reject an empty key before throttling so a bad batch fails fast; writeBatch
	// re-checks under the lock.
	for _, op := range b.ops {
		if len(op.key) == 0 {
			return ErrEmptyKey
		}
	}
	db.throttleWrite()
	db.mu.RLock()
	err := db.writeBatch(b)
	db.mu.RUnlock()
	if err != nil {
		return err
	}
	db.maybeCheckpoint()
	return nil
}

// throttleWrite applies write backpressure before the batch is logged, so a
// burst cannot outrun the serialized compactor and grow the fresh tier without
// bound. It never holds db.mu, so background flush/compaction keeps running
// while a writer waits.
func (db *DB) throttleWrite() {
	if db.opts.ReadOnly {
		return
	}
	slow, stop := db.opts.L0SlowdownTables, db.opts.L0StopTables
	if slow <= 0 && stop <= 0 {
		return
	}
	stalled := false
	// Hard stop: block while the fresh tier is at or above the stop threshold,
	// re-signalling the scheduler so it drains, until it falls below stop.
	if stop > 0 {
		for db.eng.depth0Count() >= stop {
			if !stalled {
				db.metrics.writeStalls.Add(1)
				stalled = true
			}
			db.mu.RLock()
			closed := db.closed
			db.mu.RUnlock()
			// Stop waiting if the DB closed or a background compaction/flush failure
			// is latched: the tier will never drain, so let writeBatch surface the
			// error instead of spinning here forever.
			if closed || db.backgroundError() != nil {
				return
			}
			db.sched.Signal()
			time.Sleep(writeStopPoll)
		}
	}
	// Soft slowdown: a single brief delay when the fresh tier is over the mark
	// (skip if the hard path already stalled this write).
	if !stalled && slow > 0 && db.eng.depth0Count() >= slow {
		db.metrics.writeStalls.Add(1)
		time.Sleep(writeSlowdownDelay)
	}
}

func (db *DB) writeBatch(b *Batch) error {
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
	// The whole batch is applied to the memtable before any flush check, so bound
	// the cumulative bytes as well as each entry. Both limits keep the skiplist
	// arena (uint32 offsets) well below 2^32.
	var total int64
	for i, op := range b.ops {
		if len(op.key) == 0 {
			return ErrEmptyKey
		}
		if op.rangeDel {
			// A range delete needs a non-empty exclusive end strictly above start.
			if len(op.value) == 0 || bytes.Compare(op.key, op.value) >= 0 {
				return ErrInvalidRange
			}
			entrySize := len(op.key) + len(op.value)
			if entrySize > maxEntrySize {
				return ErrEntryTooLarge
			}
			total += int64(entrySize)
			if total > int64(maxEntrySize) {
				return ErrBatchTooLarge
			}
			entries[i] = walEntry{Kind: walKindRangeDelete, Key: op.key, Value: op.value}
			continue
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
		total += int64(entrySize)
		if total > int64(maxEntrySize) {
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
			Kind:      kind,
			Key:       op.key,
			Value:     op.value,
			ExpiresAt: expiresAt,
		}
	}

	if err := db.appendWAL(entries); err != nil {
		db.setBackgroundError(err)
		return err
	}
	return nil
}

// reserveSeq atomically reserves n consecutive global sequence numbers and
// returns the first. It fails rather than wrap the 56-bit ikey sequence space.
func (db *DB) reserveSeq(n uint64) (uint64, error) {
	for {
		cur := db.walSeq.Load()
		if cur > maxIKeySeq || n > maxIKeySeq-cur {
			return 0, fmt.Errorf("levisdb: sequence number exhausted")
		}
		if db.walSeq.CompareAndSwap(cur, cur+n) {
			return cur + 1, nil
		}
	}
}

// appendWAL assigns the batch a contiguous seq range and enqueues it to the
// single WAL under db.seqMu, so records are written and recovered in ascending
// seq order. The fsync/commit runs outside seqMu, so a slow commit does not
// serialize the next batch's enqueue. readSeq advances only after the batch
// applies (publishSeq), so a snapshot never observes it half-applied.
func (db *DB) appendWAL(entries []walEntry) error {
	db.seqMu.Lock()
	base, err := db.reserveSeq(uint64(len(entries)))
	if err != nil {
		db.seqMu.Unlock()
		return err
	}
	for i := range entries {
		entries[i].Seq = base + uint64(i)
	}
	pb, mustCommit, err := db.wal.wal.enqueue(entries)
	db.seqMu.Unlock()
	if err != nil {
		return err
	}
	return db.wal.wal.runCommit(pb, mustCommit)
}

// maxWALSegmentSize returns the current WAL segment byte count, the checkpoint
// trigger.
func (db *DB) maxWALSegmentSize() int64 {
	if db.wal == nil {
		return 0
	}
	return db.wal.wal.currentSize()
}

// maybeCheckpoint rotates and retires the WAL once it grows past
// walSegmentBytes. It runs the checkpoint on a background goroutine so a writer
// never blocks on the flush barrier; write backpressure (throttleWrite) already
// bounds how far ahead writes can get. Only one checkpoint runs at a time
// (checkpointRunning), and Close/crash wait for an in-flight one via
// checkpointWG. The synchronous force path (prepareCompactionFilter) still calls
// checkpointWALMode directly.
func (db *DB) maybeCheckpoint() {
	if walSegmentBytes <= 0 || db.opts.ReadOnly {
		return
	}
	size := db.maxWALSegmentSize()
	if size < walSegmentBytes {
		return
	}
	// Single-flight: at most one checkpoint runs at a time. If one is already
	// running, the write normally proceeds without blocking (async) - but if the
	// live WAL has grown past the hard cap, the writer has outrun the background
	// checkpoint, so stall here until it drains rather than let the WAL grow
	// without bound. This bounds the live WAL (and crash-recovery cost) at roughly
	// walCheckpointStallMultiple * walSegmentBytes.
	if !db.checkpointRunning.CompareAndSwap(false, true) {
		for size >= walSegmentBytes*walCheckpointStallMultiple {
			db.mu.RLock()
			closed := db.closed
			db.mu.RUnlock()
			if closed || db.backgroundError() != nil {
				return
			}
			if !db.checkpointRunning.Load() {
				return
			}
			time.Sleep(writeStopPoll)
			size = db.maxWALSegmentSize()
		}
		return
	}
	// Register the checkpoint under db.mu with the closed check, so Close/crash
	// (which set closed under db.mu.Lock before calling checkpointWG.Wait) never
	// race a WaitGroup.Add against their Wait: either we add before closed is set,
	// or we observe closed and do not add.
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		db.checkpointRunning.Store(false)
		return
	}
	db.checkpointWG.Add(1)
	db.mu.RUnlock()
	go func() {
		defer db.checkpointWG.Done()
		defer db.checkpointRunning.Store(false)
		if err := db.checkpointWAL(); err != nil && err != ErrClosed {
			db.setBackgroundError(err)
		}
	}()
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
	if !force && db.maxWALSegmentSize() < walSegmentBytes {
		return db.filterSafeSeq.Load(), nil
	}
	if force && db.readSeq.Load() <= db.filterSafeSeq.Load() {
		return db.filterSafeSeq.Load(), nil
	}
	ckStart := time.Now()
	db.log.Debug("wal checkpoint started",
		"op", "checkpoint", "force", force, "wal_bytes", db.maxWALSegmentSize())
	// Rotate the WAL to a fresh segment, recording the retired segment.
	num := db.alloc.Next()
	if num == 0 {
		return db.filterSafeSeq.Load(), ErrFileNumberExhausted
	}
	path, err := db.store.logPath(num)
	if err != nil {
		return db.filterSafeSeq.Load(), err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return db.filterSafeSeq.Load(), err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		_ = f.Close()
		_ = db.store.removeLog(num)
		return db.filterSafeSeq.Load(), err
	}
	oldNum := db.wal.num
	oldFile, err := db.wal.wal.rotate(f)
	if err != nil {
		_ = f.Close()
		_ = removeFileDurable(path)
		return db.filterSafeSeq.Load(), err
	}
	db.wal.file = f
	db.wal.num = num
	db.wal.retired = append(db.wal.retired, oldNum)
	if err := oldFile.Close(); err != nil {
		return db.filterSafeSeq.Load(), err
	}

	// readSeq is a true contiguous watermark: a writer holds db.mu.RLock across
	// its whole batch (apply then publish), and there is one commit stream, so
	// once the current writers drain past apply every seq <= readSeq is applied.
	// Take db.mu exclusively for the instant of the read to drain in-flight
	// writers, so the cutoff never precedes an un-applied write; without it a
	// crash could replay a value a compaction filter dropped from a table.
	db.mu.Lock()
	cutoff := db.readSeq.Load()
	db.mu.Unlock()

	// Two passes cover the case where an immutable memtable already existed when
	// the checkpoint began: the first drains it, the second captures the active
	// table that contains every write from the retired segment.
	if err := db.eng.Flush(); err != nil {
		return db.filterSafeSeq.Load(), err
	}
	if err := db.eng.Flush(); err != nil {
		return db.filterSafeSeq.Load(), err
	}
	if err := db.backgroundError(); err != nil {
		return db.filterSafeSeq.Load(), err
	}
	// Flushed through cutoff; the old segments are now captured in tables. With
	// retention off they are deleted immediately. With retention on they are left
	// on disk for GetUpdatesSince and the disk-driven reaper deletes them once past
	// the age/size horizon.
	retain := db.opts.WALRetention > 0 || db.opts.WALRetentionBytes > 0
	for len(db.wal.retired) > 0 {
		segNum := db.wal.retired[0]
		if !retain {
			if err := db.store.removeLog(segNum); err != nil {
				return db.filterSafeSeq.Load(), err
			}
		}
		db.wal.retired = db.wal.retired[1:]
	}
	if retain {
		db.reapRetainedWAL(db.wal.num)
	}
	// Advance filterSafeSeq only forward: a concurrent force checkpoint may already
	// have stored a higher cutoff, so never move it backward.
	safe := db.filterSafeSeq.Load()
	if cutoff > safe {
		db.filterSafeSeq.Store(cutoff)
		safe = cutoff
	}
	db.log.Debug("wal checkpoint done",
		"op", "checkpoint", "cutoff", cutoff, "filter_safe_seq", safe, "dur", time.Since(ckStart))
	return safe, nil
}

// applyCommitted installs one durable WAL batch into the memtable and advances
// readSeq over it. The single WAL commits batches in seq order, so readSeq is a
// simple forward store and a snapshot never observes a half-applied batch.
func (db *DB) applyCommitted(entries []walEntry) {
	if len(entries) == 0 {
		return
	}
	for i := range entries {
		e := &entries[i]
		switch e.Kind {
		case walKindDelete:
			db.eng.del(e.Seq, e.Key)
		case walKindRangeDelete:
			db.eng.delRange(e.Seq, e.Key, e.Value)
		case walKindPutTTL:
			db.eng.putTTL(e.Seq, e.Key, e.Value, e.ExpiresAt)
		default:
			db.eng.Put(e.Seq, e.Key, e.Value)
		}
	}
	db.publishSeq(entries[len(entries)-1].Seq)
	// Signal a flush if the memtable reached the threshold; the scheduler drains
	// it off this goroutine so writes are not blocked by flush or compaction.
	if db.eng.needFlush() {
		db.sched.Signal()
	}
}

// publishSeq advances readSeq to seq. Batches commit in seq order on the single
// WAL, so a forward CAS keeps readSeq a true "all seqs <= readSeq are applied"
// watermark.
func (db *DB) publishSeq(seq uint64) {
	for {
		cur := db.readSeq.Load()
		if seq <= cur {
			return
		}
		if db.readSeq.CompareAndSwap(cur, seq) {
			return
		}
	}
}

// flushEngine flushes the memtable and runs any ready compaction, recording each
// change in the manifest. It is the scheduler's work unit.
func (db *DB) flushEngine() {
	s := db.eng

	// A writer can fill the new active memtable while an older immutable is being
	// flushed. Such writes cannot signal another flush until the immutable is
	// installed, so re-check here and drain every over-limit active table.
	for {
		logFlush := db.log.Enabled(context.Background(), slog.LevelDebug)
		var memSize int64
		var start time.Time
		if logFlush {
			memSize = s.memSize()
			start = time.Now()
		}
		if err := s.Flush(); err != nil {
			db.log.Error("flush failed", "op", "flush", "err", err)
			db.setBackgroundError(err)
			return
		}
		if logFlush {
			_, l0Bytes := s.tierTableStats(0)
			db.log.Debug("memtable flushed",
				"op", "flush", "memtable_bytes", memSize,
				"l0_tables", s.tierTableCount(0), "l0_bytes", l0Bytes, "dur", time.Since(start))
		}
		if !s.needFlush() {
			break
		}
	}

	// Drain all ready tiers so a burst of flushes does not leave the engine
	// permanently over the tier ratio, and reclaim delete-heavy tiers early.
	for {
		// A tombstone can only be dropped when the retained sequence covers it.
		// While a live snapshot pins the oldest retained sequence below the
		// committed one, a tombstone-triggered compaction reclaims nothing and
		// merely relocates the delete-heavy tier one level deeper, where it
		// re-fires without bound. Gate the trigger off in that state; count-based
		// compaction still runs.
		tombstoneRatio := db.opts.TombstoneCompactionRatio
		if db.snaps.oldest(db.readSeq.Load()) < db.readSeq.Load() {
			tombstoneRatio = 0
		}
		depth := s.pickCompaction(db.opts.TierRatio, db.opts.TierByteTrigger, tombstoneRatio)
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
		logCompaction := db.log.Enabled(context.Background(), slog.LevelDebug)
		var startTables int
		var start time.Time
		if logCompaction {
			startTables = s.tierTableCount(depth)
			start = time.Now()
			db.log.Debug("compaction started",
				"op", "compaction", "depth", depth, "input_tables", startTables)
		}
		err = s.Compact(depth, retain, cc)
		release()
		if err != nil {
			db.log.Error("compaction failed", "op", "compaction", "depth", depth, "err", err)
			db.setBackgroundError(err)
			break
		}
		if logCompaction {
			outTables, outBytes := s.tierTableStats(depth + 1)
			db.log.Debug("compaction done",
				"op", "compaction", "depth", depth,
				"input_tables", startTables, "output_tables", outTables,
				"output_bytes", outBytes, "dur", time.Since(start))
		}
	}
}

// CompactRange forces a full compaction, collapsing all tiers so tombstones and
// dead versions are reclaimed without waiting for the tier threshold. The
// start/end bounds are advisory: the whole keyspace is compacted regardless.
// Versions a live snapshot may still read are retained. It blocks until done and
// is serialized against background compaction.
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
	return db.compactEngineFully()
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

// compactEngineFully flushes then merges all tables into sized bottom-tier
// outputs, reclaiming tombstones and dead versions.
func (db *DB) compactEngineFully() error {
	if err := db.eng.Flush(); err != nil {
		return err
	}

	retain, cc, release, err := db.compactionRunConfig(false)
	if err != nil {
		return err
	}
	err = db.eng.CompactAll(retain, cc)
	release()
	return err
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
