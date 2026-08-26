package levisdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type blockingSecondShardCall struct {
	mu      sync.Mutex
	calls   int
	blocked chan struct{}
	release chan struct{}
}

func (p *blockingSecondShardCall) Shard([]byte, int) int {
	p.mu.Lock()
	p.calls++
	call := p.calls
	p.mu.Unlock()
	if call == 2 {
		close(p.blocked)
		<-p.release
	}
	return 0
}

func (*blockingSecondShardCall) Name() string { return "blocking-second-shard-call" }

func TestPointReadPinsCapturedSequenceAgainstCompaction(t *testing.T) {
	for _, tc := range []struct {
		name string
		read func(*DB) error
	}{
		{
			name: "Get",
			read: func(db *DB) error {
				value, err := db.Get([]byte("key"))
				if err == nil && !bytes.Equal(value, []byte("old")) {
					return fmt.Errorf("Get returned %q", value)
				}
				return err
			},
		},
		{
			name: "Has",
			read: func(db *DB) error {
				has, err := db.Has([]byte("key"))
				if err == nil && !has {
					return fmt.Errorf("Has returned false")
				}
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			part := &blockingSecondShardCall{
				blocked: make(chan struct{}),
				release: make(chan struct{}),
			}
			db := openTestDB(t, func(o *Options) {
				o.ShardCount = 1
				o.MemtableSize = 1 << 30
				o.CustomPartitioner = part
			})
			released := false
			defer func() {
				if !released {
					close(part.release)
				}
			}()
			require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("old")}))
			require.NoError(t, db.shards[0].Flush())

			result := make(chan error, 1)
			go func() { result <- tc.read(db) }()
			<-part.blocked // the read captured and pinned sequence one

			require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("new")}))
			require.NoError(t, db.shards[0].Flush())
			require.NoError(t, db.CompactShard(0))
			close(part.release)
			released = true
			require.NoError(t, <-result)
			assert.Zero(t, db.snaps.live(), "point-read sequence pin must be released")
		})
	}
}

func TestRecoverWALAcrossJournalFlushBoundary(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openAt(t, dir, 1<<30)
	wantA := make([]byte, 10<<10)
	wantB := make([]byte, 30<<10)
	for i := range wantA {
		wantA[i] = byte(i)
	}
	for i := range wantB {
		wantB[i] = byte(i * 3)
	}
	require.NoError(t, db.Put(PutOptions{Key: []byte("a"), Value: wantA}))
	require.NoError(t, db.Put(PutOptions{Key: []byte("b"), Value: wantB}))
	db.crash()

	db, err := Open(func() Options {
		o := DefaultOptions(dir)
		o.ShardCount = 4
		o.MemtableSize = 1 << 30
		return o
	}())
	require.NoError(t, err)
	defer db.Close()
	gotA, err := db.Get([]byte("a"))
	require.NoError(t, err)
	gotB, err := db.Get([]byte("b"))
	require.NoError(t, err)
	assert.Equal(t, wantA, gotA)
	assert.Equal(t, wantB, gotB)
}

// TestConcurrentWritersSurviveCrash guards against silent data loss when many
// goroutines write to one shard and the process crashes. The global sequence is
// assigned inside each shard WAL's append (under the enqueue lock), so a shard's
// records are physically ordered by seq. If seq were assigned before the append
// (a global pre-reservation), two writers to one shard could enqueue in the
// opposite order to their seqs; crash recovery treats a regressing seq within a
// shard as corruption and drops every record after it - here that would silently
// lose most of the writes. All must survive.
func TestConcurrentWritersSurviveCrash(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := func() Options {
		o := DefaultOptions(dir)
		o.ShardCount = 1 // one shard so all writers share one WAL and can invert
		o.NoSync = true
		o.MemtableSize = 1 << 30 // keep everything in the WAL until the crash
		return o
	}
	db, err := Open(opts())
	require.NoError(t, err)

	const writers = 8
	const perWriter = 100
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := []byte(fmt.Sprintf("w%02d-k%05d", w, i))
				require.NoError(t, db.Put(PutOptions{Key: key, Value: key}))
			}
		}(w)
	}
	wg.Wait()
	db.crash()

	db, err = Open(opts())
	require.NoError(t, err)
	defer db.Close()
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			key := []byte(fmt.Sprintf("w%02d-k%05d", w, i))
			got, gerr := db.Get(key)
			require.NoErrorf(t, gerr, "lost key %s after crash+recovery", key)
			assert.Equal(t, key, got)
		}
	}
}

// TestMultiShardBatchVisibleAtomically is a concurrent -race smoke check for
// cross-shard batch atomicity: a two-shard batch must never be seen half-applied
// by a concurrent snapshot, while a noise writer to a third shard interleaves
// seqs between the batch's shards. The deterministic guarantee lives in
// TestReadSeqWatermarkHoldsForUnappliedGap; this exercises the same paths under
// real concurrency and the race detector, kept lean with bounded loops.
func TestMultiShardBatchVisibleAtomically(t *testing.T) {
	t.Parallel()
	o := DefaultOptions(t.TempDir())
	o.ShardCount = 3
	o.NoSync = true
	o.MemtableSize = 1 << 30
	o.Partitioner = PartitionerRange
	db, err := Open(o)
	require.NoError(t, err)
	defer db.Close()

	shardKey := func(want int) []byte {
		for i := 0; i < 256; i++ {
			k := []byte{byte(i)}
			if s, _ := db.shardForKey(k); s == want {
				return k
			}
		}
		return nil
	}
	k0, k1, kn := shardKey(0), shardKey(1), shardKey(2)
	require.NotNil(t, k0)
	require.NotNil(t, k1)
	require.NotNil(t, kn)

	stop := make(chan struct{})
	var torn int64
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = db.Put(PutOptions{Key: kn, Value: []byte(fmt.Sprintf("n%d", i))})
			runtime.Gosched()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for s := 0; s < 800; s++ {
			snap, e := db.Snapshot()
			if e != nil {
				return
			}
			_, e0 := snap.Get(k0)
			_, e1 := snap.Get(k1)
			if (e0 == nil) != (e1 == nil) {
				atomic.AddInt64(&torn, 1)
			}
			snap.Release()
			runtime.Gosched()
		}
	}()

	for i := 0; i < 300; i++ {
		var b Batch
		b.Put(PutOptions{Key: k0, Value: []byte(fmt.Sprintf("a%d", i))})
		b.Put(PutOptions{Key: k1, Value: []byte(fmt.Sprintf("b%d", i))})
		require.NoError(t, db.Write(&b))
	}
	close(stop)
	wg.Wait()
	assert.Zero(t, atomic.LoadInt64(&torn), "a two-shard batch was observed half-applied")
}

// TestReadSeqWatermarkHoldsForUnappliedGap deterministically guards the readSeq
// watermark: while a multi-shard batch is mid-apply (one shard gated), a
// concurrent single-shard write with a HIGHER seq must NOT advance readSeq past
// the gated batch. publishRange advances readSeq only over the fully-applied
// contiguous prefix, so the concurrent write's range is buffered until the gap
// fills. A plain CAS-max publish would expose the gated batch half-applied.
func TestReadSeqWatermarkHoldsForUnappliedGap(t *testing.T) {
	t.Parallel()
	o := DefaultOptions(t.TempDir())
	o.ShardCount = 3
	o.NoSync = true
	o.MemtableSize = 1 << 30
	o.Partitioner = PartitionerRange
	db, err := Open(o)
	require.NoError(t, err)
	defer db.Close()

	shardKey := func(want int) []byte {
		for i := 0; i < 256; i++ {
			k := []byte{byte(i)}
			if s, _ := db.shardForKey(k); s == want {
				return k
			}
		}
		return nil
	}
	k0, k2, kn := shardKey(0), shardKey(2), shardKey(1)
	require.NotNil(t, k0)
	require.NotNil(t, k2)
	require.NotNil(t, kn)

	// Gate shard 2's first apply so the batch cannot finish.
	orig2 := db.wals[2].wal.apply
	gate := make(chan struct{})
	var once bool
	db.wals[2].wal.apply = func(e []walEntry) {
		if !once {
			once = true
			<-gate
		}
		orig2(e)
	}

	bdone := make(chan struct{})
	go func() {
		var b Batch
		b.Put(PutOptions{Key: k0, Value: []byte("A")})
		b.Put(PutOptions{Key: k2, Value: []byte("C")})
		_ = db.Write(&b)
		close(bdone)
	}()
	time.Sleep(50 * time.Millisecond) // batch stuck applying shard 2

	// A higher-seq single-shard write completes fully.
	require.NoError(t, db.Put(PutOptions{Key: kn, Value: []byte("N")}))

	// readSeq must not have advanced past the gated batch: a snapshot must see
	// neither the batch's applied half nor tear.
	snap, err := db.Snapshot()
	require.NoError(t, err)
	_, e0 := snap.Get(k0)
	_, e2 := snap.Get(k2)
	assert.Equal(t, e0 == nil, e2 == nil, "gated batch must not be visible half-applied")
	assert.NotNil(t, e0, "gated batch must be fully invisible while an unapplied seq precedes readSeq")
	snap.Release()

	close(gate)
	<-bdone

	// Once the gate opens everything is visible (read-your-writes).
	_, e0 = db.Get(k0)
	_, e2 = db.Get(k2)
	_, en := db.Get(kn)
	require.NoError(t, e0)
	require.NoError(t, e2)
	require.NoError(t, en)
}

// TestAbortedBatchDoesNotStallWatermark guards the readSeq watermark against a
// permanent stall when a multi-shard batch aborts mid-fan-out. assignSeq reserves
// the batch's contiguous range before the per-shard enqueue; if a later shard's
// enqueue fails, the reserved range still has appendToShardWALs as its sole
// publisher, so it MUST publish the range even on error - otherwise readSeq stalls
// below that range forever and a concurrent healthy writer's acked write (a higher
// range buffered in pendingRanges) is never made visible. After the fix readSeq
// advances to walSeq even though the batch failed.
func TestAbortedBatchDoesNotStallWatermark(t *testing.T) {
	o := DefaultOptions(t.TempDir())
	o.ShardCount = 3
	o.NoSync = true
	o.MemtableSize = 1 << 30
	o.Partitioner = PartitionerRange
	db, err := Open(o)
	require.NoError(t, err)
	defer db.Close()

	shardKey := func(want int) []byte {
		for i := 0; i < 256; i++ {
			k := []byte{byte(i)}
			if s, _ := db.shardForKey(k); s == want {
				return k
			}
		}
		return nil
	}
	k0, k2 := shardKey(0), shardKey(2)
	require.NotNil(t, k0)
	require.NotNil(t, k2)

	// Poison shard 2's WAL so the two-shard batch aborts after shard 0 is enqueued.
	w2 := db.wals[2].wal
	w2.mu.Lock()
	w2.err = errors.New("injected wal failure")
	w2.mu.Unlock()

	var b Batch
	b.Put(PutOptions{Key: k0, Value: []byte("a")})
	b.Put(PutOptions{Key: k2, Value: []byte("c")})
	require.Error(t, db.Write(&b), "batch must fail when a shard WAL is poisoned")

	// The reserved range was published (gap-filled) on abort, so the watermark did
	// not stall below the reservation counter.
	assert.Equal(t, db.walSeq.Load(), db.readSeq.Load(),
		"readSeq must not stall below walSeq after an aborted batch")
}

func TestWALAppliesConcurrentBatchesInCommitOrder(t *testing.T) {
	// One shard so both writes share a single WAL committer: the ordering
	// guarantee is per-shard now, so this exercises it within a shard.
	db := openTestDB(t, func(o *Options) { o.ShardCount = 1; o.MemtableSize = 1 << 30 })
	originalApply := db.wals[0].wal.apply
	entered := make(chan struct{})
	release := make(chan struct{})
	db.wals[0].wal.apply = func(entries []walEntry) {
		if len(entries) > 1 {
			close(entered)
			<-release
		}
		originalApply(entries)
	}

	var large Batch
	for i := 0; i < 100; i++ {
		large.Put(PutOptions{Key: []byte(fmt.Sprintf("bulk-%03d", i)), Value: []byte("v")})
	}
	largeDone := make(chan error, 1)
	go func() { largeDone <- db.Write(&large) }()
	<-entered

	laterDone := make(chan error, 1)
	go func() { laterDone <- db.Put(PutOptions{Key: []byte("later"), Value: []byte("v")}) }()
	select {
	case err := <-laterDone:
		require.NoError(t, err)
		t.Fatal("later batch returned before the earlier batch was fully applied")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-largeDone)
	require.NoError(t, <-laterDone)
	for i := 0; i < 100; i++ {
		_, err := db.Get([]byte(fmt.Sprintf("bulk-%03d", i)))
		require.NoError(t, err)
	}
}

func TestConcurrentIteratorAndWrites(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.ShardCount = 1
		o.MemtableSize = 1 << 30
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k-%04d", i)), Value: []byte("value")}))
		}
	}()
	for pass := 0; pass < 40; pass++ {
		it, err := db.NewIterator()
		require.NoError(t, err)
		var prev []byte
		for it.Next() {
			key := append([]byte(nil), it.Key()...)
			if prev != nil {
				assert.Less(t, string(prev), string(key))
			}
			prev = key
		}
		require.NoError(t, it.Error())
		require.NoError(t, it.Close())
	}
	wg.Wait()
}

func TestCleanCloseRetiresWALWithoutDuplicateTables(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	o := DefaultOptions(dir)
	o.ShardCount = 1
	o.MemtableSize = 1 << 30
	db, err := Open(o)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
	require.NoError(t, db.Close())

	s, err := openStorage(dir)
	require.NoError(t, err)
	logs, err := s.listLogs(0)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	assert.Empty(t, logs)

	db, err = Open(o)
	require.NoError(t, err)
	st, err := db.Stats()
	require.NoError(t, err)
	assert.Equal(t, 1, st.Tables)
	require.NoError(t, db.Close())
}

type fileFingerprint struct {
	size int64
	mode os.FileMode
	mod  time.Time
}

func fingerprintTree(t *testing.T, root string) map[string]fileFingerprint {
	t.Helper()
	out := map[string]fileFingerprint{}
	require.NoError(t, filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		out[rel] = fileFingerprint{size: info.Size(), mode: info.Mode(), mod: info.ModTime()}
		return nil
	}))
	return out
}

func TestReadOnlyOpenDoesNotMutateRecoveryState(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openAt(t, dir, 1<<30)
	require.NoError(t, db.Put(PutOptions{Key: []byte("wal-only"), Value: []byte("value")}))
	db.crash()
	before := fingerprintTree(t, dir)

	o := DefaultOptions(dir)
	o.ShardCount = 4
	o.MemtableSize = 1 << 30
	o.ReadOnly = true
	ro, err := Open(o)
	require.NoError(t, err)
	v, err := ro.Get([]byte("wal-only"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), v)
	require.NoError(t, ro.Close())

	assert.Equal(t, before, fingerprintTree(t, dir))
}

func TestReadOnlyLargeWALReplayStaysInMemory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeOpts := DefaultOptions(dir)
	writeOpts.ShardCount = 1
	writeOpts.MemtableSize = 1 << 30
	// The crash helper syncs the WAL before closing it. Avoid an unrelated fsync
	// for every setup write so this recovery regression stays within the package's
	// test-duration budget.
	writeOpts.NoSync = true
	db, err := Open(writeOpts)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("changing"), Value: []byte("old")}))
	for i := 0; i < 200; i++ {
		require.NoError(t, db.Put(PutOptions{
			Key:   []byte(fmt.Sprintf("key-%04d", i)),
			Value: []byte("value-payload"),
		}))
		if i == 99 {
			require.NoError(t, db.Delete([]byte("changing")))
		}
	}
	require.NoError(t, db.Put(PutOptions{Key: []byte("changing"), Value: []byte("new")}))
	db.crash()
	before := fingerprintTree(t, dir)

	readOpts := writeOpts
	readOpts.MemtableSize = 128
	readOpts.ReadOnly = true
	ro, err := Open(readOpts)
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		value, err := ro.Get([]byte(fmt.Sprintf("key-%04d", i)))
		require.NoError(t, err, i)
		assert.Equal(t, []byte("value-payload"), value)
	}
	value, err := ro.Get([]byte("changing"))
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), value)
	has, err := ro.Has([]byte("changing"))
	require.NoError(t, err)
	assert.True(t, has)
	it, err := ro.NewIterator()
	require.NoError(t, err)
	count := 0
	for it.Next() {
		count++
	}
	require.NoError(t, it.Error())
	require.NoError(t, it.Close())
	assert.Equal(t, 201, count)
	require.NoError(t, ro.Close())

	assert.Equal(t, before, fingerprintTree(t, dir))
}

func TestWALCheckpointBoundsSegmentsAndSurvivesCrash(t *testing.T) {
	oldLimit := walSegmentBytes
	walSegmentBytes = 1024
	defer func() { walSegmentBytes = oldLimit }()

	dir := t.TempDir()
	o := DefaultOptions(dir)
	o.ShardCount = 2
	o.MemtableSize = 1 << 30
	db, err := Open(o)
	require.NoError(t, err)
	value := make([]byte, 700)
	for i := 0; i < 20; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("key-%02d", i)), Value: value}))
	}
	// Checkpointing is asynchronous: writes trigger it but do not block on it, and
	// the single-flight guard may skip a trigger while one is in flight. Wait for
	// any in-flight checkpoint, then force a final synchronous one so the last
	// segment is retired before we assert on the WAL directory.
	db.checkpointWG.Wait()
	require.NoError(t, db.checkpointWAL())
	// Per-shard WALs: after a checkpoint each shard keeps exactly its one live
	// segment and every rotated-out segment is retired.
	for shard := 0; shard < o.ShardCount; shard++ {
		logs, err := db.store.listLogs(shard)
		require.NoError(t, err)
		require.Len(t, logs, 1, "checkpoint must retire every old segment")
	}
	db.crash()

	db, err = Open(o)
	require.NoError(t, err)
	defer db.Close()
	for i := 0; i < 20; i++ {
		got, err := db.Get([]byte(fmt.Sprintf("key-%02d", i)))
		require.NoError(t, err)
		assert.Equal(t, value, got)
	}
}

// TestWALStallsWhenCheckpointLagsBehind guards the WAL-size cap on the async
// checkpoint: when a checkpoint is already in flight and the live WAL has grown
// past walCheckpointStallMultiple * walSegmentBytes, maybeCheckpoint must block
// the writer until the in-flight checkpoint drains, rather than let the WAL grow
// without bound. It drives the guard deterministically by holding a fake
// in-flight checkpoint, so it does not depend on flush timing.
func TestWALStallsWhenCheckpointLagsBehind(t *testing.T) {
	oldLimit := walSegmentBytes
	walSegmentBytes = 4096 // small so a few writes push the live WAL over the cap

	o := DefaultOptions(t.TempDir())
	o.ShardCount = 1
	o.MemtableSize = 1 << 30
	o.NoSync = true
	db, err := Open(o)
	require.NoError(t, err)
	// Close and restore the global while still inside the test body (not via a
	// deferred/Cleanup restore that could run while background goroutines still
	// read walSegmentBytes, which the race detector flags).
	defer func() {
		db.Close()
		walSegmentBytes = oldLimit
	}()

	// Simulate a slow in-flight checkpoint by holding the single-flight guard and
	// the WaitGroup, so no real checkpoint can fire and retire the WAL.
	require.True(t, db.checkpointRunning.CompareAndSwap(false, true))
	db.checkpointWG.Add(1)

	// Grow the live WAL past the cap. maybeCheckpoint is called by each Put; with
	// the checkpoint guard held, its over-cap branch would stall the writer, so
	// grow the WAL by appending to the WAL directly here (bypassing the Put path's
	// maybeCheckpoint) until it is over the cap.
	value := make([]byte, 2048)
	walCap := walSegmentBytes * walCheckpointStallMultiple
	sw := db.wals[0].wal
	for i := 0; sw.currentSize() < walCap; i++ {
		require.NoError(t, sw.append([]walEntry{{
			Shard: 0,
			Kind:  walKindPut,
			Key:   []byte(fmt.Sprintf("k%06d", i)),
			Value: value,
			Seq:   uint64(i + 1),
		}}))
		require.Less(t, i, 1_000_000, "WAL never reached the cap")
	}

	// maybeCheckpoint (called on the write path after each commit) must stall while
	// the WAL is over the cap and a checkpoint is in flight.
	blocked := make(chan struct{})
	go func() {
		db.maybeCheckpoint()
		close(blocked)
	}()

	select {
	case <-blocked:
		t.Fatal("maybeCheckpoint did not stall while the WAL was over the cap with a checkpoint in flight")
	case <-time.After(100 * time.Millisecond):
		// Still blocked, as required.
	}

	// Release the fake checkpoint; the stalled writer must now unblock.
	db.checkpointRunning.Store(false)
	db.checkpointWG.Done()
	select {
	case <-blocked:
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not unblock after the checkpoint drained")
	}
}

// TestWALStallReleasesWithoutWaitGroup guards the checkpoint stall path against a
// sync.WaitGroup reuse panic. The old stall waited on checkpointWG, but that
// WaitGroup is reused per checkpoint: a stalled writer's Wait() (holding no lock)
// could run concurrently with another writer's Add(1) the instant a checkpoint
// finished, which panics ("WaitGroup is reused before previous Wait has
// returned"). The fix polls checkpointRunning instead. This asserts the poll
// contract deterministically: clearing checkpointRunning alone must release the
// stall. The old Wait()-based code stayed blocked until checkpointWG.Done(), so
// this exercises exactly the code path that no longer touches the WaitGroup.
func TestWALStallReleasesWithoutWaitGroup(t *testing.T) {
	oldLimit := walSegmentBytes
	walSegmentBytes = 4096

	o := DefaultOptions(t.TempDir())
	o.ShardCount = 1
	o.MemtableSize = 1 << 30
	o.NoSync = true
	db, err := Open(o)
	require.NoError(t, err)
	defer func() {
		db.Close()
		walSegmentBytes = oldLimit
	}()

	// Fake an in-flight checkpoint via the single-flight guard ONLY - deliberately
	// NOT touching checkpointWG. The fix's stall polls checkpointRunning, so it must
	// block now and release when the guard clears, with no WaitGroup involved.
	require.True(t, db.checkpointRunning.CompareAndSwap(false, true))

	value := make([]byte, 2048)
	walCap := walSegmentBytes * walCheckpointStallMultiple
	sw := db.wals[0].wal
	for i := 0; sw.currentSize() < walCap; i++ {
		require.NoError(t, sw.append([]walEntry{{
			Shard: 0,
			Kind:  walKindPut,
			Key:   []byte(fmt.Sprintf("k%06d", i)),
			Value: value,
			Seq:   uint64(i + 1),
		}}))
		require.Less(t, i, 1_000_000, "WAL never reached the cap")
	}

	blocked := make(chan struct{})
	go func() {
		db.maybeCheckpoint()
		close(blocked)
	}()
	select {
	case <-blocked:
		t.Fatal("maybeCheckpoint did not stall while over the cap with a checkpoint in flight")
	case <-time.After(50 * time.Millisecond):
	}

	// Clearing the guard alone (no checkpointWG.Done) must release the poll-based
	// stall; the old Wait()-based stall would hang here.
	db.checkpointRunning.Store(false)
	select {
	case <-blocked:
	case <-time.After(3 * time.Second):
		t.Fatal("stall did not release when checkpointRunning cleared: it is still waiting on the WaitGroup")
	}
}

func TestBackgroundErrorIsStickyAndObservable(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	want := errors.New("disk stopped")
	db.setBackgroundError(want)
	err := db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")})
	assert.ErrorIs(t, err, want)
	st, err := db.Stats()
	require.NoError(t, err)
	assert.ErrorIs(t, st.BackgroundError, want)
	prop, err := db.GetProperty("levisdb.background-error")
	require.NoError(t, err)
	assert.Contains(t, prop, want.Error())
}

func TestDisableBlockCache(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.DisableBlockCache = true })
	assert.Zero(t, db.cache.capacity)
}

func TestCompactionFileSizeTargetsAndSplitting(t *testing.T) {
	t.Parallel()
	cc := testCompactionConfig()
	cc.FileSizeBase = 256
	cc.FileSizeMultiplier = 2
	cc.FileSizeMax = 700
	assert.Equal(t, int64(256), cc.targetFileSize(0))
	assert.Equal(t, int64(512), cc.targetFileSize(1))
	assert.Equal(t, int64(700), cc.targetFileSize(2))
	assert.Equal(t, int64(700), cc.targetFileSize(20))

	s := newTestShard(t, 1<<30)
	value := make([]byte, 128)
	const entriesPerTable = 10
	for table := 0; table < 2; table++ {
		for i := 0; i < entriesPerTable; i++ {
			key := []byte(fmt.Sprintf("t%d-key-%03d", table, i))
			s.Put(uint64(table*entriesPerTable+i+1), key, value)
		}
		require.NoError(t, s.Flush())
	}
	require.NoError(t, s.Compact(0, maxIKeySeq, cc))
	tables := s.Tables()
	assert.Greater(t, len(tables), 1)
	for _, table := range tables {
		assert.Equal(t, 1, table.Depth)
	}
}

func TestCompactionReadFailureKeepsInputs(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	flushSingle(t, s, 1, "a", "1")
	flushSingle(t, s, 2, "b", "2")
	before := s.Tables()
	require.Len(t, before, 2)
	s.mu.Lock()
	s.tables[0].reader.r = errReaderAt{}
	s.mu.Unlock()

	err := s.Compact(0, maxIKeySeq, testCompactionConfig())
	assert.ErrorIs(t, err, errRead)
	after := s.Tables()
	require.Len(t, after, 2)
	nums := []uint32{after[0].Num, after[1].Num}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	wantNums := []uint32{before[0].Num, before[1].Num}
	sort.Slice(wantNums, func(i, j int) bool { return wantNums[i] < wantNums[j] })
	assert.Equal(t, wantNums, nums)
	s.mu.RLock()
	paths := make([]string, len(s.tables))
	for i, table := range s.tables {
		paths[i] = table.path
	}
	s.mu.RUnlock()
	for _, path := range paths {
		_, statErr := os.Stat(path)
		assert.NoError(t, statErr)
	}
}

func TestFlushManifestFailureKeepsImmutableForRetry(t *testing.T) {
	t.Parallel()
	want := errors.New("manifest unavailable")
	s := newTestShard(t, 1<<20)
	s.cfg.Commit = func([]*tableMeta, []*tableMeta, func()) error { return want }
	s.Put(1, []byte("old"), []byte("value"))
	err := s.Flush()
	assert.ErrorIs(t, err, want)
	assert.Empty(t, s.Tables())
	ambiguousOutput, pathErr := s.tablePath(1)
	require.NoError(t, pathErr)
	assert.FileExists(t, ambiguousOutput, "an ambiguously committed manifest edit may reference this output")

	// Writes continue in the fresh active memtable while the failed immutable
	// remains available for a later retry.
	s.Put(2, []byte("new"), []byte("value"))
	s.cfg.Commit = nil
	require.NoError(t, s.Flush())
	require.NoError(t, s.Flush())
	assert.Equal(t, []byte("value"), mustGet(t, s, 10, "old"))
	assert.Equal(t, []byte("value"), mustGet(t, s, 10, "new"))
}

func TestCompactionManifestFailureKeepsSourceTables(t *testing.T) {
	t.Parallel()
	want := errors.New("manifest unavailable")
	s := newTestShard(t, 1<<20)
	flushSingle(t, s, 1, "a", "1")
	flushSingle(t, s, 2, "b", "2")
	before := s.Tables()
	s.cfg.Commit = func([]*tableMeta, []*tableMeta, func()) error { return want }

	err := s.Compact(0, maxIKeySeq, testCompactionConfig())
	assert.ErrorIs(t, err, want)
	assert.Equal(t, before, s.Tables())
	ambiguousOutput, pathErr := s.tablePath(3)
	require.NoError(t, pathErr)
	assert.FileExists(t, ambiguousOutput, "an ambiguously committed replacement may reference this output")
	assert.Equal(t, []byte("1"), mustGet(t, s, 10, "a"))
	assert.Equal(t, []byte("2"), mustGet(t, s, 10, "b"))
}

func TestIteratorPinsSequenceUntilClosed(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))

	it, err := db.NewIterator()
	require.NoError(t, err)
	assert.Equal(t, 1, db.snaps.live(), "ordinary iterator must pin its read sequence")
	require.NoError(t, it.Close())
	assert.Zero(t, db.snaps.live())

	snap, err := db.Snapshot()
	require.NoError(t, err)
	it, err = snap.NewIterator()
	require.NoError(t, err)
	assert.Equal(t, 2, db.snaps.live(), "snapshot and its iterator own independent pins")
	snap.Release()
	assert.Equal(t, 1, db.snaps.live())
	require.NoError(t, it.Close())
	assert.Zero(t, db.snaps.live())
}

func TestExhaustedIteratorReleasesSequencePin(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	it, err := db.NewIterator()
	require.NoError(t, err)
	assert.Equal(t, 1, db.snaps.live())
	assert.False(t, it.Next())
	assert.Zero(t, db.snaps.live())
	require.NoError(t, it.Close())
}

func TestCompactionDropsVersionAtRetentionBoundary(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	flushSingle(t, s, 1, "k", "v1")
	flushSingle(t, s, 3, "k", "v3")
	require.NoError(t, s.Compact(0, 3, testCompactionConfig()))

	count := 0
	for _, table := range s.tables {
		it := table.reader.newIterator()
		for it.Next() {
			count++
		}
		require.NoError(t, it.Error())
	}
	assert.Equal(t, 1, count, "a version exactly at retainSeq already satisfies the oldest snapshot")
	assert.Equal(t, []byte("v3"), mustGet(t, s, 3, "k"))
}

func TestCompactionDeduplicatesReplayedInternalKeys(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	flushSingle(t, s, 7, "k", "value")
	flushSingle(t, s, 7, "k", "value")
	require.NoError(t, s.Compact(0, maxIKeySeq, testCompactionConfig()))

	count := 0
	for _, table := range s.tables {
		it := table.reader.newIterator()
		for it.Next() {
			count++
		}
		require.NoError(t, it.Error())
	}
	assert.Equal(t, 1, count)
	assert.Equal(t, []byte("value"), mustGet(t, s, 7, "k"))
}

func TestDeepCompactionOutputCannotShadowNewerShallowTable(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	cc := testCompactionConfig()

	// Build two old depth-1 tables.
	flushSingle(t, s, 1, "value-key", "old")
	flushSingle(t, s, 2, "old-a", "value")
	require.NoError(t, s.Compact(0, maxIKeySeq, cc))
	flushSingle(t, s, 3, "deleted-key", "old")
	flushSingle(t, s, 4, "old-b", "value")
	require.NoError(t, s.Compact(0, maxIKeySeq, cc))

	// Leave genuinely newer mutations in a shallow table.
	s.Put(5, []byte("value-key"), []byte("new"))
	s.del(6, []byte("deleted-key"))
	require.NoError(t, s.Flush())

	// Compacting the two old depth-1 tables creates a file after the shallow
	// table. File creation order must not make its old versions win point reads.
	require.NoError(t, s.Compact(1, maxIKeySeq, cc))
	assert.Equal(t, []byte("new"), mustGet(t, s, maxIKeySeq, "value-key"))
	has, deleted, err := s.has(maxIKeySeq, []byte("deleted-key"))
	require.NoError(t, err)
	assert.True(t, has)
	assert.True(t, deleted)
	_, found, deleted, err := s.get(maxIKeySeq, []byte("deleted-key"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, deleted)
}

func TestReplayedOlderMemtableEntryCannotShadowNewerTable(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	s.Put(10, []byte("value-key"), []byte("new"))
	s.del(11, []byte("deleted-key"))
	require.NoError(t, s.Flush())

	// Recovery deliberately replays intact WAL entries even when an identical
	// or newer version is already manifest-backed. Model an older surviving WAL
	// prefix in the active memtable.
	s.Put(5, []byte("value-key"), []byte("old"))
	s.Put(6, []byte("deleted-key"), []byte("old"))

	assert.Equal(t, []byte("new"), mustGet(t, s, maxIKeySeq, "value-key"))
	_, found, deleted, err := s.get(maxIKeySeq, []byte("deleted-key"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, deleted)
	has, deleted, err := s.has(maxIKeySeq, []byte("deleted-key"))
	require.NoError(t, err)
	assert.True(t, has)
	assert.True(t, deleted)
}

func TestCompactAllRewritesSingleTable(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	s.Put(1, []byte("gone"), []byte("value"))
	s.del(2, []byte("gone"))
	require.NoError(t, s.Flush())
	require.Len(t, s.Tables(), 1)

	require.NoError(t, s.CompactAll(maxIKeySeq, testCompactionConfig()))
	assert.Empty(t, s.Tables(), "full compaction must reclaim a lone bottom-table tombstone")
	_, found, _, err := s.get(maxIKeySeq, []byte("gone"))
	require.NoError(t, err)
	assert.False(t, found)
}

func TestCloseWaitsForManualCompaction(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.ShardCount = 1
		o.MemtableSize = 1 << 30
		o.TierRatio = 100
	})
	require.NoError(t, db.Put(PutOptions{Key: []byte("a"), Value: []byte("1")}))
	require.NoError(t, db.shards[0].Flush())
	require.NoError(t, db.Put(PutOptions{Key: []byte("b"), Value: []byte("2")}))
	require.NoError(t, db.shards[0].Flush())

	originalCommit := db.shards[0].cfg.Commit
	entered := make(chan struct{})
	release := make(chan struct{})
	db.shards[0].cfg.Commit = func(inputs, outputs []*tableMeta, install func()) error {
		close(entered)
		<-release
		return originalCommit(inputs, outputs, install)
	}
	compactDone := make(chan error, 1)
	go func() { compactDone <- db.CompactShard(0) }()
	<-entered

	closeDone := make(chan error, 1)
	go func() { closeDone <- db.Close() }()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
		t.Fatal("Close returned while manual compaction still owned table and manifest resources")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-compactDone)
	require.NoError(t, <-closeDone)
}

func TestFailedRecoveryRemovesUnmanifestedTables(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Write session: a huge memtable so NOTHING flushes before the crash - every
	// write stays only in the WAL, so any .sst that exists after recovery is a
	// recovery output (never a pre-crash manifested table).
	writeOpts := DefaultOptions(dir)
	writeOpts.ShardCount = 2
	writeOpts.Partitioner = PartitionerRange
	writeOpts.MemtableSize = 1 << 30
	db, err := Open(writeOpts)
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte{0x01, byte(i)}, Value: []byte("left")}))
	}
	require.NoError(t, db.Put(PutOptions{Key: []byte{0xff}, Value: []byte("right")}))
	db.crash()

	// Corrupt shard 1's WAL segment so recovery of shard 1 fails; keep the original
	// bytes to restore for the successful reopen. Under StrictWALRecovery the
	// corruption aborts Open, and shard 0's recovery tables (flushed during replay,
	// not yet in a manifest) must be rolled back.
	s, err := openStorage(dir)
	require.NoError(t, err)
	logs1, err := s.listLogs(1)
	require.NoError(t, err)
	require.NotEmpty(t, logs1, "shard 1 must have a leftover WAL segment")
	seg1, err := s.logPath(1, logs1[0])
	require.NoError(t, err)
	require.NoError(t, s.Close())
	orig, err := os.ReadFile(seg1)
	require.NoError(t, err)
	require.Greater(t, len(orig), headerSize, "segment must hold a full record to corrupt")
	// Flip a byte in the record body (past the 7-byte chunk header) so the frame
	// stays intact but the CRC fails: a genuine corruption, not a torn tail (which
	// strict recovery still tolerates).
	corrupt := append([]byte(nil), orig...)
	corrupt[len(corrupt)-1] ^= 0xff
	require.NoError(t, os.WriteFile(seg1, corrupt, 0o644))

	// Recovery session: a tiny memtable so shard 0's replay flushes tables before
	// shard 1's corrupt segment aborts the Open. StrictWALRecovery makes the
	// corruption fail rather than salvage.
	recOpts := writeOpts
	recOpts.MemtableSize = 256
	recOpts.StrictWALRecovery = true
	_, err = Open(recOpts)
	require.Error(t, err)

	var tables []string
	require.NoError(t, filepath.WalkDir(filepath.Join(dir, shardsDir), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if filepath.Ext(entry.Name()) == ".sst" {
			tables = append(tables, path)
		}
		return nil
	}))
	assert.Empty(t, tables, "recovery outputs without a manifest must be rolled back")

	// Restore the uncorrupted segment; the reopen now recovers both shards.
	require.NoError(t, os.WriteFile(seg1, orig, 0o644))
	db, err = Open(recOpts)
	require.NoError(t, err)
	defer db.Close()
	left, err := db.Get([]byte{0x01, 0x00})
	require.NoError(t, err)
	right, err := db.Get([]byte{0xff})
	require.NoError(t, err)
	assert.Equal(t, []byte("left"), left)
	assert.Equal(t, []byte("right"), right)
}

func TestGetResultCannotMutateMemtable(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value")}))
	got, err := db.Get([]byte("key"))
	require.NoError(t, err)
	got[0] = 'X'

	again, err := db.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), again)
}

func TestReadOnlyLocksAreSharedAndExcludeWriter(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.ShardCount = 1
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	readOnly := opts
	readOnly.ReadOnly = true
	first, err := Open(readOnly)
	require.NoError(t, err)
	second, err := Open(readOnly)
	require.NoError(t, err)
	_, err = Open(opts)
	assert.Error(t, err, "writer must not open while shared readers hold the lock")
	require.NoError(t, first.Close())
	_, err = Open(opts)
	assert.Error(t, err, "remaining reader must still exclude the writer")
	require.NoError(t, second.Close())

	db, err = Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

type invalidShardPartitioner struct{ shard int }

func (p invalidShardPartitioner) Shard([]byte, int) int { return p.shard }
func (invalidShardPartitioner) Name() string            { return "invalid-shard-test-v1" }

func TestInvalidCustomPartitionerReturnsErrorsOnEveryPointAPI(t *testing.T) {
	t.Parallel()
	for _, shard := range []int{-1, 2} {
		t.Run(fmt.Sprintf("shard-%d", shard), func(t *testing.T) {
			opts := DefaultOptions(t.TempDir())
			opts.ShardCount = 2
			opts.CustomPartitioner = invalidShardPartitioner{shard: shard}
			db, err := Open(opts)
			require.NoError(t, err)
			defer db.Close()

			assert.ErrorContains(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}), "partitioner returned shard")
			_, err = db.Get([]byte("k"))
			assert.ErrorContains(t, err, "partitioner returned shard")
			_, err = db.Has([]byte("k"))
			assert.ErrorContains(t, err, "partitioner returned shard")
			snap, err := db.Snapshot()
			require.NoError(t, err)
			defer snap.Release()
			_, err = snap.Get([]byte("k"))
			assert.ErrorContains(t, err, "partitioner returned shard")
			_, err = snap.Has([]byte("k"))
			assert.ErrorContains(t, err, "partitioner returned shard")
		})
	}
}

func TestWritableOpenRemovesOnlyOrphanCanonicalTables(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.ShardCount = 1
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("live"), Value: []byte("value")}))
	require.NoError(t, db.Close())

	shardDir := filepath.Join(dir, shardsDir, "00")
	orphan := filepath.Join(shardDir, "00fffffe.sst")
	nonCanonical := filepath.Join(shardDir, "00FFFFFD.sst")
	unrelated := filepath.Join(shardDir, "keep.tmp")
	require.NoError(t, os.WriteFile(orphan, []byte("orphan"), 0o644))
	require.NoError(t, os.WriteFile(nonCanonical, []byte("not-engine-owned"), 0o644))
	require.NoError(t, os.WriteFile(unrelated, []byte("not-engine-owned"), 0o644))

	db, err = Open(opts)
	require.NoError(t, err)
	value, err := db.Get([]byte("live"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), value)
	assert.NoFileExists(t, orphan)
	assert.FileExists(t, nonCanonical)
	assert.FileExists(t, unrelated)
	require.NoError(t, db.Close())
}

func TestReadOnlyOpenPreservesOrphanTables(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.ShardCount = 1
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	shardDir, err := db.store.shardDir(0)
	require.NoError(t, err)
	orphan := filepath.Join(shardDir, "00fffffe.sst")
	require.NoError(t, os.WriteFile(orphan, []byte("orphan"), 0o644))

	opts.ReadOnly = true
	ro, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, ro.Close())
	assert.FileExists(t, orphan, "read-only open must never run orphan cleanup")
}

func TestFlushWorkerDrainsMemtableFilledDuringActiveFlush(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.ShardCount = 1
		o.MemtableSize = 1
		o.TierRatio = 100
	})

	originalCommit := db.shards[0].cfg.Commit
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	db.shards[0].cfg.Commit = func(inputs, outputs []*tableMeta, install func()) error {
		once.Do(func() {
			close(entered)
			<-release
		})
		return originalCommit(inputs, outputs, install)
	}

	require.NoError(t, db.Put(PutOptions{Key: []byte("first"), Value: []byte("value")}))
	<-entered
	require.NoError(t, db.Put(PutOptions{Key: []byte("second"), Value: []byte("value")}))
	close(release)
	db.sched.drain()

	assert.True(t, db.shards[0].memEmpty(), "worker must drain the over-limit replacement memtable without a third write")
	assert.Len(t, db.shards[0].Tables(), 2)
}

func TestLegacyUnversionedPartitionerIdentityIsRejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := openStorage(dir)
	require.NoError(t, err)
	manifest, err := createManifest(store.manifestPath(1), "hash", 1)
	require.NoError(t, err)
	require.NoError(t, manifest.Close())
	require.NoError(t, store.setCurrent(1))
	require.NoError(t, store.Close())

	opts := DefaultOptions(dir)
	opts.ShardCount = 1
	_, err = Open(opts)
	assert.ErrorIs(t, err, ErrPartitionerMismatch)
}
