package levisdb

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompactionFilterKeepsAndDiscardsValues(t *testing.T) {
	t.Parallel()
	var seen []CompactionFilterEntry
	db := openTestDB(t, func(o *Options) {
		o.ShardCount = 1
		o.MemtableSize = 1 << 30
		o.CompactionFilter = func(entry CompactionFilterEntry) bool {
			seen = append(seen, CompactionFilterEntry{
				Key:   append([]byte(nil), entry.Key...),
				Value: append([]byte(nil), entry.Value...),
				TTL:   entry.TTL,
			})
			keep := string(entry.Key) != "drop"
			// Callback input is isolated from the merge buffers.
			entry.Key[0] = 'X'
			entry.Value[0] = 'X'
			return keep
		}
	})

	// The discarded newest version must not expose this older value.
	require.NoError(t, db.Put(PutOptions{Key: []byte("drop"), Value: []byte("old")}))
	require.NoError(t, db.shards[0].Flush())
	require.NoError(t, db.Put(PutOptions{Key: []byte("drop"), Value: []byte("new")}))
	require.NoError(t, db.Put(PutOptions{Key: []byte("keep"), Value: []byte("value")}))
	require.NoError(t, db.Put(PutOptions{Key: []byte("ttl"), Value: []byte("temporary"), TTL: time.Hour}))

	require.NoError(t, db.CompactShard(0))
	_, err := db.Get([]byte("drop"))
	assert.ErrorIs(t, err, ErrNotFound)
	value, err := db.Get([]byte("keep"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), value)
	value, err = db.Get([]byte("ttl"))
	require.NoError(t, err)
	assert.Equal(t, []byte("temporary"), value)

	require.Len(t, seen, 3, "only the version selected for rewrite is filtered")
	assert.Equal(t, []byte("drop"), seen[0].Key)
	assert.Equal(t, []byte("new"), seen[0].Value)
	assert.Zero(t, seen[0].TTL)
	assert.Equal(t, []byte("keep"), seen[1].Key)
	assert.Equal(t, []byte("value"), seen[1].Value)
	assert.Zero(t, seen[1].TTL)
	assert.Equal(t, []byte("ttl"), seen[2].Key)
	assert.Equal(t, []byte("temporary"), seen[2].Value)
	assert.Positive(t, seen[2].TTL)
	assert.LessOrEqual(t, seen[2].TTL, time.Hour)
}

func TestCompactionFilterTTLAtCompactionCutoff(t *testing.T) {
	t.Parallel()
	t.Run("live", func(t *testing.T) {
		s := newTestShard(t, 1<<20)
		const expiresAt = int64(1_000)
		s.putTTL(1, []byte("key"), []byte("value"), expiresAt)
		require.NoError(t, s.Flush())

		var got CompactionFilterEntry
		cc := testCompactionConfig()
		cc.ExpireBefore = 900
		cc.Filter = func(entry CompactionFilterEntry) bool {
			got = entry
			return true
		}
		require.NoError(t, s.CompactAll(maxIKeySeq, cc))
		assert.Equal(t, []byte("key"), got.Key)
		assert.Equal(t, []byte("value"), got.Value)
		assert.Equal(t, 100*time.Nanosecond, got.TTL)

		value, found, deleted, err := s.getAtTime(1, []byte("key"), expiresAt-1)
		require.NoError(t, err)
		assert.True(t, found)
		assert.False(t, deleted)
		assert.Equal(t, []byte("value"), value, "kept TTL must remain encoded on disk")
	})

	t.Run("expired", func(t *testing.T) {
		s := newTestShard(t, 1<<20)
		s.putTTL(1, []byte("key"), []byte("value"), 1_000)
		require.NoError(t, s.Flush())

		var calls atomic.Int32
		cc := testCompactionConfig()
		cc.ExpireBefore = 1_000
		cc.Filter = func(CompactionFilterEntry) bool {
			calls.Add(1)
			return true
		}
		require.NoError(t, s.CompactAll(maxIKeySeq, cc))
		assert.Zero(t, calls.Load(), "expired values are reclaimed before filtering")
		assert.Empty(t, s.Tables())
	})
}

func TestCompactionFilterPanicFailsWithoutReplacingInputs(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	flushSingle(t, s, 1, "key", "value")
	original := s.Tables()

	cc := testCompactionConfig()
	cc.Filter = func(CompactionFilterEntry) bool { panic("broken filter") }
	err := s.CompactAll(maxIKeySeq, cc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "compaction filter panic: broken filter")
	assert.Equal(t, original, s.Tables(), "failed filtering must leave source tables installed")
	assert.Equal(t, []byte("value"), mustGet(t, s, 1, "key"))
}

func TestCompactionFilterHonorsCrashSafeSequence(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	s.Put(1, []byte("safe"), []byte("one"))
	s.Put(2, []byte("unsafe"), []byte("two"))
	require.NoError(t, s.Flush())

	var seen [][]byte
	cc := testCompactionConfig()
	cc.FilterThrough = 1
	cc.Filter = func(entry CompactionFilterEntry) bool {
		seen = append(seen, append([]byte(nil), entry.Key...))
		return false
	}
	require.NoError(t, s.CompactAll(maxIKeySeq, cc))
	assert.Equal(t, [][]byte{[]byte("safe")}, seen)
	_, found, deleted, err := s.get(2, []byte("safe"))
	require.NoError(t, err)
	assert.False(t, found)
	assert.False(t, deleted)
	assert.Equal(t, []byte("two"), mustGet(t, s, 2, "unsafe"))
}

func TestCompactionFilterSurvivesCrashReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.ShardCount = 1
	opts.MemtableSize = 1 << 30
	opts.CompactionFilter = func(CompactionFilterEntry) bool { return false }

	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value")}))
	require.NoError(t, db.CompactShard(0))
	_, err = db.Get([]byte("key"))
	require.ErrorIs(t, err, ErrNotFound)
	db.crash()

	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Get([]byte("key"))
	assert.ErrorIs(t, err, ErrNotFound, "WAL replay must not resurrect a filtered value")
}

// TestCheckpointCutoff guards two per-shard-WAL properties of the checkpoint's
// filterSafeSeq cutoff (which authorizes a compaction filter to physically drop
// versions at or below it):
//  1. Safety: the cutoff never exceeds the highest APPLIED sequence, so it cannot
//     name a reserved-but-unapplied seq that lives only in the live WAL (the
//     resurrection bug). The cutoff is readSeq captured after all shards' WAL
//     committers drain, then every shard is flushed, so seq <= cutoff is durable.
//  2. Liveness: an idle shard (no writes) must NOT pin the cutoff low - the whole
//     point of using readSeq rather than min(per-shard maxSeq).
func TestCheckpointCutoff(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.ShardCount = 4
	opts.Partitioner = PartitionerRange // route by key prefix so we can leave shards idle
	opts.NoSync = true
	opts.MemtableSize = 1 << 30 // nothing auto-flushes; the checkpoint does the flushing
	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()

	// Write ONLY low-byte keys so they land in the first shard; shards 1-3 stay
	// idle. This is the workload that starved the old min(maxSeq) cutoff.
	for i := 0; i < 100; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte{0x00, byte(i)}, Value: []byte("v")}))
	}

	safe, err := db.checkpointWALMode(true)
	require.NoError(t, err)

	// Safety: cutoff must not exceed the highest applied seq.
	assert.LessOrEqual(t, safe, db.readSeq.Load(),
		"filterSafeSeq must not exceed the highest applied sequence")
	assert.Equal(t, safe, db.filterSafeSeq.Load())

	// Liveness: with 300 writes applied, readSeq is well above 0; an idle shard
	// must not have pinned the cutoff to 0. It must have advanced.
	assert.Positive(t, safe, "idle shards must not pin filterSafeSeq at 0")
	assert.Equal(t, db.readSeq.Load(), safe, "cutoff should reach the applied high-water despite idle shards")

	// Monotonic: a second checkpoint with no new writes must not regress it.
	before := db.filterSafeSeq.Load()
	safe2, err := db.checkpointWALMode(true)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, safe2, before, "filterSafeSeq must be monotonic")
}

// TestCheckpointCutoffUnderConcurrentWriters guards the load-bearing direction
// the quiescent TestCheckpointCutoff cannot: that every seq <= filterSafeSeq is
// actually APPLIED, not merely <= the highest applied seq. readSeq advances by
// CAS-max across shards, so a writer that reserved a low seq but has not yet
// appended leaves a gap below readSeq; capturing the cutoff without draining
// in-flight writers would let filterSafeSeq name that un-applied (non-durable)
// seq, which a crash could resurrect after a compaction filter dropped it.
//
// The test hammers concurrent writers against repeated force checkpoints. The
// invariant: after each checkpoint every key whose seq <= the returned cutoff
// must be readable (present in a memtable/table). A missed seq would be a key
// the checkpoint claimed durable while it was still only a reserved number.
func TestCheckpointCutoffUnderConcurrentWriters(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.ShardCount = 4 // multiple shards drive the cross-shard CAS-max reorder window
	opts.NoSync = true
	opts.MemtableSize = 1 << 30 // only the checkpoint flushes
	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()

	const writers = 4
	const perWriter = 80
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := []byte(fmt.Sprintf("w%02d-k%04d", w, i))
				require.NoError(t, db.Put(PutOptions{Key: key, Value: key}))
			}
		}(w)
	}

	// Force checkpoints concurrently with the writers to straddle the reserve/apply
	// window. After each, every seq <= cutoff must be applied: assert by re-reading
	// the whole keyspace written so far is consistent (no value regresses).
	stop := make(chan struct{})
	cpDone := make(chan struct{})
	go func() {
		defer close(cpDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := db.checkpointWALMode(true); err != nil {
				require.ErrorIs(t, err, ErrClosed)
				return
			}
			time.Sleep(time.Millisecond) // pace the loop: still straddles writes, no flush storm
		}
	}()
	wg.Wait()
	close(stop)
	<-cpDone

	// Every applied write must be present, and filterSafeSeq must never exceed the
	// applied high-water. If the cutoff had named an un-applied seq, that shard's
	// double-Flush would have skipped it and the key would be missing here.
	assert.LessOrEqual(t, db.filterSafeSeq.Load(), db.readSeq.Load())
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			key := []byte(fmt.Sprintf("w%02d-k%04d", w, i))
			got, err := db.Get(key)
			require.NoErrorf(t, err, "missing key %s", key)
			assert.Equal(t, key, got)
		}
	}
}

func TestBackgroundCompactionUsesFilter(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	db := openTestDB(t, func(o *Options) {
		o.ShardCount = 1
		o.MemtableSize = 1 << 30
		o.TierRatio = 2
		o.CompactionFilter = func(CompactionFilterEntry) bool {
			calls.Add(1)
			return false
		}
	})
	require.NoError(t, db.Put(PutOptions{Key: []byte("a"), Value: []byte("one")}))
	require.NoError(t, db.shards[0].Flush())
	require.NoError(t, db.Put(PutOptions{Key: []byte("b"), Value: []byte("two")}))
	require.NoError(t, db.shards[0].Flush())

	db.flushShard(0)
	require.NoError(t, db.backgroundError())
	assert.Equal(t, int32(2), calls.Load())
	_, err := db.Get([]byte("a"))
	assert.ErrorIs(t, err, ErrNotFound)
	_, err = db.Get([]byte("b"))
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestCompactionFilterDefersForSnapshot(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	db := openTestDB(t, func(o *Options) {
		o.ShardCount = 1
		o.MemtableSize = 1 << 30
		o.CompactionFilter = func(CompactionFilterEntry) bool {
			calls.Add(1)
			return false
		}
	})
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value")}))
	require.NoError(t, db.shards[0].Flush())

	snapshot, err := db.Snapshot()
	require.NoError(t, err)
	require.NoError(t, db.CompactShard(0))
	assert.Zero(t, calls.Load())
	value, err := snapshot.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), value)
	value, err = db.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), value)

	snapshot.Release()
	require.NoError(t, db.CompactShard(0))
	assert.Equal(t, int32(1), calls.Load())
	_, err = db.Get([]byte("key"))
	assert.True(t, errors.Is(err, ErrNotFound))
}

func TestReadPinWaitsForCompactionFilter(t *testing.T) {
	t.Parallel()
	snaps := newSnapshots()
	require.True(t, snaps.beginCompactionFilter())

	started := make(chan struct{})
	done := make(chan uint64, 1)
	var current atomic.Uint64
	current.Store(7)
	go func() {
		close(started)
		done <- snaps.acquireCurrent(&current)
	}()
	<-started
	select {
	case <-done:
		t.Fatal("read pin acquired while compaction filtering was active")
	case <-time.After(20 * time.Millisecond):
	}

	snaps.endCompactionFilter()
	select {
	case seq := <-done:
		assert.Equal(t, uint64(7), seq)
		snaps.release(seq)
	case <-time.After(time.Second):
		t.Fatal("read pin did not resume after compaction filtering ended")
	}
}

func TestSnapshotCreationWaitsForFilteringCompaction(t *testing.T) {
	t.Parallel()
	filterEntered := make(chan struct{})
	allowFilter := make(chan struct{})
	var allowOnce sync.Once
	releaseFilter := func() { allowOnce.Do(func() { close(allowFilter) }) }
	defer releaseFilter()
	db := openTestDB(t, func(o *Options) {
		o.ShardCount = 1
		o.MemtableSize = 1 << 30
		o.CompactionFilter = func(CompactionFilterEntry) bool {
			close(filterEntered)
			<-allowFilter
			return false
		}
	})
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value")}))
	require.NoError(t, db.shards[0].Flush())

	compactDone := make(chan error, 1)
	go func() { compactDone <- db.CompactShard(0) }()
	select {
	case <-filterEntered:
	case <-time.After(time.Second):
		t.Fatal("compaction filter did not run")
	}

	type snapshotResult struct {
		snapshot *Snapshot
		err      error
	}
	snapshotDone := make(chan snapshotResult, 1)
	go func() {
		snapshot, err := db.Snapshot()
		snapshotDone <- snapshotResult{snapshot: snapshot, err: err}
	}()
	select {
	case <-snapshotDone:
		t.Fatal("snapshot was created before filtering compaction committed")
	case <-time.After(20 * time.Millisecond):
	}

	releaseFilter()
	require.NoError(t, <-compactDone)
	select {
	case result := <-snapshotDone:
		require.NoError(t, result.err)
		defer result.snapshot.Release()
		_, err := result.snapshot.Get([]byte("key"))
		assert.ErrorIs(t, err, ErrNotFound)
	case <-time.After(time.Second):
		t.Fatal("snapshot creation did not resume after compaction committed")
	}
}
