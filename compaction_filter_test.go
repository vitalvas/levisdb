package levisdb

import (
	"errors"
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
	require.NoError(t, db.eng.Flush())
	require.NoError(t, db.Put(PutOptions{Key: []byte("drop"), Value: []byte("new")}))
	require.NoError(t, db.Put(PutOptions{Key: []byte("keep"), Value: []byte("value")}))
	require.NoError(t, db.Put(PutOptions{Key: []byte("ttl"), Value: []byte("temporary"), TTL: time.Hour}))

	require.NoError(t, db.CompactRange(nil, nil))
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
		s := newTestEngine(t, 1<<20)
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
		s := newTestEngine(t, 1<<20)
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
	s := newTestEngine(t, 1<<20)
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
	s := newTestEngine(t, 1<<20)
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
	opts.MemtableSize = 1 << 30
	opts.CompactionFilter = func(CompactionFilterEntry) bool { return false }

	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value")}))
	require.NoError(t, db.CompactRange(nil, nil))
	_, err = db.Get([]byte("key"))
	require.ErrorIs(t, err, ErrNotFound)
	db.crash()

	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Get([]byte("key"))
	assert.ErrorIs(t, err, ErrNotFound, "WAL replay must not resurrect a filtered value")
}

// TestCheckpointCutoff guards the checkpoint's filterSafeSeq cutoff (which
// authorizes a compaction filter to physically drop versions at or below it):
// the cutoff never exceeds the highest applied sequence, so it cannot name a
// reserved-but-unapplied seq that lives only in the live WAL (the resurrection
// bug), it advances above zero once writes are applied, and it never regresses.
func TestCheckpointCutoff(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.NoSync = true
	opts.MemtableSize = 1 << 30 // nothing auto-flushes; the checkpoint does the flushing
	db, err := Open(opts)
	require.NoError(t, err)
	defer db.Close()

	for i := 0; i < 100; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte{0x00, byte(i)}, Value: []byte("v")}))
	}

	safe, err := db.checkpointWALMode(true)
	require.NoError(t, err)

	// Safety: cutoff must not exceed the highest applied seq.
	assert.LessOrEqual(t, safe, db.readSeq.Load(),
		"filterSafeSeq must not exceed the highest applied sequence")
	assert.Equal(t, safe, db.filterSafeSeq.Load())
	assert.Positive(t, safe, "applied writes must advance filterSafeSeq above 0")
	assert.Equal(t, db.readSeq.Load(), safe, "cutoff should reach the applied high-water")

	// Monotonic: a second checkpoint with no new writes must not regress it.
	before := db.filterSafeSeq.Load()
	safe2, err := db.checkpointWALMode(true)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, safe2, before, "filterSafeSeq must be monotonic")
}

func TestBackgroundCompactionUsesFilter(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	db := openTestDB(t, func(o *Options) {
		o.MemtableSize = 1 << 30
		o.TierRatio = 2
		o.CompactionFilter = func(CompactionFilterEntry) bool {
			calls.Add(1)
			return false
		}
	})
	require.NoError(t, db.Put(PutOptions{Key: []byte("a"), Value: []byte("one")}))
	require.NoError(t, db.eng.Flush())
	require.NoError(t, db.Put(PutOptions{Key: []byte("b"), Value: []byte("two")}))
	require.NoError(t, db.eng.Flush())

	require.NoError(t, db.CompactRange(nil, nil))
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
		o.MemtableSize = 1 << 30
		o.CompactionFilter = func(CompactionFilterEntry) bool {
			calls.Add(1)
			return false
		}
	})
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value")}))
	require.NoError(t, db.eng.Flush())

	snapshot, err := db.Snapshot()
	require.NoError(t, err)
	require.NoError(t, db.CompactRange(nil, nil))
	assert.Zero(t, calls.Load())
	value, err := snapshot.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), value)
	value, err = db.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), value)

	snapshot.Release()
	require.NoError(t, db.CompactRange(nil, nil))
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
		o.MemtableSize = 1 << 30
		o.CompactionFilter = func(CompactionFilterEntry) bool {
			close(filterEntered)
			<-allowFilter
			return false
		}
	})
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value")}))
	require.NoError(t, db.eng.Flush())

	compactDone := make(chan error, 1)
	go func() { compactDone <- db.CompactRange(nil, nil) }()
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

func TestRegressionRetainedWALFilterCrash(t *testing.T) {
	t.Parallel()
	opts := DefaultOptions(t.TempDir())
	opts.WALRetention = time.Hour
	opts.CompactionFilter = func(CompactionFilterEntry) bool { return false }
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
	require.NoError(t, db.CompactRange(nil, nil))
	_, err = db.Get([]byte("k"))
	require.ErrorIs(t, err, ErrNotFound)
	db.crash()
	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()
	v, err := db.Get([]byte("k"))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("filtered key resurrected after crash: value=%q err=%v", v, err)
	}
}

func TestRegressionFilterCutoffIncludesLiveWAL(t *testing.T) {
	t.Parallel()
	opts := DefaultOptions(t.TempDir())
	opts.MemtableSize = 1 << 30
	opts.CompactionFilter = func(CompactionFilterEntry) bool { return false }
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("seed"), Value: []byte("v")}))
	db.seqMu.Lock()
	written := make(chan error, 1)
	go func() { written <- db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}) }()
	time.Sleep(20 * time.Millisecond) // writer holds db.mu.RLock while waiting for seqMu
	checkpointed := make(chan error, 1)
	go func() { _, err := db.checkpointWALMode(true); checkpointed <- err }()
	deadline := time.Now().Add(time.Second)
	for db.maxWALSegmentSize() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("WAL did not rotate")
		}
		time.Sleep(time.Millisecond)
	}
	db.seqMu.Unlock() // queued write lands in the NEW WAL before cutoff is captured
	require.NoError(t, <-written)
	require.NoError(t, <-checkpointed)
	require.Equal(t, uint64(1), db.filterSafeSeq.Load(), "the write in the new WAL is not covered by the retired cutoff")
	require.NoError(t, db.CompactRange(nil, nil))
	_, err = db.Get([]byte("k"))
	require.ErrorIs(t, err, ErrNotFound)
	db.crash()
	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()
	v, err := db.Get([]byte("k"))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("filtered key replayed from live WAL with retention disabled: value=%q err=%v", v, err)
	}
}

func TestRegressionFilterReaderLockCycle(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) {
		o.MemtableSize = 1 << 30
		o.CompactionFilter = func(CompactionFilterEntry) bool { return true }
	})
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
	db.checkpointMu.Lock()
	configured := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		_, _, release, err := db.compactionRunConfig(true)
		release()
		configured <- err
	}()
	<-started
	time.Sleep(20 * time.Millisecond)
	readDone := make(chan error, 1)
	go func() { _, err := db.Get([]byte("k")); readDone <- err }()
	// A pending checkpoint must not reserve the filter interval and block reads.
	select {
	case err := <-readDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Error("reader blocked by a filter reservation while checkpoint was pending")
	}
	db.checkpointMu.Unlock()
	select {
	case err := <-configured:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("filter checkpoint did not complete")
	}
}
