package levisdb

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingObserver struct {
	mu      sync.Mutex
	batches [][]walEntry
}

func (o *recordingObserver) observe(batch []walEntry) {
	o.mu.Lock()
	defer o.mu.Unlock()
	cp := make([]walEntry, len(batch))
	copy(cp, batch)
	o.batches = append(o.batches, cp)
}

func (o *recordingObserver) all() []walEntry {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []walEntry
	for _, b := range o.batches {
		out = append(out, b...)
	}
	return out
}

func openWALFile(t *testing.T) (*os.File, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wal.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	require.NoError(t, err)
	return f, path
}

func TestWALPersistsCallerSeqAndObserves(t *testing.T) {
	t.Parallel()
	f, _ := openWALFile(t)
	obs := &recordingObserver{}
	w := newWAL(f, obs, walConfig{sync: true})

	// The caller (the DB's global sequence) assigns Seq; the WAL persists and
	// observes the entries with those sequences unchanged.
	require.NoError(t, w.append([]walEntry{
		{Kind: walKindPut, Key: []byte("a"), Value: []byte("1"), Seq: 1},
		{Kind: walKindPut, Key: []byte("b"), Value: []byte("2"), Seq: 2},
	}))
	require.NoError(t, w.append([]walEntry{
		{Kind: walKindDelete, Key: []byte("c"), Seq: 3},
	}))
	require.NoError(t, w.Close())

	got := obs.all()
	require.Len(t, got, 3)
	assert.Equal(t, uint64(1), got[0].Seq)
	assert.Equal(t, uint64(2), got[1].Seq)
	assert.Equal(t, uint64(3), got[2].Seq)
}

// TestWALApplyPanicDoesNotWedge guards that a panic inside the apply hook (the
// memtable install) is recovered into an error rather than unwinding the committer
// goroutine. An unrecovered panic would leave w.writing set, so Close (which waits
// for w.writing==false) would deadlock forever. The append must return the error,
// and Close must return promptly.
func TestWALApplyPanicDoesNotWedge(t *testing.T) {
	t.Parallel()
	f, _ := openWALFile(t)
	w := newWAL(f, nil, walConfig{sync: true})
	w.apply = func([]walEntry) { panic("boom") }

	appendErr := make(chan error, 1)
	go func() {
		appendErr <- w.append([]walEntry{{Kind: walKindPut, Key: []byte("a"), Value: []byte("1"), Seq: 1}})
	}()
	select {
	case err := <-appendErr:
		require.Error(t, err, "an apply panic must surface as an error to the caller")
		assert.Contains(t, err.Error(), "apply panic")
	case <-time.After(5 * time.Second):
		t.Fatal("append wedged after an apply panic (w.writing left set)")
	}

	closed := make(chan error, 1)
	go func() { closed <- w.Close() }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close deadlocked after an apply panic")
	}
}

func TestWALConcurrentAppend(t *testing.T) {
	t.Parallel()
	f, _ := openWALFile(t)
	obs := &recordingObserver{}
	w := newWAL(f, nil, walConfig{sync: true})
	w.obs = obs

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			// Each goroutine supplies a unique caller-assigned seq (1..n).
			assert.NoError(t, w.append([]walEntry{
				{Kind: walKindPut, Key: []byte{byte(i)}, Value: []byte("v"), Seq: uint64(i + 1)},
			}))
		}(i)
	}
	wg.Wait()
	require.NoError(t, w.Close())

	got := obs.all()
	require.Len(t, got, n)
	// Every caller-assigned sequence number is preserved, unique, and in range.
	seen := map[uint64]bool{}
	for _, e := range got {
		assert.False(t, seen[e.Seq], "duplicate seq %d", e.Seq)
		seen[e.Seq] = true
		assert.True(t, e.Seq >= 1 && e.Seq <= n)
	}
}

func TestWALAppendAfterClose(t *testing.T) {
	t.Parallel()
	f, _ := openWALFile(t)
	w := newWAL(f, nil, walConfig{sync: true})
	require.NoError(t, w.Close())
	err := w.append([]walEntry{{Kind: walKindPut, Key: []byte("k")}})
	assert.ErrorIs(t, err, os.ErrClosed)
}

func TestReplayWALFile(t *testing.T) {
	t.Parallel()
	f, path := openWALFile(t)
	w := newWAL(f, nil, walConfig{sync: true})
	require.NoError(t, w.append([]walEntry{
		{Kind: walKindPut, Key: []byte("a"), Value: []byte("1"), Seq: 1},
		{Kind: walKindDelete, Key: []byte("b"), Seq: 2},
	}))
	require.NoError(t, w.append([]walEntry{
		{Kind: walKindPut, Key: []byte("c"), Value: []byte("3"), Seq: 3},
	}))
	require.NoError(t, w.Close())

	rf, err := os.Open(path)
	require.NoError(t, err)
	defer rf.Close()

	entries, seq, err := replayWALFile(rf, 0)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	assert.Equal(t, uint64(3), seq)
	assert.Equal(t, []byte("a"), entries[0].Key)
	assert.Equal(t, walKindDelete, entries[1].Kind)
	assert.Equal(t, uint64(3), entries[2].Seq)
}

func TestReplayWALFileEmpty(t *testing.T) {
	t.Parallel()
	f, path := openWALFile(t)
	require.NoError(t, newWAL(f, nil, walConfig{sync: true}).Close())

	rf, err := os.Open(path)
	require.NoError(t, err)
	defer rf.Close()

	entries, seq, err := replayWALFile(rf, 42)
	require.NoError(t, err)
	assert.Empty(t, entries)
	assert.Equal(t, uint64(42), seq)
}

func TestWALAppendEmptyBatch(t *testing.T) {
	t.Parallel()
	f, _ := openWALFile(t)
	obs := &recordingObserver{}
	w := newWAL(f, obs, walConfig{sync: true})

	// An empty batch hits the len(pb.entries)==0 continue in writeGroup and
	// commits with no error and no observed entries.
	require.NoError(t, w.append([]walEntry{}))
	require.NoError(t, w.Close())
	assert.Empty(t, obs.all())
}

func TestWALCloseAfterFileClosed(t *testing.T) {
	t.Parallel()
	f, _ := openWALFile(t)
	w := newWAL(f, nil, walConfig{sync: true})
	require.NoError(t, w.append([]walEntry{{Kind: walKindPut, Key: []byte("k"), Value: []byte("v")}}))

	// Close the underlying file out from under the WAL so Close's Sync fails.
	require.NoError(t, f.Close())
	assert.Error(t, w.Close())
}

// keyPanicObserver panics only when it observes a batch whose first key matches
// panicKey, so a test can fail one batch in a coalesced group and check the rest.
type keyPanicObserver struct{ panicKey string }

func (o keyPanicObserver) observe(batch []walEntry) {
	if len(batch) > 0 && string(batch[0].Key) == o.panicKey {
		panic("observer failed")
	}
}

func TestWALObserverPanicDoesNotStrandDurableGroup(t *testing.T) {
	t.Parallel()
	f, _ := openWALFile(t)
	// Only the second batch's observer panics.
	w := newWAL(f, keyPanicObserver{panicKey: "b"}, walConfig{sync: false})
	var applied []uint64
	w.apply = func(entries []walEntry) {
		for _, entry := range entries {
			applied = append(applied, entry.Seq)
		}
	}
	first := &pendingBatch{entries: []walEntry{{Kind: walKindPut, Key: []byte("a"), Seq: 1}}, done: make(chan error, 1)}
	second := &pendingBatch{entries: []walEntry{{Kind: walKindPut, Key: []byte("b"), Seq: 2}}, done: make(chan error, 1)}
	w.mu.Lock()
	w.pending = []*pendingBatch{first, second}
	w.writing = true
	w.mu.Unlock()

	w.commit()
	// The first batch is durable, applied, and observed cleanly: it must report
	// success even though a sibling batch's observer panicked.
	assert.NoError(t, <-first.done, "sibling batch committed before the failing one must not be failed")
	assert.ErrorContains(t, <-second.done, "observer panic")
	assert.Equal(t, []uint64{1, 2}, applied, "all durable records must be published before callbacks run")
	// An observer panic does not corrupt the WAL, so later appends still succeed.
	assert.NoError(t, w.append([]walEntry{{Kind: walKindPut, Key: []byte("c")}}))
	assert.NoError(t, w.Close())
}

func BenchmarkWALAppend(b *testing.B) {
	path := filepath.Join(b.TempDir(), "wal.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	require.NoError(b, err)
	// sync=false so the benchmark measures encode+write throughput, not fsync.
	w := newWAL(f, nil, walConfig{sync: false})
	entry := walEntry{Kind: walKindPut, Key: []byte("key"), Value: []byte("value")}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.append([]walEntry{entry}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	require.NoError(b, w.Close())
}

func TestReplayWALFileDecodeError(t *testing.T) {
	t.Parallel()
	f, path := openWALFile(t)

	// Write a well-framed journal record whose batch payload claims more
	// entries than it contains, so the journal returns it intact but
	// decodeBatch fails inside replayWALFile.
	rec := encodeBatch(nil, 1, []walEntry{{Kind: walKindPut, Key: []byte("k"), Value: []byte("v")}})
	rec[8] = 9 // count uvarint: claim 9 entries where only 1 exists
	jw := newJournalWriter(f)
	require.NoError(t, jw.Write(rec))
	require.NoError(t, jw.Flush())
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	rf, err := os.Open(path)
	require.NoError(t, err)
	defer rf.Close()

	_, _, err = replayWALFile(rf, 0)
	assert.Error(t, err)
}

func TestReplayWALRejectsNonIncreasingSequences(t *testing.T) {
	t.Parallel()
	f, path := openWALFile(t)
	jw := newJournalWriter(f)
	entry := func(seq uint64, key string) []byte {
		return encodeBatch(nil, seq, []walEntry{{Kind: walKindPut, Key: []byte(key), Value: []byte("v")}})
	}
	require.NoError(t, jw.Write(entry(2, "newer")))
	require.NoError(t, jw.Write(entry(1, "older")))
	require.NoError(t, jw.Flush())
	require.NoError(t, f.Close())

	rf, err := os.Open(path)
	require.NoError(t, err)
	defer rf.Close()
	_, _, err = replayWALFile(rf, 0)
	assert.ErrorContains(t, err, "non-increasing sequence")
}

func TestReplayWALRejectsEmptyRecord(t *testing.T) {
	t.Parallel()
	f, path := openWALFile(t)
	jw := newJournalWriter(f)
	require.NoError(t, jw.Write(encodeBatch(nil, 1, nil)))
	require.NoError(t, jw.Flush())
	require.NoError(t, f.Close())

	rf, err := os.Open(path)
	require.NoError(t, err)
	defer rf.Close()
	_, _, err = replayWALFile(rf, 0)
	assert.ErrorContains(t, err, "empty batch")
}
