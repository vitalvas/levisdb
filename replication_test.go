package levisdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// drainUpdates collects every entry GetUpdatesSince yields from since, in order.
func drainUpdates(t *testing.T, db *DB, since uint64) []WALEntry {
	t.Helper()
	u, err := db.GetUpdatesSince(since)
	require.NoError(t, err)
	defer u.Close()
	var out []WALEntry
	for u.Next() {
		out = append(out, u.Batch()...)
	}
	require.NoError(t, u.Error())
	return out
}

func TestLatestSeqTracksWrites(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	assert.Equal(t, uint64(0), db.LatestSeq())
	require.NoError(t, db.Put(PutOptions{Key: []byte("a"), Value: []byte("1")}))
	assert.Equal(t, uint64(1), db.LatestSeq())
	var b Batch
	b.Put(PutOptions{Key: []byte("b"), Value: []byte("2")})
	b.Put(PutOptions{Key: []byte("c"), Value: []byte("3")})
	require.NoError(t, db.Write(&b))
	assert.Equal(t, uint64(3), db.LatestSeq())
}

func TestGetUpdatesSinceLiveSegment(t *testing.T) {
	t.Parallel()
	// Large memtable so nothing flushes: all writes stay in the live WAL segment.
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })
	for i := 0; i < 10; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%02d", i)), Value: []byte("v")}))
	}
	require.NoError(t, db.Delete([]byte("k00")))

	all := drainUpdates(t, db, 0)
	require.Len(t, all, 11) // 10 puts + 1 delete
	for i, e := range all {
		assert.Equal(t, uint64(i+1), e.Seq, "entries stream in ascending seq")
	}
	assert.Equal(t, EntryDelete, all[10].Kind)
	assert.Nil(t, all[10].Value)

	// Catch-up from a watermark yields only newer entries.
	tail := drainUpdates(t, db, 8)
	require.Len(t, tail, 3) // seqs 9, 10, 11
	assert.Equal(t, uint64(9), tail[0].Seq)

	// Caught up: empty stream.
	assert.Empty(t, drainUpdates(t, db, db.LatestSeq()))
}

func TestGetUpdatesSinceDeliversAbsoluteExpiry(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v"), TTL: time.Hour}))

	all := drainUpdates(t, db, 0)
	require.Len(t, all, 1)
	assert.NotZero(t, all[0].ExpiresAt, "TTL put carries an absolute deadline")
	assert.Greater(t, all[0].TTL, 50*time.Minute, "remaining TTL is derived from the deadline")
	assert.Equal(t, EntryPut, all[0].Kind)
}

func TestGetUpdatesSinceRetainsAcrossFlush(t *testing.T) {
	// Not parallel: mutates the global walSegmentBytes.
	oldSeg := walSegmentBytes
	walSegmentBytes = 4 << 10
	t.Cleanup(func() { walSegmentBytes = oldSeg })

	db := openTestDB(t, func(o *Options) {
		o.MemtableSize = 4 << 10
		o.WALRetention = time.Hour    // wide time horizon: nothing reaped during the test
		o.WALRetentionBytes = 1 << 30 // wide byte horizon
	})
	const n = 500
	for i := 0; i < n; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%05d", i)), Value: []byte("value-payload")}))
	}
	db.sched.drain()

	// Every committed mutation is still replayable from seq 0 despite flushes,
	// because retention kept the flushed segments.
	all := drainUpdates(t, db, 0)
	require.Len(t, all, n)
	for i, e := range all {
		assert.Equal(t, uint64(i+1), e.Seq)
		assert.Equal(t, []byte(fmt.Sprintf("k%05d", i)), e.Key)
	}
}

func TestGetUpdatesSinceRetentionExpired(t *testing.T) {
	// Not parallel: mutates the global walSegmentBytes.
	oldSeg := walSegmentBytes
	walSegmentBytes = 4 << 10
	t.Cleanup(func() { walSegmentBytes = oldSeg })

	db := openTestDB(t, func(o *Options) { o.MemtableSize = 4 << 10 })
	for i := 0; i < 500; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%05d", i)), Value: []byte("value-payload")}))
	}
	db.sched.drain()

	// filterSafeSeq has advanced past the earliest writes; asking from 0 must fail.
	_, err := db.GetUpdatesSince(0)
	require.ErrorIs(t, err, ErrRetentionExpired)

	// Asking from the current watermark still works (nothing to send).
	assert.Empty(t, drainUpdates(t, db, db.LatestSeq()))
}

func TestGetUpdatesSinceReaperExpiresOldSegments(t *testing.T) {
	// Not parallel: mutates the global walSegmentBytes.
	oldSeg := walSegmentBytes
	walSegmentBytes = 4 << 10
	t.Cleanup(func() { walSegmentBytes = oldSeg })

	// Retention on by BYTES only, with a tiny byte horizon so the reaper deletes
	// all but the most recent retained segment. WALRetention=0 means the time
	// horizon never protects a segment, so bytes alone decide.
	db := openTestDB(t, func(o *Options) {
		o.MemtableSize = 4 << 10
		o.WALRetentionBytes = 8 << 10 // keep only a few KB of flushed WAL
	})
	for i := 0; i < 500; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%05d", i)), Value: []byte("value-payload")}))
	}
	db.sched.drain()
	// The reaper runs inside a WAL checkpoint on a background goroutine; force a
	// synchronous checkpoint so the reap has definitely happened before asserting.
	_, err := db.checkpointWALMode(true)
	require.NoError(t, err)

	// The reaper deleted the oldest segments, so seq 0 is below the horizon.
	_, err = db.GetUpdatesSince(0)
	require.ErrorIs(t, err, ErrRetentionExpired)

	// The tail is still replayable: catching up from near the end returns the most
	// recent mutations in order.
	last := db.LatestSeq()
	tail := drainUpdates(t, db, last-5)
	require.Len(t, tail, 5)
	assert.Equal(t, last, tail[len(tail)-1].Seq)
}

// TestGetUpdatesSinceNoSilentGapUnderReaping stresses the reaper-vs-reader race:
// while writes drive checkpoints (and the byte-bounded reaper deletes flushed
// segments), a reader repeatedly catches up from seq 0. Each returned stream must
// be either gap-free from seq 1 or rejected with ErrRetentionExpired - never a
// partial stream with a hole presented as complete.
func TestGetUpdatesSinceNoSilentGapUnderReaping(t *testing.T) {
	oldSeg := walSegmentBytes
	walSegmentBytes = 4 << 10
	t.Cleanup(func() { walSegmentBytes = oldSeg })

	db := openTestDB(t, func(o *Options) {
		o.MemtableSize = 4 << 10
		o.WALRetentionBytes = 8 << 10 // small horizon: the reaper actively deletes
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			_ = db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%06d", i)), Value: []byte("payload")})
		}
	}()

	for i := 0; i < 200; i++ {
		u, err := db.GetUpdatesSince(0)
		if err != nil {
			require.ErrorIs(t, err, ErrRetentionExpired, "the only allowed error is retention-expired")
			continue
		}
		// If it returned a stream from seq 0, it must be contiguous with no hole.
		want := uint64(1)
		for u.Next() {
			for _, e := range u.Batch() {
				require.Equalf(t, want, e.Seq, "silent gap: expected seq %d, got %d", want, e.Seq)
				want++
			}
		}
		require.NoError(t, u.Error())
		require.NoError(t, u.Close())
	}
	<-done
}

func TestGetUpdatesSinceClosedDB(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	require.NoError(t, db.Close())
	_, err := db.GetUpdatesSince(0)
	assert.ErrorIs(t, err, ErrClosed)
}
