package levisdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStats(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 256 })

	// Fresh DB: no tables yet.
	st, err := db.Stats()
	require.NoError(t, err)
	assert.Equal(t, 0, st.Tables)
	assert.Equal(t, 0, st.LiveSnapshots)

	for i := 0; i < 100; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%03d", i)), Value: []byte("value")}))
	}
	db.sched.drain()

	snap, err := db.Snapshot()
	require.NoError(t, err)
	defer snap.Release()

	st, err = db.Stats()
	require.NoError(t, err)
	assert.Positive(t, st.Tables, "flushed data should have produced tables")
	assert.Positive(t, st.TablesSize)
	assert.Equal(t, 1, st.LiveSnapshots)
	assert.NotEmpty(t, st.TablesPerDepth)

	// Depth histogram sums to the total table count.
	sum := 0
	for _, c := range st.TablesPerDepth {
		sum += c
	}
	assert.Equal(t, st.Tables, sum)
}

func TestGetProperty(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 256 })
	for i := 0; i < 120; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%03d", i)), Value: []byte("v")}))
	}
	db.sched.drain()

	for _, name := range []string{
		"levisdb.num-tables",
		"levisdb.tables-size",
		"levisdb.live-snapshots",
		"levisdb.cache-blocks",
		"levisdb.cache-bytes",
		"levisdb.cache-hits",
		"levisdb.cache-misses",
		"levisdb.open-files",
		"levisdb.tables-per-depth",
		"levisdb.compaction-count",
		"levisdb.compaction-bytes-read",
		"levisdb.compaction-bytes-written",
		"levisdb.flush-count",
		"levisdb.flush-bytes-written",
		"levisdb.wal-bytes-written",
		"levisdb.write-stalls",
	} {
		v, err := db.GetProperty(name)
		require.NoError(t, err, name)
		assert.NotEmpty(t, v, name)
	}

	_, err := db.GetProperty("levisdb.bogus")
	assert.Error(t, err)
}

func TestStatsCumulativeCounters(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 256 })

	const entries = 80
	for i := 0; i < entries; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%04d", i)), Value: []byte("value-payload")}))
	}
	db.sched.drain()

	st, err := db.Stats()
	require.NoError(t, err)

	// WAL and flushes recorded bytes.
	assert.Positive(t, st.WALBytesWritten, "WAL bytes counted")
	assert.Positive(t, st.FlushCount, "flushes counted")
	assert.Positive(t, st.FlushBytesWritten, "flush bytes counted")

	// Reads populate cache hit/miss counters (a warm re-read should hit).
	for i := 0; i < entries; i++ {
		_, _ = db.Get([]byte(fmt.Sprintf("k%04d", i)))
	}
	for i := 0; i < entries; i++ {
		_, _ = db.Get([]byte(fmt.Sprintf("k%04d", i)))
	}
	st, err = db.Stats()
	require.NoError(t, err)
	assert.Positive(t, st.CacheHits+st.CacheMisses, "reads recorded cache accesses")
}

func BenchmarkStats(b *testing.B) {
	o := DefaultOptions(b.TempDir())
	o.NoSync = true
	db, err := Open(o)
	require.NoError(b, err)
	b.Cleanup(func() { db.Close() })

	for i := 0; i < 500; i++ {
		require.NoError(b, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%05d", i)), Value: []byte("value")}))
	}
	db.sched.drain()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Stats(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestStatsAfterClose(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	require.NoError(t, db.Close())
	_, err := db.Stats()
	assert.ErrorIs(t, err, ErrClosed)
	_, err = db.GetProperty("levisdb.num-tables")
	assert.ErrorIs(t, err, ErrClosed)
}
