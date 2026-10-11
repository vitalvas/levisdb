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
		"levisdb.point-reads",
		"levisdb.table-probes",
		"levisdb.bloom-false-positives",
		"levisdb.user-bytes-written",
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
	// User (logical) bytes written -- the write-amp denominator -- is exactly the
	// sum of key+value lengths stored.
	assert.Equal(t, int64(entries)*int64(len("k0000")+len("value-payload")), st.UserBytesWritten,
		"user bytes = sum of key+value lengths")

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
	// Read-amplification counters: every Get is a point read; probes accrue as
	// lookups consult tables the key-range gate did not skip.
	assert.GreaterOrEqual(t, st.PointReads, int64(2*entries), "point reads counted")
	assert.Positive(t, st.TableProbes, "reads probed on-disk tables")
}

// TestReadAmpReflectsTableFanout checks that TableProbes rises with the number of
// overlapping tables a lookup must consult -- the structural read fan-out that
// governs per-lookup seek cost where random reads are expensive. Keys that share a range across
// several on-disk tables are probed in more tables than a key isolated to one.
func TestReadAmpReflectsTableFanout(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 }) // no auto-flush

	// Flush three tables that all overlap on key "k" (same key, newer each time),
	// so a lookup of "k" must probe all three; "iso" lives in only the last table.
	for _, v := range []string{"v1", "v2", "v3"} {
		require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte(v)}))
		require.NoError(t, db.eng.Flush())
	}
	require.NoError(t, db.Put(PutOptions{Key: []byte("iso"), Value: []byte("x")}))
	require.NoError(t, db.eng.Flush())

	base, err := db.Stats()
	require.NoError(t, err)
	r, err := db.Get([]byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v3"), r.Value) // newest wins across the overlap
	after, err := db.Stats()
	require.NoError(t, err)

	probesForK := after.TableProbes - base.TableProbes
	assert.Equal(t, int64(1), after.PointReads-base.PointReads)
	// "k" exists in the three overlapping tables; the key-range gate lets the lookup
	// probe exactly those (the "iso"-only table is skipped by its bounds).
	assert.Equal(t, int64(3), probesForK, "overlapping key probes every table that may hold it")

	// An isolated key probes only its single table: lower fan-out.
	base = after
	_, err = db.Get([]byte("iso"))
	require.NoError(t, err)
	after, err = db.Stats()
	require.NoError(t, err)
	assert.Equal(t, int64(1), after.TableProbes-base.TableProbes, "isolated key probes one table")

	// Has participates in read-amp accounting exactly like Get.
	base = after
	_, err = db.Has([]byte("k"))
	require.NoError(t, err)
	after, err = db.Stats()
	require.NoError(t, err)
	assert.Equal(t, int64(1), after.PointReads-base.PointReads, "Has counts as a point read")
	assert.Equal(t, int64(3), after.TableProbes-base.TableProbes, "Has probes the overlapping tables too")
}

// TestBloomFalsePositivesCounted checks the wasted-read-I/O signal: looking up a
// key absent from a table whose key range still covers it either misses in the
// bloom (no read) or, on a bloom false positive, reads a block that finds nothing
// and is counted. Probing a key inside the stored range maximizes the chance of a
// counted false positive; the counter must never exceed the block reads.
func TestBloomFalsePositivesCounted(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) {
		o.MemtableSize = 1 << 30
		o.BloomBits = 1 // a weak filter makes false positives likely, exercising the counter
	})
	// Store sparse keys so many absent keys fall inside the table's [min,max] range.
	const n = 150
	for i := 0; i < n; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%06d", i*2)), Value: []byte("v")}))
	}
	require.NoError(t, db.eng.Flush())

	base, err := db.Stats()
	require.NoError(t, err)
	// Look up absent odd keys, all within the stored key range.
	for i := 0; i < n; i++ {
		_, err := db.Get([]byte(fmt.Sprintf("k%06d", i*2+1)))
		require.ErrorIs(t, err, ErrNotFound)
	}
	after, err := db.Stats()
	require.NoError(t, err)

	fp := after.BloomFalsePositives - base.BloomFalsePositives
	probes := after.TableProbes - base.TableProbes
	assert.Positive(t, fp, "a weak bloom over in-range absent keys must yield false positives")
	assert.LessOrEqual(t, fp, probes, "a false positive is one probe, so FP <= probes")
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
