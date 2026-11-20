package levisdb

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openIterDB(t *testing.T) *DB {
	t.Helper()
	o := DefaultOptions(t.TempDir())
	o.MemtableSize = 512 // small, so some data still flushes to several tables
	// High tier ratio so those tables are not compacted away mid-test: iterators
	// then genuinely merge across several on-disk tables, and the test avoids the
	// compaction churn that dominated its runtime.
	o.TierRatio = 1000
	o.L0StopTables = -1 // keep read fixtures without backpressure
	o.FreshCodec = CodecNone
	o.BottomCodec = CodecNone
	o.NoSync = true // iterator tests exercise reads, not durability fsyncs
	db, err := Open(o)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func drainDBIter(t *testing.T, it Iterator) (keys, vals []string) {
	t.Helper()
	for it.Next() {
		keys = append(keys, string(it.Key()))
		vals = append(vals, string(it.Value()))
	}
	require.NoError(t, it.Error())
	require.NoError(t, it.Close())
	return keys, vals
}

func TestDBIteratorSorted(t *testing.T) {
	t.Parallel()
	db := openIterDB(t)

	const n = 200
	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%03d", i)
		require.NoError(t, db.Put(PutOptions{Key: []byte(k), Value: []byte(fmt.Sprintf("v%d", i))}))
		want = append(want, k)
	}
	sort.Strings(want)

	it, err := db.NewIterator()
	require.NoError(t, err)
	keys, vals := drainDBIter(t, it)

	require.Equal(t, want, keys, "merged stream is globally ascending")
	for i, k := range keys {
		var idx int
		fmt.Sscanf(k, "key%03d", &idx)
		assert.Equal(t, fmt.Sprintf("v%d", idx), vals[i])
	}
}

func TestDBIteratorEmpty(t *testing.T) {
	t.Parallel()
	db := openIterDB(t)
	it, err := db.NewIterator()
	require.NoError(t, err)
	keys, _ := drainDBIter(t, it)
	assert.Empty(t, keys)
}

func TestDBIteratorReflectsDeletes(t *testing.T) {
	t.Parallel()
	db := openIterDB(t)
	for _, k := range []string{"a", "b", "c", "d"} {
		require.NoError(t, db.Put(PutOptions{Key: []byte(k), Value: []byte(k)}))
	}
	require.NoError(t, db.Delete([]byte("b")))

	it, err := db.NewIterator()
	require.NoError(t, err)
	keys, _ := drainDBIter(t, it)
	assert.Equal(t, []string{"a", "c", "d"}, keys, "deleted key absent from scan")
}

func TestDBIteratorExposesExpiresAt(t *testing.T) {
	t.Parallel()
	db := openIterDB(t)

	const future = 1 << 40 // far enough ahead to never expire during the test
	require.NoError(t, db.Put(PutOptions{Key: []byte("plain"), Value: []byte("v")}))
	require.NoError(t, db.Put(PutOptions{Key: []byte("ttl"), Value: []byte("v"), ExpiresAt: future}))

	it, err := db.NewIterator()
	require.NoError(t, err)
	t.Cleanup(func() { it.Close() })

	got := map[string]int64{}
	for it.Next() {
		got[string(it.Key())] = it.ExpiresAt()
	}
	require.NoError(t, it.Error())

	assert.Equal(t, int64(0), got["plain"], "a non-TTL entry reports 0")
	assert.Equal(t, int64(future), got["ttl"], "a TTL entry reports its exact stored deadline")
}

func TestDBRangeIteratorBoundsDistinguishNilAndEmpty(t *testing.T) {
	t.Parallel()
	db := openIterDB(t)
	for _, k := range []string{"a", "b", "c"} {
		require.NoError(t, db.Put(PutOptions{Key: []byte(k), Value: []byte(k)}))
	}

	// A non-nil empty end bound is an exclusive upper bound of "" and matches
	// nothing (no key sorts before the empty string). It must not be treated as
	// an unbounded (nil) end.
	it, err := db.NewRangeIterator(nil, []byte{})
	require.NoError(t, err)
	keys, _ := drainDBIter(t, it)
	assert.Empty(t, keys, "empty end bound yields an empty range, not the whole keyspace")

	// A nil end bound remains unbounded.
	it, err = db.NewRangeIterator(nil, nil)
	require.NoError(t, err)
	keys, _ = drainDBIter(t, it)
	assert.Equal(t, []string{"a", "b", "c"}, keys, "nil end bound is unbounded")
}

// openReverseIterDB opens a DB whose data stays in the memtable (large memtable,
// no flush), so the public reverse plumbing is tested without depending on the
// descending table source wired in a later step.
func openReverseIterDB(t *testing.T) *DB {
	t.Helper()
	o := DefaultOptions(t.TempDir())
	o.MemtableSize = 1 << 30
	o.NoSync = true
	db, err := Open(o)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestDBReverseIterator(t *testing.T) {
	t.Parallel()
	db := openReverseIterDB(t)
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("key%03d", i)
		require.NoError(t, db.Put(PutOptions{Key: []byte(k), Value: []byte(fmt.Sprintf("v%d", i))}))
	}

	t.Run("full descending scan", func(t *testing.T) {
		it, err := db.NewReverseIterator()
		require.NoError(t, err)
		keys, _ := drainDBIter(t, it)
		require.Len(t, keys, 50)
		assert.Equal(t, "key049", keys[0])
		assert.Equal(t, "key000", keys[49])
		for i := 1; i < len(keys); i++ {
			assert.Greater(t, keys[i-1], keys[i], "strictly descending")
		}
	})

	t.Run("latest N via reverse range", func(t *testing.T) {
		it, err := db.NewReverseRangeIterator(nil, nil)
		require.NoError(t, err)
		defer it.Close()
		var latest []string
		for it.Next() && len(latest) < 5 {
			latest = append(latest, string(it.Key()))
		}
		require.NoError(t, it.Error())
		assert.Equal(t, []string{"key049", "key048", "key047", "key046", "key045"}, latest)
	})

	t.Run("reverse range is half-open descending", func(t *testing.T) {
		it, err := db.NewReverseRangeIterator([]byte("key010"), []byte("key013"))
		require.NoError(t, err)
		keys, _ := drainDBIter(t, it)
		assert.Equal(t, []string{"key012", "key011", "key010"}, keys)
	})
}

func TestDBReverseIteratorLatestOfPrefix(t *testing.T) {
	t.Parallel()
	// The run/<zero-padded uint32> use case: the newest job has the largest id and
	// sorts last, so a reverse range over the prefix reads the latest jobs first.
	// openIterDB has a tiny memtable, so these 1000 rows span several on-disk tables.
	db := openIterDB(t)
	for i := 0; i < 1000; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("run/%010d", i)), Value: []byte(fmt.Sprintf("job%d", i))}))
	}

	it, err := db.NewReverseRangeIterator([]byte("run/"), []byte("run/:"))
	require.NoError(t, err)
	defer it.Close()
	var keys, vals []string
	for it.Next() && len(keys) < 10 {
		keys = append(keys, string(it.Key()))
		vals = append(vals, string(it.Value()))
	}
	require.NoError(t, it.Error())

	want := make([]string, 10)
	wantVals := make([]string, 10)
	for n := 0; n < 10; n++ {
		id := 999 - n
		want[n] = fmt.Sprintf("run/%010d", id)
		wantVals[n] = fmt.Sprintf("job%d", id)
	}
	assert.Equal(t, want, keys, "the 10 newest jobs, highest id first")
	assert.Equal(t, wantVals, vals, "values resolved for the latest jobs")
}

func TestDBReverseIteratorClosedDB(t *testing.T) {
	t.Parallel()
	db := openReverseIterDB(t)
	require.NoError(t, db.Close())
	_, err := db.NewReverseIterator()
	assert.ErrorIs(t, err, ErrClosed)
}

func TestReverseIteratorOverBlobs(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) {
		o.ValueThreshold = 64
		o.BlobFileSize = 4 << 10
		o.MemtableSize = 1 << 30
		o.FreshCodec = CodecNone
		o.BottomCodec = CodecNone
	})

	big := func(k string) []byte { return []byte(fmt.Sprintf("%s-%s", k, string(bytes.Repeat([]byte("x"), 128)))) }
	keys := []string{"k1", "k2", "k3"}
	for _, k := range keys {
		require.NoError(t, db.Put(PutOptions{Key: []byte(k), Value: big(k)}))
	}
	// Flush so large values separate into blobs; the reverse iterator must resolve
	// each pointer through the blob file.
	require.NoError(t, db.eng.Flush())

	it, err := db.NewReverseIterator()
	require.NoError(t, err)
	var gotKeys []string
	var gotVals [][]byte
	for it.Next() {
		gotKeys = append(gotKeys, string(it.Key()))
		gotVals = append(gotVals, append([]byte(nil), it.Value()...))
	}
	require.NoError(t, it.Error())
	require.NoError(t, it.Close())
	assert.Equal(t, []string{"k3", "k2", "k1"}, gotKeys)
	assert.Equal(t, big("k3"), gotVals[0], "blob-backed value resolved in reverse scan")
	assert.Equal(t, big("k1"), gotVals[2])
}

func TestSnapshotReverseIterator(t *testing.T) {
	t.Parallel()
	db := openReverseIterDB(t)
	for i := 0; i < 10; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")}))
	}
	snap, err := db.Snapshot()
	require.NoError(t, err)
	defer snap.Release()

	require.NoError(t, db.Put(PutOptions{Key: []byte("k9zzz"), Value: []byte("late")}))

	it, err := snap.NewReverseIterator()
	require.NoError(t, err)
	keys, _ := drainDBIter(t, it)
	require.Len(t, keys, 10)
	assert.Equal(t, "k9", keys[0], "post-snapshot write not visible")
	assert.Equal(t, "k0", keys[9])
}
