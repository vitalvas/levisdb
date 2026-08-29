package levisdb

import (
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
