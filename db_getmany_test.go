package levisdb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetMany(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })

	const n = 500
	for i := 0; i < n; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%04d", i)), Value: []byte(fmt.Sprintf("v%d", i))}))
	}
	require.NoError(t, db.eng.Flush()) // land in a table so the sweep reads blocks

	t.Run("found keys returned, missing omitted", func(t *testing.T) {
		keys := [][]byte{
			[]byte("k0001"), []byte("k0250"), []byte("k0499"),
			[]byte("absent"), []byte("k9999"),
		}
		got, err := db.GetMany(keys)
		require.NoError(t, err)
		require.Len(t, got, 3, "only existing keys are in the map")
		assert.Equal(t, []byte("v1"), got["k0001"].Value)
		assert.Equal(t, []byte("v250"), got["k0250"].Value)
		assert.Equal(t, []byte("v499"), got["k0499"].Value)
		_, ok := got["absent"]
		assert.False(t, ok, "a missing key is omitted, not present with a zero value")
	})

	t.Run("matches Get for every key", func(t *testing.T) {
		keys := make([][]byte, n)
		for i := range keys {
			keys[i] = []byte(fmt.Sprintf("k%04d", i))
		}
		got, err := db.GetMany(keys)
		require.NoError(t, err)
		require.Len(t, got, n)
		for i := 0; i < n; i++ {
			k := fmt.Sprintf("k%04d", i)
			single, err := db.Get([]byte(k))
			require.NoError(t, err)
			assert.Equal(t, single.Value, got[k].Value, "GetMany agrees with Get for %s", k)
		}
	})

	t.Run("dedup and empty keys", func(t *testing.T) {
		keys := [][]byte{[]byte("k0001"), []byte("k0001"), {}, nil, []byte("k0002")}
		got, err := db.GetMany(keys)
		require.NoError(t, err)
		assert.Len(t, got, 2, "duplicate collapses, empty keys skipped")
		assert.Equal(t, []byte("v1"), got["k0001"].Value)
		assert.Equal(t, []byte("v2"), got["k0002"].Value)
	})

	t.Run("nil input returns empty map", func(t *testing.T) {
		got, err := db.GetMany(nil)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

func TestGetManyReflectsDeletesAndTTL(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })

	require.NoError(t, db.Put(PutOptions{Key: []byte("live"), Value: []byte("v")}))
	require.NoError(t, db.Put(PutOptions{Key: []byte("gone"), Value: []byte("v")}))
	require.NoError(t, db.Delete([]byte("gone")))
	const future = 1 << 40
	require.NoError(t, db.Put(PutOptions{Key: []byte("ttl"), Value: []byte("v"), ExpiresAt: future}))

	got, err := db.GetMany([][]byte{[]byte("live"), []byte("gone"), []byte("ttl")})
	require.NoError(t, err)
	assert.Len(t, got, 2, "deleted key omitted")
	assert.Equal(t, []byte("v"), got["live"].Value)
	_, ok := got["gone"]
	assert.False(t, ok)
	assert.Equal(t, int64(future), got["ttl"].ExpiresAt, "stored expiry surfaced")
}

func TestGetManySnapshotConsistent(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })
	require.NoError(t, db.Put(PutOptions{Key: []byte("a"), Value: []byte("1")}))

	snap, err := db.Snapshot()
	require.NoError(t, err)
	defer snap.Release()

	// Write after the snapshot: a new key and an overwrite. The snapshot batch read
	// must not see either.
	require.NoError(t, db.Put(PutOptions{Key: []byte("a"), Value: []byte("2")}))
	require.NoError(t, db.Put(PutOptions{Key: []byte("b"), Value: []byte("new")}))

	got, err := snap.GetMany([][]byte{[]byte("a"), []byte("b")})
	require.NoError(t, err)
	assert.Equal(t, []byte("1"), got["a"].Value, "snapshot sees the pre-snapshot value")
	_, ok := got["b"]
	assert.False(t, ok, "snapshot does not see a key written after it")

	// The live DB does see both.
	live, err := db.GetMany([][]byte{[]byte("a"), []byte("b")})
	require.NoError(t, err)
	assert.Equal(t, []byte("2"), live["a"].Value)
	assert.Equal(t, []byte("new"), live["b"].Value)
}

func TestGetManyLargeValues(t *testing.T) {
	t.Parallel()
	// Value separation on: GetMany must resolve blob-backed values across the batch.
	db := openTestDB(t, func(o *Options) {
		o.ValueThreshold = 64
		o.MemtableSize = 1 << 30
		o.FreshCodec = CodecNone
		o.BottomCodec = CodecNone
	})
	want := map[string][]byte{}
	var keys [][]byte
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("big%03d", i)
		v := bytes.Repeat([]byte{byte('a' + i%26)}, 200) // >= threshold -> blob
		require.NoError(t, db.Put(PutOptions{Key: []byte(k), Value: v}))
		want[k] = v
		keys = append(keys, []byte(k))
	}
	require.NoError(t, db.eng.Flush())

	got, err := db.GetMany(keys)
	require.NoError(t, err)
	require.Len(t, got, len(want))
	for k, v := range want {
		assert.Equal(t, v, got[k].Value, "blob value resolved for %s", k)
	}
}

func TestGetManyClosedDB(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	require.NoError(t, db.Close())
	_, err := db.GetMany([][]byte{[]byte("k")})
	assert.ErrorIs(t, err, ErrClosed)
}
