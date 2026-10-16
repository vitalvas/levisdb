package levisdb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlobPointerRoundTrip covers the fixed-width pointer encoding.
func TestBlobPointerRoundTrip(t *testing.T) {
	t.Parallel()
	for _, p := range []blobPointer{
		{num: 1, offset: 0, length: 0, expiresAt: 0},
		{num: 42, offset: 1 << 40, length: 1 << 20, expiresAt: 1700000000},
		{num: 0xffffffff, offset: 0xffffffffffff, length: 0xffffffff, expiresAt: -1},
	} {
		got, err := decodeBlobPointer(encodeBlobPointer(nil, p))
		require.NoError(t, err)
		assert.Equal(t, p, got)
	}
	_, err := decodeBlobPointer([]byte{1, 2, 3})
	assert.Error(t, err, "a wrong-length pointer is rejected")
}

// openBlobDB opens a DB with value separation enabled at a small threshold and a
// small blob roll so tests exercise rolling without large writes.
func openBlobDB(t *testing.T) *DB {
	t.Helper()
	return openTestDB(t, func(o *Options) {
		o.ValueThreshold = 64    // separate values >= 64 bytes
		o.BlobFileSize = 4 << 10 // roll every 4 KiB so several files appear
		o.MemtableSize = 1 << 30 // flush only on explicit Flush
		o.FreshCodec = CodecNone // keep blob/inline bytes verbatim for assertions
		o.BottomCodec = CodecNone
	})
}

func TestValueSeparationRoundTrip(t *testing.T) {
	t.Parallel()
	db := openBlobDB(t)

	big := bytes.Repeat([]byte("x"), 512) // >= threshold -> blob
	small := []byte("small")              // < threshold -> inline
	require.NoError(t, db.Put(PutOptions{Key: []byte("big"), Value: big}))
	require.NoError(t, db.Put(PutOptions{Key: []byte("small"), Value: small}))

	// Read from the memtable (pre-flush): values are inline there regardless.
	got, err := db.Get([]byte("big"))
	require.NoError(t, err)
	assert.Equal(t, big, got.Value)

	// Flush so the big value is separated into a blob and read back through a pointer.
	require.NoError(t, db.eng.Flush())
	blobs, err := db.store.listBlobs()
	require.NoError(t, err)
	require.NotEmpty(t, blobs, "a large value produced at least one blob file")

	got, err = db.Get([]byte("big"))
	require.NoError(t, err)
	assert.Equal(t, big, got.Value, "large value resolved from its blob")
	got, err = db.Get([]byte("small"))
	require.NoError(t, err)
	assert.Equal(t, small, got.Value, "small value stayed inline")
}

func TestValueSeparationTTLPreserved(t *testing.T) {
	t.Parallel()
	db := openBlobDB(t)

	const future = 1 << 40 // far future, never expires during the test
	big := bytes.Repeat([]byte("v"), 256)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: big, ExpiresAt: future}))
	require.NoError(t, db.eng.Flush())

	got, err := db.Get([]byte("k"))
	require.NoError(t, err)
	assert.Equal(t, big, got.Value, "TTL value's bytes resolved from blob")
	assert.Equal(t, int64(future), got.ExpiresAt, "exact deadline carried in the pointer")
}

func TestValueSeparationIterator(t *testing.T) {
	t.Parallel()
	db := openBlobDB(t)

	want := map[string][]byte{}
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("k%03d", i)
		// Alternate large (blob) and small (inline) so the scan resolves a mix.
		var v []byte
		if i%2 == 0 {
			v = bytes.Repeat([]byte{byte('a' + i)}, 300)
		} else {
			v = []byte(fmt.Sprintf("v%d", i))
		}
		require.NoError(t, db.Put(PutOptions{Key: []byte(k), Value: v}))
		want[k] = v
	}
	require.NoError(t, db.eng.Flush())

	it, err := db.NewIterator()
	require.NoError(t, err)
	t.Cleanup(func() { it.Close() })
	seen := map[string][]byte{}
	for it.Next() {
		seen[string(it.Key())] = append([]byte(nil), it.Value()...)
	}
	require.NoError(t, it.Error())
	assert.Equal(t, want, seen, "iterator resolves blob and inline values alike")
}

func TestValueSeparationSurvivesReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	open := func() *DB {
		o := DefaultOptions(dir)
		o.ValueThreshold = 64
		o.BlobFileSize = 4 << 10
		o.FreshCodec = CodecNone
		o.BottomCodec = CodecNone
		db, err := Open(o)
		require.NoError(t, err)
		return db
	}

	db := open()
	big := bytes.Repeat([]byte("z"), 500)
	require.NoError(t, db.Put(PutOptions{Key: []byte("big"), Value: big}))
	require.NoError(t, db.eng.Flush())
	require.NoError(t, db.Close())

	db = open()
	t.Cleanup(func() { db.Close() })
	got, err := db.Get([]byte("big"))
	require.NoError(t, err)
	assert.Equal(t, big, got.Value, "separated value survives close and reopen")

	// The blob file the manifest references must survive cleanup on reopen.
	blobs, err := db.store.listBlobs()
	require.NoError(t, err)
	assert.NotEmpty(t, blobs, "referenced blob file not reclaimed as an orphan")
}

func TestValueSeparationSurvivesCompaction(t *testing.T) {
	t.Parallel()
	db := openBlobDB(t)

	// Two flushes of large values at overlapping keys, then compact: the merge must
	// carry the blob pointers through to the output tables and the values still
	// resolve. keys_dropped for the superseded version does not touch the kept blob.
	v1 := bytes.Repeat([]byte("1"), 300)
	v2 := bytes.Repeat([]byte("2"), 300)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: v1}))
	require.NoError(t, db.Put(PutOptions{Key: []byte("filler"), Value: bytes.Repeat([]byte("f"), 300)}))
	require.NoError(t, db.eng.Flush())
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: v2}))
	require.NoError(t, db.Put(PutOptions{Key: []byte("other"), Value: bytes.Repeat([]byte("o"), 300)}))
	require.NoError(t, db.eng.Flush())

	require.NoError(t, db.CompactRange(nil, nil))

	got, err := db.Get([]byte("k"))
	require.NoError(t, err)
	assert.Equal(t, v2, got.Value, "newest separated value survives compaction")
	got, err = db.Get([]byte("filler"))
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte("f"), 300), got.Value)
}

func TestValueSeparationOffWritesNoBlobs(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 }) // feature off (default)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: bytes.Repeat([]byte("x"), 4096)}))
	require.NoError(t, db.eng.Flush())
	blobs, err := db.store.listBlobs()
	require.NoError(t, err)
	assert.Empty(t, blobs, "value separation off: no blob files are ever written")
}
