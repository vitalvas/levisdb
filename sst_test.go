package levisdb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildSST writes an SST at dir/name.sst with the given put entries (ascending)
// and returns its path.
func buildSST(t *testing.T, dir, name string, puts [][2]string) string {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("%s.sst", name))
	w, err := NewSstFileWriter(path, SstWriterOptions{})
	require.NoError(t, err)
	for _, kv := range puts {
		require.NoError(t, w.Put([]byte(kv[0]), []byte(kv[1])))
	}
	require.NoError(t, w.Finish())
	return path
}

func TestSstIngestRoundTrip(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	path := buildSST(t, t.TempDir(), "bulk", [][2]string{
		{"a", "1"}, {"b", "2"}, {"c", "3"},
	})
	require.NoError(t, db.IngestExternalFile(path))

	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		got, err := db.Get([]byte(kv[0]))
		require.NoError(t, err, kv[0])
		assert.Equal(t, []byte(kv[1]), got)
	}
	// The ingest advanced the sequence past the file's assigned one.
	assert.NotZero(t, db.LatestSeq())
}

func TestSstIngestNewestWins(t *testing.T) {
	t.Parallel()
	// A pre-existing value for "k" must be shadowed by the ingested one: ingest
	// assigns a fresh sequence above every committed write and lands at the bottom
	// tier, so its version is newest. "k" is flushed out of the memtable first
	// (memtable overlap is rejected); overlapping an on-disk table is allowed.
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("old")}))
	require.NoError(t, db.eng.Flush()) // move "k" out of the memtable into a table

	path := buildSST(t, t.TempDir(), "over", [][2]string{{"k", "new"}})
	require.NoError(t, db.IngestExternalFile(path))

	got, err := db.Get([]byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), got, "ingested value is the newest version")
}

func TestSstIngestTTLAndDelete(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	path := filepath.Join(t.TempDir(), "mixed.sst")
	w, err := NewSstFileWriter(path, SstWriterOptions{})
	require.NoError(t, err)
	require.NoError(t, w.Put([]byte("keep"), []byte("v")))
	require.NoError(t, w.PutTTL([]byte("live"), []byte("v"), time.Hour))
	require.NoError(t, w.Delete([]byte("zzz")))
	require.NoError(t, w.Finish())

	require.NoError(t, db.IngestExternalFile(path))

	v, err := db.Get([]byte("keep"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), v)

	v, err = db.Get([]byte("live"))
	require.NoError(t, err, "unexpired TTL value is readable")
	assert.Equal(t, []byte("v"), v)

	_, err = db.Get([]byte("zzz"))
	assert.ErrorIs(t, err, ErrNotFound, "ingested tombstone hides the key")
}

func TestSstIngestRejectsMemtableOverlap(t *testing.T) {
	t.Parallel()
	// A live memtable entry for "b" must make an ingest covering "b" fail, so the
	// single ingest sequence cannot shadow a key the memtable may still update.
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })
	require.NoError(t, db.Put(PutOptions{Key: []byte("b"), Value: []byte("mem")}))

	path := buildSST(t, t.TempDir(), "ov", [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}})
	err := db.IngestExternalFile(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overlap")

	// The rejected ingest changed nothing: "b" still reads its memtable value.
	got, err := db.Get([]byte("b"))
	require.NoError(t, err)
	assert.Equal(t, []byte("mem"), got)
}

func TestSstWriterRejectsUnorderedKeys(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "bad.sst")
	w, err := NewSstFileWriter(path, SstWriterOptions{})
	require.NoError(t, err)
	require.NoError(t, w.Put([]byte("b"), []byte("1")))
	err = w.Put([]byte("a"), []byte("2"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ascending")
}

func TestSstWriterEmptyFinishErrors(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "empty.sst")
	w, err := NewSstFileWriter(path, SstWriterOptions{})
	require.NoError(t, err)
	assert.Error(t, w.Finish())
}

func TestSstIngestReopenPersists(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	open := func() *DB {
		o := DefaultOptions(dir)
		o.NoSync = true
		db, err := Open(o)
		require.NoError(t, err)
		return db
	}
	db := open()
	path := buildSST(t, t.TempDir(), "p", [][2]string{{"x", "1"}, {"y", "2"}})
	require.NoError(t, db.IngestExternalFile(path))
	require.NoError(t, db.Close())

	db2 := open()
	defer db2.Close()
	for _, kv := range [][2]string{{"x", "1"}, {"y", "2"}} {
		got, err := db2.Get([]byte(kv[0]))
		require.NoError(t, err, kv[0])
		assert.Equal(t, []byte(kv[1]), got)
	}
}

func TestSstIngestClosedDB(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	path := buildSST(t, t.TempDir(), "c", [][2]string{{"a", "1"}})
	require.NoError(t, db.Close())
	assert.ErrorIs(t, db.IngestExternalFile(path), ErrClosed)
}

// TestIngestRejectsRangeTombstones guards that IngestExternalFile refuses a raw
// table that carries range tombstones (which ingest cannot reproduce), rather
// than silently dropping them and resurrecting covered keys. SstFileWriter cannot
// produce range tombstones, so the source is built with the low-level tableWriter.
func TestIngestRejectsRangeTombstones(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "withrange.sst")
	f, err := os.Create(path)
	require.NoError(t, err)
	none, _ := codecFromID(codecNone)
	w := newTableWriter(f, tableWriterConfig{codec: none, bloomBits: 10, blockSize: 4096})
	w.setRangeTombstones([]rangeTombstone{{start: []byte("k03"), end: []byte("k07"), seq: 100}})
	for i := 0; i < 10; i++ {
		ik := ikeyEncode(nil, []byte(fmt.Sprintf("k%02d", i)), uint64(i+1), ikeyKindSet)
		require.NoError(t, w.Add(ik, []byte("v")))
	}
	_, err = w.finish()
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	db := openTestDB(t, nil)
	err = db.IngestExternalFile(path)
	require.ErrorIs(t, err, ErrIngestRangeDeletes, "ingest must refuse a file with range tombstones, not drop them")

	// The rejected ingest changed nothing: no key from the file is present.
	_, err = db.Get([]byte("k00"))
	assert.ErrorIs(t, err, ErrNotFound)
}
