package levisdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func FuzzTableReaderOpen(f *testing.F) {
	// A valid table so the corpus mutates from real bytes.
	c, _ := codecFromName("none")
	var buf bytes.Buffer
	w := newTableWriter(&buf, tableWriterConfig{codec: c, bloomBits: 10, blockSize: 128})
	_ = w.Add(ikeyEncode(nil, []byte("k"), 1, ikeyKindSet), []byte("v"))
	_, _ = w.finish()
	f.Add(buf.Bytes())
	f.Add([]byte{})
	f.Add(make([]byte, footerLen))

	f.Fuzz(func(_ *testing.T, raw []byte) {
		// Contract: opening a table from arbitrary bytes never panics; it returns
		// a reader or an error. If it opens, a lookup must also not panic.
		tr, err := newCachedTableReader(bytes.NewReader(raw), int64(len(raw)), nil, 1)
		if err != nil {
			return
		}
		_, _, _, _ = tr.get([]byte("anything"), 1<<30)
		_, _, _ = tr.has([]byte("anything"), 1<<30)
	})
}

func TestTableReaderRejectsInvalidMetadataLayout(t *testing.T) {
	t.Parallel()
	c, err := codecFromName("none")
	require.NoError(t, err)
	var buf bytes.Buffer
	w := newTableWriter(&buf, tableWriterConfig{codec: c, bloomBits: 10, blockSize: 128})
	require.NoError(t, w.Add(ikeyEncode(nil, []byte("k"), 1, ikeyKindSet), []byte("v")))
	_, err = w.finish()
	require.NoError(t, err)
	original := buf.Bytes()

	t.Run("gap between filter and index", func(t *testing.T) {
		raw := append([]byte(nil), original...)
		footer := raw[len(raw)-footerLen:]
		index, n := decodeHandle(footer)
		filter, _ := decodeHandle(footer[n:])
		filter.offset++
		clear(footer)
		encoded := index.encode(nil)
		encoded = filter.encode(encoded)
		copy(footer, encoded)
		binary.LittleEndian.PutUint64(footer[footerLen-8:], magic)
		_, err := newCachedTableReader(bytes.NewReader(raw), int64(len(raw)), nil, 1)
		assert.ErrorContains(t, err, "metadata block layout")
	})

	t.Run("non-zero footer padding", func(t *testing.T) {
		raw := append([]byte(nil), original...)
		footer := raw[len(raw)-footerLen:]
		_, n := decodeHandle(footer)
		_, n2 := decodeHandle(footer[n:])
		footer[n+n2] = 1
		_, err := newCachedTableReader(bytes.NewReader(raw), int64(len(raw)), nil, 1)
		assert.ErrorContains(t, err, "footer padding")
	})
}

func TestTableReaderRejectsInvalidBloomEncoding(t *testing.T) {
	t.Parallel()
	c, err := codecFromName("none")
	require.NoError(t, err)
	var buf bytes.Buffer
	w := newTableWriter(&buf, tableWriterConfig{codec: c, bloomBits: 10, blockSize: 128})
	require.NoError(t, w.Add(ikeyEncode(nil, []byte("k"), 1, ikeyKindSet), []byte("v")))
	_, err = w.finish()
	require.NoError(t, err)
	raw := append([]byte(nil), buf.Bytes()...)

	footer := raw[len(raw)-footerLen:]
	_, n := decodeHandle(footer)
	filter, used := decodeHandle(footer[n:])
	require.Positive(t, used)
	start := int(filter.offset)
	end := start + int(filter.length)
	body := raw[start : end-4]
	// For an uncompressed block, the byte before the codec ID is the bloom
	// probe-count trailer. Keep the block CRC valid so open reaches structural
	// bloom validation rather than rejecting an unrelated checksum failure.
	body[len(body)-2] = 31
	binary.LittleEndian.PutUint32(raw[end-4:], crc32.Checksum(body, tableCastagnoli))

	_, err = newCachedTableReader(bytes.NewReader(raw), int64(len(raw)), nil, 1)
	assert.ErrorContains(t, err, "bloom probe count")
}

func FuzzTableRoundTrip(f *testing.F) {
	f.Add([]byte("a"), []byte("b"), []byte("c"), []byte("val"))
	f.Add([]byte(""), []byte("\x00"), []byte("\xff\xff"), []byte(""))
	f.Add([]byte("dup"), []byte("dup"), []byte("z"), []byte("x"))

	f.Fuzz(func(t *testing.T, k1, k2, k3, val []byte) {
		// Deduplicate user keys and sort; the writer requires ascending order.
		set := map[string]struct{}{string(k1): {}, string(k2): {}, string(k3): {}}
		keys := make([]string, 0, len(set))
		for k := range set {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		c, _ := codecFromName("s2")
		var buf bytes.Buffer
		w := newTableWriter(&buf, tableWriterConfig{codec: c, bloomBits: 10, blockSize: 64}) // tiny blocks -> exercise multi-block
		for _, k := range keys {
			require.NoError(t, w.Add(ikeyEncode(nil, []byte(k), 1, ikeyKindSet), val))
		}
		size, err := w.finish()
		require.NoError(t, err)

		tr, err := newCachedTableReader(bytes.NewReader(buf.Bytes()), size, nil, 1)
		require.NoError(t, err)

		// Every written key must read back with its value, and get/has agree.
		for _, k := range keys {
			v, found, deleted, err := tr.get([]byte(k), 10)
			require.NoError(t, err)
			require.True(t, found, "key %q missing", k)
			require.False(t, deleted)
			require.True(t, bytes.Equal(val, v), "value mismatch for %q", k)

			hFound, hDel, err := tr.has([]byte(k), 10)
			require.NoError(t, err)
			assert.Equal(t, found, hFound)
			assert.Equal(t, deleted, hDel)
		}
	})
}

// tableEntry is an internal-key entry to write into a test table.
type tableEntry struct {
	user  string
	seq   uint64
	kind  ikeyKind
	value string
}

// buildTable writes entries (which must be in ascending internal-key order) to
// a table and returns a reader over it.
func buildTable(t *testing.T, blockSize int, cache *blockCacheT, entries []tableEntry) *tableReader {
	t.Helper()
	c, err := codecFromName("none")
	require.NoError(t, err)

	var buf bytes.Buffer
	tw := newTableWriter(&buf, tableWriterConfig{codec: c, bloomBits: 10, blockSize: blockSize})
	for _, e := range entries {
		ikey := ikeyEncode(nil, []byte(e.user), e.seq, e.kind)
		require.NoError(t, tw.Add(ikey, []byte(e.value)))
	}
	size, err := tw.finish()
	require.NoError(t, err)

	tr, err := newCachedTableReader(bytes.NewReader(buf.Bytes()), size, cache, 1)
	require.NoError(t, err)
	return tr
}

func TestTableReaderHas(t *testing.T) {
	t.Parallel()
	entries := []tableEntry{
		{"a", 3, ikeyKindSet, "a3"},
		{"b", 5, ikeyKindDelete, ""},
		{"b", 2, ikeyKindSet, "b2"},
		{"c", 4, ikeyKindSet, "c4"},
	}
	tr := buildTable(t, 128, nil, entries) // small blocks -> multi-block index

	found, deleted, err := tr.has([]byte("a"), 10)
	require.NoError(t, err)
	assert.True(t, found)
	assert.False(t, deleted)

	// Newest version of b is a tombstone.
	found, deleted, err = tr.has([]byte("b"), 10)
	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, deleted)

	// Snapshot before the delete sees b as present, not deleted.
	found, deleted, err = tr.has([]byte("b"), 3)
	require.NoError(t, err)
	assert.True(t, found)
	assert.False(t, deleted)

	// Absent key: bloom-gated, not found.
	found, _, err = tr.has([]byte("zzz"), 10)
	require.NoError(t, err)
	assert.False(t, found)

	// A snapshot below all versions of c sees nothing.
	found, _, err = tr.has([]byte("c"), 1)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestTableReaderGet(t *testing.T) {
	t.Parallel()
	// Ascending internal-key order: user key asc, seq desc within a key.
	entries := []tableEntry{
		{"a", 3, ikeyKindSet, "a3"},
		{"a", 1, ikeyKindSet, "a1"},
		{"b", 5, ikeyKindDelete, ""},
		{"b", 2, ikeyKindSet, "b2"},
		{"c", 4, ikeyKindSet, "c4"},
	}
	tr := buildTable(t, 4096, nil, entries)

	t.Run("newest_version", func(t *testing.T) {
		v, found, deleted, err := tr.get([]byte("a"), 10)
		require.NoError(t, err)
		assert.True(t, found)
		assert.False(t, deleted)
		assert.Equal(t, []byte("a3"), v)
	})

	t.Run("snapshot_seq", func(t *testing.T) {
		v, found, deleted, err := tr.get([]byte("a"), 2)
		require.NoError(t, err)
		assert.True(t, found)
		assert.False(t, deleted)
		assert.Equal(t, []byte("a1"), v)
	})

	t.Run("tombstone", func(t *testing.T) {
		v, found, deleted, err := tr.get([]byte("b"), 10)
		require.NoError(t, err)
		assert.True(t, found)
		assert.True(t, deleted)
		assert.Nil(t, v)
	})

	t.Run("older_snapshot_before_tombstone", func(t *testing.T) {
		v, found, deleted, err := tr.get([]byte("b"), 3)
		require.NoError(t, err)
		assert.True(t, found)
		assert.False(t, deleted)
		assert.Equal(t, []byte("b2"), v)
	})

	t.Run("no_version_below_seq", func(t *testing.T) {
		_, found, _, err := tr.get([]byte("c"), 1)
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("missing_key", func(t *testing.T) {
		_, found, _, err := tr.get([]byte("zzz"), 10)
		require.NoError(t, err)
		assert.False(t, found)
	})
}

func TestTableReaderMultiBlock(t *testing.T) {
	t.Parallel()
	var entries []tableEntry
	for i := 0; i < 100; i++ {
		entries = append(entries, tableEntry{
			user:  string([]byte{byte(i)}),
			seq:   1,
			kind:  ikeyKindSet,
			value: string([]byte{byte(i)}),
		})
	}
	// Small block size forces many data blocks.
	tr := buildTable(t, 64, nil, entries)

	for i := 0; i < 100; i++ {
		v, found, _, err := tr.get([]byte{byte(i)}, 10)
		require.NoError(t, err)
		require.True(t, found, "key %d", i)
		assert.Equal(t, []byte{byte(i)}, v)
	}
}

func TestTableReaderCache(t *testing.T) {
	t.Parallel()
	entries := []tableEntry{
		{"a", 1, ikeyKindSet, "va"},
		{"b", 1, ikeyKindSet, "vb"},
	}
	cache := newBlockCache(1 << 20)
	tr := buildTable(t, 64, cache, entries)

	// Two lookups: the second should hit the cache and still be correct.
	for i := 0; i < 2; i++ {
		v, found, _, err := tr.get([]byte("a"), 10)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, []byte("va"), v)
	}
}

func TestTableReaderIterator(t *testing.T) {
	t.Parallel()
	entries := []tableEntry{
		{"a", 2, ikeyKindSet, "a2"},
		{"a", 1, ikeyKindSet, "a1"},
		{"b", 1, ikeyKindDelete, ""},
		{"c", 1, ikeyKindSet, "c1"},
	}
	tr := buildTable(t, 64, nil, entries)

	it := tr.newIterator()
	var userKeys []string
	count := 0
	for it.Next() {
		userKeys = append(userKeys, string(ikeyUserKey(it.internalKey())))
		require.NotNil(t, it.Value())
		count++
	}
	require.NoError(t, it.Error())
	assert.Equal(t, len(entries), count)
	assert.Equal(t, []string{"a", "a", "b", "c"}, userKeys)
}

// errReaderAt fails every ReadAt, to exercise the read-error branches.
type errReaderAt struct{}

var errRead = errors.New("errReaderAt: read failed")

func (errReaderAt) ReadAt([]byte, int64) (int, error) { return 0, errRead }

func TestTableReaderReadErrors(t *testing.T) {
	t.Parallel()

	t.Run("readBlockRaw ReadAt error", func(t *testing.T) {
		// size is large enough that the handle passes the range check, so the
		// failing ReadAt is what surfaces.
		tr := &tableReader{r: errReaderAt{}, size: 1024}
		_, err := tr.readBlockRaw(blockHandle{offset: 0, length: 8})
		assert.ErrorIs(t, err, errRead)
	})

	t.Run("readBlockRaw handle out of range", func(t *testing.T) {
		tr := &tableReader{r: errReaderAt{}, size: 4}
		_, err := tr.readBlockRaw(blockHandle{offset: 0, length: 8})
		assert.ErrorContains(t, err, "out of range")
	})

	t.Run("get surfaces block read error", func(t *testing.T) {
		tr := buildTable(t, 4096, nil, []tableEntry{{"a", 1, ikeyKindSet, "va"}})
		// Keep the loaded filter/index but make data-block reads fail.
		tr.r = errReaderAt{}
		_, _, _, err := tr.get([]byte("a"), 10)
		assert.ErrorIs(t, err, errRead)
	})

	t.Run("iterator Next surfaces block read error", func(t *testing.T) {
		tr := buildTable(t, 64, nil, []tableEntry{
			{"a", 1, ikeyKindSet, "va"},
			{"b", 1, ikeyKindSet, "vb"},
		})
		tr.r = errReaderAt{}
		it := tr.newIterator()
		assert.False(t, it.Next())
		assert.ErrorIs(t, it.Error(), errRead)
	})
}

func TestTableReaderBadFile(t *testing.T) {
	t.Parallel()
	t.Run("too_small", func(t *testing.T) {
		_, err := newCachedTableReader(bytes.NewReader([]byte("short")), 5, nil, 1)
		assert.Error(t, err)
	})

	t.Run("bad_magic", func(t *testing.T) {
		bad := make([]byte, footerLen)
		_, err := newCachedTableReader(bytes.NewReader(bad), int64(len(bad)), nil, 1)
		assert.Error(t, err)
	})
}

// buildBenchTable writes n sequential 8-byte-key entries into an s2 table and
// returns a cached reader over it.
func buildBenchTable(b *testing.B, n int) *tableReader {
	b.Helper()
	c, _ := codecFromName("s2")
	var buf bytes.Buffer
	tw := newTableWriter(&buf, tableWriterConfig{codec: c, bloomBits: 10, blockSize: 4096})
	val := make([]byte, 100)
	for i := 0; i < n; i++ {
		var uk [8]byte
		binary.BigEndian.PutUint64(uk[:], uint64(i))
		if err := tw.Add(ikeyEncode(nil, uk[:], uint64(i+1), ikeyKindSet), val); err != nil {
			b.Fatal(err)
		}
	}
	size, err := tw.finish()
	if err != nil {
		b.Fatal(err)
	}
	tr, err := newCachedTableReader(bytes.NewReader(buf.Bytes()), size, newBlockCache(64<<20), 1)
	if err != nil {
		b.Fatal(err)
	}
	return tr
}

func benchProbeKey(i, n int) []byte {
	var uk [8]byte
	binary.BigEndian.PutUint64(uk[:], uint64(i)*2654435761%uint64(n))
	return uk[:]
}

func BenchmarkTableGet(b *testing.B) {
	const n = 50000
	tr := buildBenchTable(b, n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, _, err := tr.get(benchProbeKey(i, n), uint64(n+1)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTableHas(b *testing.B) {
	const n = 50000
	tr := buildBenchTable(b, n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := tr.has(benchProbeKey(i, n), uint64(n+1)); err != nil {
			b.Fatal(err)
		}
	}
}

func TestTableReaderLazyMetaDropAndRebuild(t *testing.T) {
	t.Parallel()
	entries := []tableEntry{
		{"a", 1, ikeyKindSet, "va"},
		{"b", 2, ikeyKindSet, "vb"},
		{"c", 3, ikeyKindSet, "vc"},
	}
	tr := buildTable(t, 4096, nil, entries)

	// Meta is parsed at open.
	require.NotNil(t, tr.meta.Load(), "metadata parsed at open")

	// Simulate an fd eviction dropping the parsed metadata.
	tr.dropMeta()
	assert.Nil(t, tr.meta.Load(), "dropMeta releases parsed metadata")

	// A read rebuilds it and returns correct data.
	v, found, deleted, err := tr.get([]byte("b"), 10)
	require.NoError(t, err)
	assert.True(t, found)
	assert.False(t, deleted)
	assert.Equal(t, []byte("vb"), v)
	assert.NotNil(t, tr.meta.Load(), "metadata rebuilt on demand after a drop")
}

func TestTableMetadataEvictedWithDescriptor(t *testing.T) {
	t.Parallel()
	// A tiny fd limit forces descriptor eviction across many tables; reads must
	// stay correct as metadata is dropped and rebuilt, and open metadata must be
	// bounded (not every table retains it).
	dir := t.TempDir()
	tablePath := func(num uint32) (string, error) {
		return filepath.Join(dir, fmt.Sprintf("%08x.sst", num)), nil
	}
	pool := newFDPool(3)
	cfg := shardConfigT{MemtableSize: 1 << 20, BloomBits: 10, BlockSize: 256, FreshCodecName: "none", FDs: pool}
	s := newShard(cfg, newAllocator(0), tablePath, 1)
	defer s.Close()

	const tables = 12
	for tbl := 0; tbl < tables; tbl++ {
		s.Put(uint64(tbl+1), []byte(fmt.Sprintf("k%02d", tbl)), []byte(fmt.Sprintf("v%02d", tbl)))
		require.NoError(t, s.Flush())
	}

	// Read every key twice; evictions during the sweep drop and rebuild metadata.
	for round := 0; round < 2; round++ {
		for tbl := 0; tbl < tables; tbl++ {
			assert.Equal(t, []byte(fmt.Sprintf("v%02d", tbl)), mustGet(t, s, 1000, fmt.Sprintf("k%02d", tbl)))
		}
	}

	// With a 3-descriptor limit over 12 tables, at least some readers must have
	// had their metadata dropped (bounded resident metadata, the point of #5).
	s.mu.RLock()
	dropped := 0
	for _, tm := range s.tables {
		if tm.reader.meta.Load() == nil {
			dropped++
		}
	}
	s.mu.RUnlock()
	assert.Positive(t, dropped, "metadata for evicted tables should be released")
}
