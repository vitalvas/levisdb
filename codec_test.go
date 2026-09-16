package levisdb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCodecConstants pins the exported codec-name constants to their string
// values: they are stored in the manifest/table format indirectly and are part
// of the public API, so a rename must be deliberate, not accidental.
func TestCodecConstants(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "none", CodecNone)
	assert.Equal(t, "s2", CodecS2)
	assert.Equal(t, "zstd", CodecZstd)
	assert.Equal(t, "flate", CodecFlate)
}

func TestCodecFromName(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		id     codecID
		codecN string
	}{
		{CodecNone, codecNone, CodecNone},
		{CodecS2, codecS2, CodecS2},
		{CodecZstd, codecZstd, CodecZstd},
		{CodecFlate, codecFlate, CodecFlate},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := codecFromName(tc.name)
			require.NoError(t, err)
			assert.Equal(t, tc.id, c.id())
			assert.Equal(t, tc.codecN, c.Name())
		})
	}

	t.Run("unknown", func(t *testing.T) {
		c, err := codecFromName("bogus")
		require.Error(t, err)
		assert.Nil(t, c)
	})
}

func TestCodecFromID(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		id   codecID
	}{
		{"none", codecNone},
		{"s2", codecS2},
		{"zstd", codecZstd},
		{"flate", codecFlate},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := codecFromID(tc.id)
			require.NoError(t, err)
			assert.Equal(t, tc.id, c.id())
		})
	}

	t.Run("unknown", func(t *testing.T) {
		c, err := codecFromID(codecID(99))
		require.Error(t, err)
		assert.Nil(t, c)
	})
}

func TestResolveLevelCodec(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		levelCodecs []string
		depth       int
		bottomTier  bool
		want        string
	}{
		{"nil falls back to fresh", nil, 0, false, CodecS2},
		{"nil falls back to bottom", nil, 3, true, CodecZstd},
		{"override wins over fresh", []string{CodecZstd}, 0, false, CodecZstd},
		{"override wins over bottom", []string{"", "", CodecS2}, 2, true, CodecS2},
		{"empty entry falls back to fresh", []string{""}, 0, false, CodecS2},
		{"empty entry falls back to bottom", []string{"", ""}, 1, true, CodecZstd},
		{"depth past slice falls back", []string{CodecNone}, 5, false, CodecS2},
		{"none override honored", []string{CodecNone}, 0, false, CodecNone},
		{"negative depth falls back", []string{CodecNone}, -1, true, CodecZstd},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveLevelCodec(tc.levelCodecs, tc.depth, CodecS2, CodecZstd, tc.bottomTier)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestIncompressibleValuesRoundTrip writes near-random values that no codec can
// shrink and reads them all back after flush and compaction, exercising the
// size-check fallback (a block that would not shrink is stored raw) end to end.
func TestIncompressibleValuesRoundTrip(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) {
		o.MemtableSize = 4 << 10
		o.FreshCodec = CodecS2
		o.BottomCodec = CodecZstd
	})
	// Near-random, deterministic values the codec cannot compress, so each block
	// falls back to raw via the size-check.
	mkVal := func(i int) []byte {
		v := make([]byte, 256)
		x := uint64(i)*0x9e3779b97f4a7c15 + 1
		for j := range v {
			x = x*6364136223846793005 + 1442695040888963407
			v[j] = byte(x >> 56)
		}
		return v
	}
	const n = 400
	for i := 0; i < n; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%05d", i)), Value: mkVal(i)}))
	}
	db.sched.drain()
	require.NoError(t, db.CompactRange(nil, nil)) // force flush + compaction
	for i := 0; i < n; i++ {
		got, err := db.Get([]byte(fmt.Sprintf("k%05d", i)))
		require.NoError(t, err, "i=%d", i)
		assert.Equal(t, mkVal(i), got, "i=%d round-trips", i)
	}
}

// TestLevelCodecsAppliedToFlushedTable writes a compressible workload with a
// per-level override for depth 0 and checks the flushed L0 table's blocks were
// encoded with that codec, proving LevelCodecs threads through the flush path.
func TestLevelCodecsAppliedToFlushedTable(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) {
		// Large memtable so the workload lands in one or two L0 tables and is not
		// immediately compacted down to a deeper (non-overridden) depth.
		o.MemtableSize = 64 << 10
		// Default fresh codec is none in the test helper; force depth 0 to zstd so a
		// match proves the override took effect rather than a default.
		o.FreshCodec = CodecNone
		o.LevelCodecs = []string{CodecZstd}
	})

	// Highly compressible values so the size-fallback in finishBlock does not
	// downgrade the block to none.
	val := bytes.Repeat([]byte("levisdb"), 128)
	for i := 0; i < 200; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%05d", i)), Value: val}))
	}
	db.sched.drain()

	ids := depth0BlockCodecIDs(t, db.eng)
	require.NotEmpty(t, ids, "expected at least one flushed L0 table")
	assert.Contains(t, ids, codecZstd, "L0 blocks should use the depth-0 override (zstd)")
	assert.NotContains(t, ids, codecS2, "no L0 block should use s2")
}

// TestFlateCodecEndToEnd proves the flate codec threads through the on-disk path:
// a flate-configured DB writes compressible data, the on-disk blocks carry the
// flate codec id, and every value reads back correctly. A forced CompactRange
// settles the table set deterministically before inspecting codec ids, so the
// assertion does not race background compaction moving tables between tiers.
func TestFlateCodecEndToEnd(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) {
		o.MemtableSize = 64 << 10
		o.FreshCodec = CodecFlate
		o.BottomCodec = CodecFlate
	})

	val := bytes.Repeat([]byte("levisdb-flate"), 128) // compressible so flate is kept
	const n = 300
	for i := 0; i < n; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%05d", i)), Value: val}))
	}
	// CompactRange forces flush + compaction and blocks until done, so the live
	// table set is stable when we inspect it (no background flush/compaction race).
	require.NoError(t, db.CompactRange(nil, nil))

	ids := allBlockCodecIDs(t, db.eng)
	require.NotEmpty(t, ids, "expected at least one on-disk table")
	assert.Contains(t, ids, codecFlate, "on-disk blocks should use the flate codec")

	for i := 0; i < n; i++ {
		got, err := db.Get([]byte(fmt.Sprintf("k%05d", i)))
		require.NoError(t, err, "i=%d", i)
		assert.Equal(t, val, got, "i=%d round-trips through flate on disk", i)
	}
}

// allBlockCodecIDs returns the codec ids of the first data block of every live
// table (any depth). Layout from finishBlock: [payload][codec id][crc32].
func allBlockCodecIDs(t *testing.T, s *engineT) map[codecID]bool {
	t.Helper()
	s.mu.RLock()
	metas := append([]*tableMeta(nil), s.tables...)
	s.mu.RUnlock()

	ids := map[codecID]bool{}
	for _, tbl := range metas {
		m, err := tbl.reader.ensureMeta()
		require.NoError(t, err)
		h := m.blockHandleAt(0)
		buf := make([]byte, h.length)
		_, err = tbl.reader.r.ReadAt(buf, int64(h.offset))
		require.NoError(t, err)
		body := buf[:len(buf)-4]
		ids[codecID(body[len(body)-1])] = true
	}
	return ids
}

// depth0BlockCodecIDs reads the first data block of every live depth-0 table in
// the engine raw (bypassing decodeBlock) and returns the set of codec ids from
// their trailers. Trailer layout from finishBlock: [payload][codec id][crc32].
func depth0BlockCodecIDs(t *testing.T, s *engineT) map[codecID]bool {
	t.Helper()
	s.mu.RLock()
	var metas []*tableMeta
	for _, tbl := range s.tables {
		if tbl.depth == 0 {
			metas = append(metas, tbl)
		}
	}
	s.mu.RUnlock()

	ids := map[codecID]bool{}
	for _, tbl := range metas {
		m, err := tbl.reader.ensureMeta()
		require.NoError(t, err)
		h := m.blockHandleAt(0)
		buf := make([]byte, h.length)
		_, err = tbl.reader.r.ReadAt(buf, int64(h.offset))
		require.NoError(t, err)
		body := buf[:len(buf)-4]
		ids[codecID(body[len(body)-1])] = true
	}
	return ids
}

func TestCodecRoundTrip(t *testing.T) {
	t.Parallel()
	src := bytes.Repeat([]byte("levisdb block payload "), 64)
	for _, name := range []string{"none", "s2", "zstd", "flate"} {
		t.Run(name, func(t *testing.T) {
			c, err := codecFromName(name)
			require.NoError(t, err)

			enc := c.compress(nil, src)
			dec, err := c.decompress(nil, enc)
			require.NoError(t, err)
			assert.Equal(t, src, dec)
		})
	}
}

func TestCodecCompressAppendsToDst(t *testing.T) {
	t.Parallel()
	c, err := codecFromName("none")
	require.NoError(t, err)
	prefix := []byte("keep")
	out := c.compress(prefix, []byte("more"))
	assert.Equal(t, []byte("keepmore"), out)

	dec, err := c.decompress([]byte("head"), []byte("tail"))
	require.NoError(t, err)
	assert.Equal(t, []byte("headtail"), dec)
}

func TestCodecDecompressError(t *testing.T) {
	t.Parallel()
	t.Run("s2", func(t *testing.T) {
		c, err := codecFromName("s2")
		require.NoError(t, err)
		_, err = c.decompress(nil, []byte("not valid s2 stream"))
		require.Error(t, err)
	})

	t.Run("zstd", func(t *testing.T) {
		c, err := codecFromName("zstd")
		require.NoError(t, err)
		_, err = c.decompress(nil, []byte("not valid zstd stream"))
		require.Error(t, err)
	})

	t.Run("flate", func(t *testing.T) {
		c, err := codecFromName("flate")
		require.NoError(t, err)
		_, err = c.decompress(nil, []byte("not valid flate stream"))
		require.Error(t, err)
	})
}

func FuzzCodecRoundTrip(f *testing.F) {
	f.Add([]byte("levisdb block payload"))
	f.Add([]byte{})
	f.Add([]byte{0x00, 0xff, 0x00, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, name := range []string{"none", "s2", "zstd", "flate"} {
			c, err := codecFromName(name)
			require.NoError(t, err)

			comp := c.compress(nil, data)
			dec, err := c.decompress(nil, comp)
			require.NoError(t, err)
			assert.True(t, bytes.Equal(data, dec), "%s round trip mismatch", name)

			// Decompressing arbitrary bytes must not panic; errors are fine.
			_, _ = c.decompress(nil, data)
		}
	})
}

func benchBlock() []byte {
	// A realistic ~4 KiB block: semi-compressible key/value-ish bytes.
	out := make([]byte, 0, 4096)
	for len(out) < 4096 {
		out = append(out, []byte("key000123value-some-payload-data;")...)
	}
	return out[:4096]
}

func BenchmarkCodecCompress(b *testing.B) {
	src := benchBlock()
	for _, name := range []string{"none", "s2", "zstd", "flate"} {
		c, _ := codecFromName(name)
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(src)))
			var dst []byte
			for i := 0; i < b.N; i++ {
				dst = c.compress(dst[:0], src)
			}
		})
	}
}

func BenchmarkCodecDecompress(b *testing.B) {
	src := benchBlock()
	for _, name := range []string{"none", "s2", "zstd", "flate"} {
		c, _ := codecFromName(name)
		comp := c.compress(nil, src)
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(src)))
			var dst []byte
			for i := 0; i < b.N; i++ {
				dst, _ = c.decompress(dst[:0], comp)
			}
		})
	}
}
