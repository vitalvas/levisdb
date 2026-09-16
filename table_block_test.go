package levisdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockHandle(t *testing.T) {
	t.Parallel()
	h := blockHandle{offset: 42, length: 1000}
	enc := h.encode(nil)
	got, n := decodeHandle(enc)
	assert.Equal(t, len(enc), n)
	assert.Equal(t, h, got)
}

func TestBlockBuilder(t *testing.T) {
	t.Parallel()
	var b blockBuilder
	assert.True(t, b.empty())
	assert.Equal(t, 0, b.size())

	b.add([]byte("k1"), []byte("v1"))
	b.add([]byte("k2"), []byte("v2"))
	assert.False(t, b.empty())
	assert.Greater(t, b.size(), 0)

	b.reset()
	assert.True(t, b.empty())
	assert.Equal(t, 0, b.size())
}

func TestBlockRoundTrip(t *testing.T) {
	t.Parallel()
	c, err := codecFromName("none")
	require.NoError(t, err)

	var b blockBuilder
	entries := [][2]string{{"a", "va"}, {"b", "vb"}, {"c", "vc"}}
	for _, e := range entries {
		b.add([]byte(e[0]), []byte(e[1]))
	}

	raw := finishBlock(nil, b.buf, c)
	payload, err := decodeBlock(raw)
	require.NoError(t, err)

	it := newBlockIter(payload)
	var got [][2]string
	for it.next() {
		got = append(got, [2]string{string(it.key), string(it.value)})
	}
	require.Len(t, got, len(entries))
	for i, e := range entries {
		assert.Equal(t, e, got[i])
	}
}

func TestDataBlockRoundTrip(t *testing.T) {
	t.Parallel()
	// Enough entries to cross several restart intervals, with shared prefixes.
	var entries [][2]string
	for i := 0; i < 50; i++ {
		entries = append(entries, [2]string{
			fmt.Sprintf("user:%04d:name", i),
			fmt.Sprintf("value-%d", i),
		})
	}

	var b dataBlockBuilder
	b.restartInterval = 8
	for _, e := range entries {
		b.add([]byte(e[0]), []byte(e[1]))
	}
	payload := b.finish()

	it := newDataBlockIter(payload)
	var got [][2]string
	for it.next() {
		got = append(got, [2]string{string(it.key), string(it.value)})
	}
	require.NoError(t, it.Error())
	require.Len(t, got, len(entries))
	for i, e := range entries {
		assert.Equal(t, e, got[i], "entry %d round-trips", i)
	}
}

// TestDataBlockIterKeyStableAcrossOneAdvance verifies the one-advance stability
// contract mergeIter relies on: the key from before an advance stays valid
// through the next next() call (double-buffering), even though keys are
// reconstructed from the previous key's prefix.
func TestDataBlockIterKeyStableAcrossOneAdvance(t *testing.T) {
	t.Parallel()
	var b dataBlockBuilder
	b.restartInterval = 8
	keys := []string{"aaaa", "aaab", "aaac", "aaad"}
	for _, k := range keys {
		b.add([]byte(k), []byte("v"))
	}
	it := newDataBlockIter(b.finish())

	require.True(t, it.next())
	prev := it.key // capture, then advance once (as mergeIter does)
	require.True(t, it.next())
	// prev must still read as the first key despite the reconstruction advance.
	assert.Equal(t, "aaaa", string(prev), "key stays valid across one advance")
	assert.Equal(t, "aaab", string(it.key))
}

func TestDataBlockIterSeek(t *testing.T) {
	t.Parallel()
	// Build internal keys across several restart intervals.
	var b dataBlockBuilder
	b.restartInterval = 4
	var keys [][]byte
	for i := 0; i < 40; i++ {
		ik := ikeyEncode(nil, []byte(fmt.Sprintf("key%03d", i*2)), uint64(i+1), ikeyKindSet)
		keys = append(keys, ik)
		b.add(ik, []byte(fmt.Sprintf("v%d", i)))
	}
	payload := b.finish()

	// firstGE returns the first key >= target by linear scan (the oracle).
	firstGE := func(target []byte) int {
		for i, k := range keys {
			if ikeyCompare(k, target) >= 0 {
				return i
			}
		}
		return -1
	}

	seekTargets := [][]byte{
		ikeyEncode(nil, []byte("key000"), 1, ikeyKindSetTTL),   // at/before first
		ikeyEncode(nil, []byte("key019"), 100, ikeyKindSetTTL), // between entries
		ikeyEncode(nil, []byte("key040"), 100, ikeyKindSetTTL), // mid
		ikeyEncode(nil, []byte("key078"), 100, ikeyKindSetTTL), // last region
		ikeyEncode(nil, []byte("zzz999"), 1, ikeyKindSetTTL),   // past end
	}
	for _, target := range seekTargets {
		it := newDataBlockIter(payload)
		it.seek(target)
		// seek positions at-or-before the target's restart interval; the caller
		// scans forward to the first key >= target (as lookupAtTime does).
		var got []byte
		for it.next() {
			if ikeyCompare(it.key, target) >= 0 {
				got = append([]byte(nil), it.key...)
				break
			}
		}
		require.NoError(t, it.Error())
		want := firstGE(target)
		if want < 0 {
			assert.Nil(t, got, "no key >= target %q exists", target)
			continue
		}
		require.NotNil(t, got, "forward scan after seek must find a key >= %q", target)
		assert.Equal(t, string(keys[want]), string(got),
			"seek+scan(%q) reaches the first key >= target without skipping it", target)
	}
}

func TestDataBlockIterCorruptTrailer(t *testing.T) {
	t.Parallel()
	// A malformed trailer surfaces as an iterator error, not a panic.
	// newDataBlockIter carries the split error, so seek/next stay no-ops.
	for _, payload := range [][]byte{
		{0xff, 0xff, 0xff, 0xff}, // restart count overflows the payload
		{1, 2},                   // shorter than the count word
	} {
		it := newDataBlockIter(payload)
		require.Error(t, it.Error())
		assert.False(t, it.next())
	}
}

func TestDecodeBlockErrors(t *testing.T) {
	t.Parallel()
	c, err := codecFromName("none")
	require.NoError(t, err)

	t.Run("too_short", func(t *testing.T) {
		_, err := decodeBlock([]byte{1, 2})
		assert.Error(t, err)
	})

	t.Run("crc_mismatch", func(t *testing.T) {
		var b blockBuilder
		b.add([]byte("k"), []byte("v"))
		raw := finishBlock(nil, b.buf, c)
		raw[0] ^= 0xff // corrupt payload
		_, err := decodeBlock(raw)
		assert.Error(t, err)
	})

	t.Run("unknown_codec_id", func(t *testing.T) {
		var b blockBuilder
		b.add([]byte("k"), []byte("v"))
		raw := finishBlock(nil, b.buf, c)
		// Rewrite the codec-id byte (just before the 4-byte CRC) to an unknown
		// value, then repair the CRC so decodeBlock reaches codecFromID.
		raw[len(raw)-5] = 0xff
		body := raw[:len(raw)-4]
		binary.LittleEndian.PutUint32(raw[len(raw)-4:], crc32.Checksum(body, tableCastagnoli))
		_, err := decodeBlock(raw)
		assert.Error(t, err)
	})
}

func FuzzDecodeBlock(f *testing.F) {
	// Seed with a valid block so the corpus starts from something meaningful.
	c, _ := codecFromName("none")
	var b blockBuilder
	b.add([]byte("k"), []byte("v"))
	f.Add(finishBlock(nil, b.buf, c))
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3, 4})

	f.Fuzz(func(_ *testing.T, raw []byte) {
		// Contract: decodeBlock never panics on arbitrary on-disk bytes; it
		// returns a payload or an error. (The block iterator is only ever run on
		// payloads produced by blockBuilder, so it is not fuzzed here.)
		_, _ = decodeBlock(raw)
	})
}

func FuzzDataBlockIter(f *testing.F) {
	// Seed with a valid prefix-compressed data block.
	var b dataBlockBuilder
	b.restartInterval = 4
	for _, k := range []string{"aa", "aab", "aac", "aad", "b"} {
		b.add([]byte(k), []byte("v"))
	}
	f.Add(b.finish())
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(_ *testing.T, payload []byte) {
		// Contract: splitting, seeking, and iterating an arbitrary data-block
		// payload never panics; it yields entries or an error.
		entries, restarts, n, err := splitDataBlock(payload)
		if err != nil {
			return
		}
		it := dataBlockIter{entries: entries, restarts: restarts, nRestart: n}
		it.seek([]byte("some-target-key\x00\x00\x00\x00\x00\x00\x00\x01"))
		for it.next() {
			_ = it.key
			_ = it.value
		}
		_ = it.Error()
	})
}

func FuzzDecodeHandle(f *testing.F) {
	f.Add(blockHandle{offset: 100, length: 4096}.encode(nil))
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff})

	f.Fuzz(func(_ *testing.T, raw []byte) {
		// Contract: decodeHandle never panics on arbitrary footer/index bytes.
		_, _ = decodeHandle(raw)
	})
}

// blockCodecID returns the codec id stored in a finished block (the byte just
// before the 4-byte CRC trailer).
func blockCodecID(raw []byte) codecID {
	return codecID(raw[len(raw)-blockTrailerLen])
}

// TestFinishBlockConditionalCompression covers the size-check that keeps the
// codec result only when it actually shrank the payload: a compressible block is
// compressed, an incompressible block falls back to raw, and no block is ever
// stored larger than its raw payload.
func TestFinishBlockConditionalCompression(t *testing.T) {
	t.Parallel()
	s2c, err := codecFromName(CodecS2)
	require.NoError(t, err)

	t.Run("compressible payload is compressed", func(t *testing.T) {
		payload := bytes.Repeat([]byte("abcdefgh"), 4096)
		raw := finishBlock(nil, payload, s2c)
		assert.Equal(t, codecS2, blockCodecID(raw), "compressible block should compress")
		assert.Less(t, len(raw), len(payload), "compressed smaller than raw")
		got, err := decodeBlock(raw)
		require.NoError(t, err)
		assert.Equal(t, payload, got, "round-trips")
	})

	t.Run("incompressible payload falls back to raw by size", func(t *testing.T) {
		// Deterministic near-random bytes via a multiplicative keystream (no rand):
		// the codec cannot shrink them, so the size-check stores the block raw.
		payload := make([]byte, 8192)
		x := uint64(0x9e3779b97f4a7c15)
		for i := range payload {
			x = x*6364136223846793005 + 1442695040888963407
			payload[i] = byte(x >> 56)
		}
		raw := finishBlock(nil, payload, s2c)
		assert.Equal(t, codecNone, blockCodecID(raw), "incompressible block stored raw via size-check")
		assert.LessOrEqual(t, len(raw), len(payload)+blockTrailerLen, "never larger than raw")
		got, err := decodeBlock(raw)
		require.NoError(t, err)
		assert.Equal(t, payload, got, "round-trips")
	})

	t.Run("never larger than raw (size-check fallback)", func(t *testing.T) {
		// Small payloads where the codec still wouldn't shrink below the raw size:
		// the stored block payload must not exceed raw + trailer.
		for _, n := range []int{1, 16, 64, 200} {
			payload := bytes.Repeat([]byte{0x7e}, n)
			raw := finishBlock(nil, payload, s2c)
			assert.LessOrEqual(t, len(raw), len(payload)+blockTrailerLen,
				"n=%d: block must never exceed raw payload + trailer", n)
			got, err := decodeBlock(raw)
			require.NoError(t, err)
			assert.Equal(t, payload, got)
		}
	})
}

func FuzzFinishBlockRoundTrip(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("hello world hello world"))
	f.Add(bytes.Repeat([]byte("a"), 5000))
	f.Fuzz(func(t *testing.T, payload []byte) {
		// Contract: for any payload, finishBlock -> decodeBlock returns the exact
		// payload, regardless of which codec/conditional path was taken, and the
		// stored block never exceeds raw payload + trailer.
		for _, name := range []string{CodecNone, CodecS2, CodecZstd} {
			c, err := codecFromName(name)
			require.NoError(t, err)
			raw := finishBlock(nil, payload, c)
			if name != CodecNone {
				assert.LessOrEqual(t, len(raw), len(payload)+blockTrailerLen, "%s: never larger than raw", name)
			}
			got, err := decodeBlock(raw)
			require.NoError(t, err, "%s", name)
			assert.Equal(t, payload, got, "%s round-trip", name)
		}
	})
}
