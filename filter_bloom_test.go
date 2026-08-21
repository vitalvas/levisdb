package levisdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBloomClampsBitsPerKey(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 10, newBloom(10).bitsPerKey)
	assert.Equal(t, 1, newBloom(0).bitsPerKey)
	assert.Equal(t, 1, newBloom(-5).bitsPerKey)
}

func TestBloomProbes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		bitsPerKey int
		want       int
	}{
		{1, 1},     // rounds to below 1, clamped up
		{10, 7},    // round(10*0.69)=7
		{1000, 30}, // clamped down
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("bpk=%d", tc.bitsPerKey), func(t *testing.T) {
			assert.Equal(t, tc.want, newBloom(tc.bitsPerKey).probes())
		})
	}

	t.Run("zero bits per key clamps k up to 1", func(t *testing.T) {
		// newBloom clamps bitsPerKey to >= 1, so reach the k<1 clamp directly.
		assert.Equal(t, 1, (&bloomFilter{bitsPerKey: 0}).probes())
	})
}

func TestBloomBuildAndMayContain(t *testing.T) {
	t.Parallel()
	b := newBloom(10)
	keys := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma"), []byte("delta")}
	hashes := make([]uint32, len(keys))
	for i, k := range keys {
		hashes[i] = bloomHash(k)
	}
	filter := b.build(hashes)

	t.Run("no false negatives", func(t *testing.T) {
		for _, h := range hashes {
			assert.True(t, bloomMayContain(filter, h), "member must be reported present")
		}
	})

	t.Run("rejects absent keys", func(t *testing.T) {
		absent := 0
		for i := 0; i < 1000; i++ {
			h := bloomHash([]byte(fmt.Sprintf("absent-%d", i)))
			if !bloomMayContain(filter, h) {
				absent++
			}
		}
		assert.Greater(t, absent, 0, "bloom should reject most absent keys")
	})
}

func TestBloomFalsePositiveRate(t *testing.T) {
	t.Parallel()
	// At 10 bits/key the theoretical false-positive rate is ~1%. Power-of-two
	// sizing rounds the bit array up, so the observed rate should sit at or
	// below that; a regression here would signal poor hash distribution after
	// the switch to low-bit masking.
	const n = 10000
	b := newBloom(10)
	hashes := make([]uint32, n)
	present := make(map[uint32]struct{}, n)
	for i := range hashes {
		hashes[i] = bloomHash([]byte(fmt.Sprintf("member-%d", i)))
		present[hashes[i]] = struct{}{}
	}
	filter := b.build(hashes)

	// No false negatives for any member.
	for _, h := range hashes {
		require.True(t, bloomMayContain(filter, h))
	}

	// Measure false positives over disjoint absent keys.
	const trials = 20000
	fp := 0
	for i := 0; i < trials; i++ {
		h := bloomHash([]byte(fmt.Sprintf("absent-%d", i)))
		if _, ok := present[h]; ok {
			continue
		}
		if bloomMayContain(filter, h) {
			fp++
		}
	}
	rate := float64(fp) / float64(trials)
	assert.Less(t, rate, 0.02, "false-positive rate %.4f exceeds 2%% budget", rate)
}

func TestBloomBuildMinSize(t *testing.T) {
	t.Parallel()
	b := newBloom(10)
	filter := b.build([]uint32{bloomHash([]byte("only"))})
	// bits floored at 64 -> 8 bytes + 1 trailer.
	assert.Equal(t, 9, len(filter))
	assert.Equal(t, byte(b.probes()), filter[len(filter)-1])
}

func TestBloomBuildEmpty(t *testing.T) {
	t.Parallel()
	filter := newBloom(10).build(nil)
	require.GreaterOrEqual(t, len(filter), 2)
	// No keys were added, so any query should miss.
	assert.False(t, bloomMayContain(filter, bloomHash([]byte("x"))))
}

func TestBloomMayContainEdgeCases(t *testing.T) {
	t.Parallel()
	t.Run("too short", func(t *testing.T) {
		assert.False(t, bloomMayContain(nil, 1))
		assert.False(t, bloomMayContain([]byte{0}, 1))
	})

	t.Run("reserved k treated as maybe", func(t *testing.T) {
		filter := []byte{0, 0, 31} // k=31 > 30
		assert.True(t, bloomMayContain(filter, 12345))
	})
}

func TestValidateBloomFilter(t *testing.T) {
	t.Parallel()
	require.NoError(t, validateBloomFilter(newBloom(10).build([]uint32{1, 2, 3})))
	assert.Error(t, validateBloomFilter(nil))
	assert.Error(t, validateBloomFilter(make([]byte, 8)))
	assert.Error(t, validateBloomFilter(make([]byte, 10)))

	badProbes := newBloom(10).build([]uint32{1})
	badProbes[len(badProbes)-1] = 31
	assert.Error(t, validateBloomFilter(badProbes))
}

func TestBloomHash(t *testing.T) {
	t.Parallel()
	// Deterministic, and distinct for keys differing by one byte.
	assert.Equal(t, bloomHash([]byte("key")), bloomHash([]byte("key")))
	assert.NotEqual(t, bloomHash([]byte("key")), bloomHash([]byte("kez")))
	assert.Equal(t, bloomHash(nil), bloomHash([]byte{}))
	// The avalanche step must spread near-identical single-byte keys apart.
	assert.NotEqual(t, bloomHash([]byte{0x00}), bloomHash([]byte{0x01}))
}

func FuzzBloomMayContain(f *testing.F) {
	f.Add([]byte{}, uint32(0))
	f.Add([]byte{0x01}, uint32(123))
	f.Add(newBloom(10).build([]uint32{bloomHash([]byte("k"))}), bloomHash([]byte("k")))

	f.Fuzz(func(_ *testing.T, filter []byte, hash uint32) {
		// Contract: bloomMayContain never panics on an arbitrary filter blob,
		// regardless of the trailing probe-count byte or bit-array length.
		_ = bloomMayContain(filter, hash)
	})
}

func FuzzBloomRoundTrip(f *testing.F) {
	f.Add([]byte("a"), []byte("b"), []byte("c"))

	f.Fuzz(func(t *testing.T, k1, k2, probe []byte) {
		// A built filter must never report a false negative for a key it holds.
		b := newBloom(10)
		filter := b.build([]uint32{bloomHash(k1), bloomHash(k2)})
		assert.True(t, bloomMayContain(filter, bloomHash(k1)))
		assert.True(t, bloomMayContain(filter, bloomHash(k2)))
		_ = bloomMayContain(filter, bloomHash(probe))
	})
}

func BenchmarkBloomBuild(b *testing.B) {
	const n = 10000
	hashes := make([]uint32, n)
	for i := range hashes {
		hashes[i] = bloomHash([]byte(fmt.Sprintf("key%d", i)))
	}
	bl := newBloom(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bl.build(hashes)
	}
}

func BenchmarkBloomMayContain(b *testing.B) {
	const n = 10000
	hashes := make([]uint32, n)
	for i := range hashes {
		hashes[i] = bloomHash([]byte(fmt.Sprintf("key%d", i)))
	}
	filter := newBloom(10).build(hashes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bloomMayContain(filter, hashes[i%n])
	}
}
