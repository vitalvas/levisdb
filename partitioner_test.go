package levisdb

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashPartitionerDeterministicAndInRange(t *testing.T) {
	t.Parallel()
	p := HashPartitioner{}
	assert.Equal(t, "hash-fnv1a-v1", p.Name())
	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		s1 := p.Shard(k, 16)
		s2 := p.Shard(k, 16)
		assert.Equal(t, s1, s2, "deterministic")
		assert.GreaterOrEqual(t, s1, 0)
		assert.Less(t, s1, 16)
	}
}

// TestRangePartitionerShardRange verifies the scan-pruning bounds: the returned
// [lo,hi) must cover the shard of every key that can fall in [start,end), and it
// must be tight (skip shards no in-range key can reach).
func TestRangePartitionerShardRange(t *testing.T) {
	t.Parallel()
	p := RangePartitioner{}
	const numShards = 8

	// Property: for every first byte b, if a key starting with b is in [start,end),
	// then p.Shard(that key) is within [lo,hi). Checked by brute force over bytes.
	check := func(start, end []byte) {
		lo, hi := p.ShardRange(start, end, numShards)
		for b := 0; b < 256; b++ {
			key := []byte{byte(b)}
			inRange := (start == nil || bytes.Compare(key, start) >= 0) && (end == nil || bytes.Compare(key, end) < 0)
			if !inRange {
				continue
			}
			s := p.Shard(key, numShards)
			assert.GreaterOrEqualf(t, s, lo, "key %d shard %d below lo %d", b, s, lo)
			assert.Lessf(t, s, hi, "key %d shard %d at/above hi %d", b, s, hi)
		}
	}

	check(nil, nil)                   // full scan -> all shards
	check([]byte{0x00}, []byte{0xff}) // near-full
	check([]byte{0x40}, []byte{0x80}) // a middle slice
	check([]byte{0x10}, []byte{0x11}) // single first-byte
	check(nil, []byte{0x40})          // unbounded start
	check([]byte{0xc0}, nil)          // unbounded end

	// Full scan spans every shard; a narrow slice spans fewer.
	lo, hi := p.ShardRange(nil, nil, numShards)
	assert.Equal(t, 0, lo)
	assert.Equal(t, numShards, hi)
	lo2, hi2 := p.ShardRange([]byte{0x10}, []byte{0x11}, numShards)
	assert.Less(t, hi2-lo2, numShards, "a one-byte-wide range prunes to fewer shards")
}

// TestHashPartitionersAreNotShardRangers pins that hash partitioners do NOT
// implement ShardRanger: their tokens are not key-ordered, so scan-pruning would
// drop keys. The iterator must scan all shards for them.
func TestHashPartitionersAreNotShardRangers(t *testing.T) {
	t.Parallel()
	_, hashOK := any(HashPartitioner{}).(ShardRanger)
	_, murmurOK := any(Murmur3Partitioner{}).(ShardRanger)
	_, rangeOK := any(RangePartitioner{}).(ShardRanger)
	assert.False(t, hashOK, "hash (FNV-1a) must not be a ShardRanger")
	assert.False(t, murmurOK, "murmur3 token ring must not be a ShardRanger")
	assert.True(t, rangeOK, "range partitioner is order-preserving and must prune")
}

func TestHashPartitionerSpread(t *testing.T) {
	t.Parallel()
	p := HashPartitioner{}
	counts := make([]int, 8)
	for i := 0; i < 8000; i++ {
		counts[p.Shard([]byte(fmt.Sprintf("k%d", i)), 8)]++
	}
	// Every shard should get a reasonable share (not perfectly even).
	for i, c := range counts {
		assert.Greater(t, c, 500, "shard %d underloaded: %d", i, c)
	}
}

func TestMurmur3PartitionerDeterministicAndInRange(t *testing.T) {
	t.Parallel()
	p := Murmur3Partitioner{}
	assert.Equal(t, "murmur3-token-ring-v1", p.Name())
	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		s1 := p.Shard(k, 16)
		s2 := p.Shard(k, 16)
		assert.Equal(t, s1, s2, "deterministic")
		assert.GreaterOrEqual(t, s1, 0)
		assert.Less(t, s1, 16)
	}
}

func TestMurmur3PartitionerSpread(t *testing.T) {
	t.Parallel()
	p := Murmur3Partitioner{}
	counts := make([]int, 8)
	for i := 0; i < 8000; i++ {
		counts[p.Shard([]byte(fmt.Sprintf("k%d", i)), 8)]++
	}
	for i, c := range counts {
		assert.Greater(t, c, 500, "shard %d underloaded: %d", i, c)
	}
}

// murmur3ShardOf maps a raw token to a shard the same way Murmur3Partitioner
// does, so tests can probe the token->shard mapping directly.
func murmur3ShardOf(token int64, numShards int) int {
	hi, _ := bits.Mul64(ringPos(token), uint64(numShards))
	return int(hi)
}

// TestMurmur3MapsEveryTokenToValidShard checks the direct multiply-shift mapping
// assigns every token to a valid shard across the whole int64 range, including
// the extreme tokens at both ends where a naive division could over/underflow.
func TestMurmur3MapsEveryTokenToValidShard(t *testing.T) {
	t.Parallel()
	for _, numShards := range []int{1, 2, 8, 32, 256, 1000} {
		tokens := []int64{math.MinInt64, math.MinInt64 + 1, -1, 0, 1, math.MaxInt64 - 1, math.MaxInt64}
		for i := 0; i < 10000; i++ {
			tokens = append(tokens, int64(uint64(i)*0x9e3779b97f4a7c15))
		}
		for _, tok := range tokens {
			s := murmur3ShardOf(tok, numShards)
			assert.GreaterOrEqualf(t, s, 0, "shards=%d token=%d", numShards, tok)
			assert.Lessf(t, s, numShards, "shards=%d token=%d", numShards, tok)
		}
	}
	// The most-negative token is ring position 0 -> shard 0; the most-positive is
	// ring position 2^64-1 -> the last shard. Confirm both endpoints.
	assert.Equal(t, 0, murmur3ShardOf(math.MinInt64, 8))
	assert.Equal(t, 7, murmur3ShardOf(math.MaxInt64, 8))
}

// TestMurmur3SmoothsClusteredKeys shows MurmurHash3 spreads even highly
// clustered sequential keys evenly across all shards (this is why the hash
// partitioner exists; no vnodes are needed for it).
func TestMurmur3SmoothsClusteredKeys(t *testing.T) {
	t.Parallel()
	p := Murmur3Partitioner{}
	const numShards = 16
	counts := make([]int, numShards)
	// Highly clustered keys: a fixed prefix with a small incrementing suffix.
	for i := 0; i < 16000; i++ {
		counts[p.Shard([]byte(fmt.Sprintf("user:session:%06d", i)), numShards)]++
	}
	for i, c := range counts {
		assert.Greater(t, c, 500, "shard %d underloaded under clustered keys: %d", i, c)
	}
}

// TestMurmur3Deterministic confirms the mapping is a pure function of key and
// shard count, so it is identical across reopens.
func TestMurmur3Deterministic(t *testing.T) {
	t.Parallel()
	p := Murmur3Partitioner{}
	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		assert.Equal(t, p.Shard(k, 7), p.Shard(k, 7))
	}
}

// TestMurmur3KnownAnswers pins the hash to canonical MurmurHash3 x64_128 test
// vectors (seed 0) so this stays a real Murmur3 x64_128 and the shard mapping is
// stable across versions (the partitioner identity is baked into data). The
// empty input hashes to (0, 0) by definition.
func TestMurmur3KnownAnswers(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in     string
		h1, h2 uint64
	}{
		{"", 0, 0},
		{"hello", 0xcbd8a7b341bd9b02, 0x5b1e906a48ae1d19},
		{"The quick brown fox jumps over the lazy dog", 0xe34bbc7bbc071b6c, 0x7a433ca9c49a9347},
	}
	for _, tc := range cases {
		h1, h2 := murmur3x64128([]byte(tc.in), 0)
		assert.Equalf(t, tc.h1, h1, "murmur3_x64_128(%q) h1", tc.in)
		assert.Equalf(t, tc.h2, h2, "murmur3_x64_128(%q) h2", tc.in)
	}
}

func TestRangePartitionerContiguous(t *testing.T) {
	t.Parallel()
	p := RangePartitioner{}
	assert.Equal(t, "range-first-byte-v1", p.Name())

	// Low first bytes map to low shards, high to high.
	assert.Equal(t, 0, p.Shard([]byte{0x00}, 16))
	assert.Equal(t, 15, p.Shard([]byte{0xff}, 16))

	// Contiguity: shard index is monotonic in the first byte.
	prev := -1
	for b := 0; b < 256; b++ {
		s := p.Shard([]byte{byte(b)}, 16)
		assert.GreaterOrEqual(t, s, prev)
		prev = s
	}
}

func TestRangePartitionerEmptyKey(t *testing.T) {
	t.Parallel()
	p := RangePartitioner{}
	assert.Equal(t, 0, p.Shard(nil, 16))
}

// note: the `idx >= numShards` clamp in RangePartitioner.Shard is unreachable
// for valid inputs. With a first byte in [0,255], idx = first*numShards/256 is
// at most 255*numShards/256 < numShards, so the clamp only guards against
// integer overflow at absurd shard counts. It is left as a defensive check.
func TestRangePartitionerMaxByteStaysInRange(t *testing.T) {
	t.Parallel()
	p := RangePartitioner{}
	for _, n := range []int{1, 2, 7, 16, 100, 256} {
		assert.Less(t, p.Shard([]byte{0xff}, n), n)
		assert.GreaterOrEqual(t, p.Shard([]byte{0xff}, n), 0)
	}
}

// TestPartitionerConstants pins the built-in selector constants to their string
// values: they choose which partitioner (and thus shard mapping) a database
// uses, so a rename must be deliberate.
func TestPartitionerConstants(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "hash", PartitionerHash)
	assert.Equal(t, "murmur3", PartitionerMurmur3)
	assert.Equal(t, "range", PartitionerRange)
}

func TestResolvePartitioner(t *testing.T) {
	t.Parallel()
	o := DefaultOptions("/tmp/x")
	assert.Equal(t, "hash-fnv1a-v1", o.resolvePartitioner().Name())

	o.Partitioner = PartitionerRange
	assert.Equal(t, "range-first-byte-v1", o.resolvePartitioner().Name())

	o.Partitioner = PartitionerMurmur3
	assert.Equal(t, "murmur3-token-ring-v1", o.resolvePartitioner().Name())

	o.CustomPartitioner = namedPartitioner{name: "custom"}
	assert.Equal(t, "custom", o.resolvePartitioner().Name())
}

func TestCustomPartitionerRoundTrips(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	custom := everythingToShardZero{}

	o := DefaultOptions(dir)
	o.ShardCount = 4
	o.CustomPartitioner = custom
	db, err := Open(o)
	assert.NoError(t, err)
	assert.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
	assert.NoError(t, db.Close())

	// Reopening with the same custom name succeeds.
	o2 := DefaultOptions(dir)
	o2.ShardCount = 4
	o2.CustomPartitioner = custom
	db2, err := Open(o2)
	assert.NoError(t, err)
	v, err := db2.Get([]byte("k"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v"), v)
	assert.NoError(t, db2.Close())

	// Reopening with a different custom name is rejected.
	o3 := DefaultOptions(dir)
	o3.ShardCount = 4
	o3.CustomPartitioner = namedPartitioner{name: "different"}
	_, err = Open(o3)
	assert.ErrorIs(t, err, ErrPartitionerMismatch)
}

type everythingToShardZero struct{}

func (everythingToShardZero) Shard(_ []byte, _ int) int { return 0 }
func (everythingToShardZero) Name() string              { return "all-zero" }

func BenchmarkPartitionerShard(b *testing.B) {
	keys := make([][]byte, 256)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
	}
	const numShards = 32
	b.Run("hash", func(b *testing.B) {
		p := HashPartitioner{}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			p.Shard(keys[i%len(keys)], numShards)
		}
	})
	b.Run("range", func(b *testing.B) {
		p := RangePartitioner{}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			p.Shard(keys[i%len(keys)], numShards)
		}
	})
	b.Run("murmur3", func(b *testing.B) {
		p := Murmur3Partitioner{}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			p.Shard(keys[i%len(keys)], numShards)
		}
	})
}

func FuzzPartitionerShard(f *testing.F) {
	f.Add([]byte("key"), uint16(16))
	f.Add([]byte{}, uint16(1))
	f.Add([]byte{0xff}, uint16(256))
	f.Fuzz(func(t *testing.T, key []byte, ns uint16) {
		n := int(ns%1024) + 1 // clamp to [1, 1024]
		for _, p := range []Partitioner{HashPartitioner{}, RangePartitioner{}, Murmur3Partitioner{}} {
			s := p.Shard(key, n)
			assert.GreaterOrEqual(t, s, 0, "%s shard below 0", p.Name())
			assert.Less(t, s, n, "%s shard >= numShards", p.Name())
		}
	})
}
