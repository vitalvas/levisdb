package levisdb

import (
	"encoding/binary"
	"math/bits"
)

// Built-in partitioner selectors for Options.Partitioner. Use these constants
// instead of bare strings when choosing a built-in key-to-shard mapping.
const (
	// PartitionerHash spreads keys evenly via FNV-1a. Default.
	PartitionerHash = "hash"
	// PartitionerMurmur3 spreads keys evenly via MurmurHash3, with better
	// distribution than FNV-1a for structured or low-entropy keys.
	PartitionerMurmur3 = "murmur3"
	// PartitionerRange assigns contiguous key ranges by first byte (scan-local).
	PartitionerRange = "range"
)

// HashPartitioner spreads keys evenly across shards by a hash of the key. It is
// the default: load is balanced regardless of key distribution, at the cost of
// range scans spanning all shards.
type HashPartitioner struct{}

func (HashPartitioner) Name() string { return "hash-fnv1a-v1" }

func (HashPartitioner) Shard(key []byte, numShards int) int {
	// FNV-1a over the key, reduced to a shard index.
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for _, c := range key {
		h ^= uint64(c)
		h *= prime
	}
	return int(h % uint64(numShards))
}

// Murmur3Partitioner maps keys to shards on a fixed token ring: it hashes the
// key to a 64-bit token (the low 64 bits of MurmurHash3 x64_128, seed 0), then
// maps that token to one of numShards equal, contiguous ring arcs. Load is
// balanced for any key distribution because the hash spreads tokens uniformly.
// The mapping is a pure function of the shard count, so it is stable across
// reopens; range scans still span all shards.
//
// It does not use virtual tokens (vnodes): tokens are placed evenly and the
// shard count is fixed at open (never rebalanced), so vnodes would add a bounds
// array and a binary search for no measurable distribution gain over the direct
// mapping below.
type Murmur3Partitioner struct{}

func (Murmur3Partitioner) Name() string { return "murmur3-token-ring-v1" }

func (Murmur3Partitioner) Shard(key []byte, numShards int) int {
	// shard = floor(ringPos * numShards / 2^64) via a 128-bit multiply-shift:
	// the high word of (ringPos * numShards) is exactly that quotient, which
	// slices the ring into numShards equal arcs with no bounds table or division.
	hi, _ := bits.Mul64(ringPos(murmur3Token(key)), uint64(numShards))
	return int(hi)
}

// murmur3Token derives the ring token: the low 64 bits of MurmurHash3 x64_128
// with seed 0.
func murmur3Token(key []byte) int64 {
	lo, _ := murmur3x64128(key, 0)
	return int64(lo)
}

// ringPos maps a signed token onto the ring as an unsigned offset from the ring
// origin (the most-negative token). Flipping the sign bit is an order-preserving
// bijection int64 -> uint64, so ring arithmetic stays in uint64 without
// overflow.
func ringPos(token int64) uint64 {
	return uint64(token) ^ (uint64(1) << 63)
}

// murmur3x64128 is the canonical 128-bit MurmurHash3 (x64 variant) returning the
// two 64-bit halves. Implemented inline: MurmurHash3 is not in the standard
// library and no third-party hash dependency is permitted.
func murmur3x64128(data []byte, seed uint64) (h1, h2 uint64) {
	const (
		c1 = 0x87c37b91114253d5
		c2 = 0x4cf5ad432745937f
	)
	h1, h2 = seed, seed
	n := len(data)

	// Body: consume 16-byte blocks as two little-endian uint64s.
	nblocks := n / 16
	for i := 0; i < nblocks; i++ {
		j := i * 16
		k1 := binary.LittleEndian.Uint64(data[j:])
		k2 := binary.LittleEndian.Uint64(data[j+8:])

		k1 *= c1
		k1 = bits.RotateLeft64(k1, 31)
		k1 *= c2
		h1 ^= k1
		h1 = bits.RotateLeft64(h1, 27)
		h1 += h2
		h1 = h1*5 + 0x52dce729

		k2 *= c2
		k2 = bits.RotateLeft64(k2, 33)
		k2 *= c1
		h2 ^= k2
		h2 = bits.RotateLeft64(h2, 31)
		h2 += h1
		h2 = h2*5 + 0x38495ab5
	}

	// Tail: up to 15 trailing bytes.
	var k1, k2 uint64
	tail := data[nblocks*16:]
	switch len(tail) {
	case 15:
		k2 ^= uint64(tail[14]) << 48
		fallthrough
	case 14:
		k2 ^= uint64(tail[13]) << 40
		fallthrough
	case 13:
		k2 ^= uint64(tail[12]) << 32
		fallthrough
	case 12:
		k2 ^= uint64(tail[11]) << 24
		fallthrough
	case 11:
		k2 ^= uint64(tail[10]) << 16
		fallthrough
	case 10:
		k2 ^= uint64(tail[9]) << 8
		fallthrough
	case 9:
		k2 ^= uint64(tail[8])
		k2 *= c2
		k2 = bits.RotateLeft64(k2, 33)
		k2 *= c1
		h2 ^= k2
		fallthrough
	case 8:
		k1 ^= uint64(tail[7]) << 56
		fallthrough
	case 7:
		k1 ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		k1 ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		k1 ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		k1 ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		k1 ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint64(tail[0])
		k1 *= c1
		k1 = bits.RotateLeft64(k1, 31)
		k1 *= c2
		h1 ^= k1
	}

	// Finalization.
	h1 ^= uint64(n)
	h2 ^= uint64(n)
	h1 += h2
	h2 += h1
	h1 = fmix64(h1)
	h2 = fmix64(h2)
	h1 += h2
	h2 += h1
	return h1, h2
}

// fmix64 is MurmurHash3's 64-bit finalization mix.
func fmix64(k uint64) uint64 {
	k ^= k >> 33
	k *= 0xff51afd7ed558ccd
	k ^= k >> 33
	k *= 0xc4ceb9fe1a85ec53
	k ^= k >> 33
	return k
}

// rangePartitioner assigns contiguous key ranges to shards using the first key
// byte, so most range scans stay within one shard. Keys clustering by prefix
// can create hot shards.
type RangePartitioner struct{}

func (RangePartitioner) Name() string { return "range-first-byte-v1" }

func (RangePartitioner) Shard(key []byte, numShards int) int {
	var first int
	if len(key) > 0 {
		first = int(key[0])
	}
	// Map the 0..255 first-byte space onto numShards contiguous buckets.
	idx := first * numShards / 256
	if idx >= numShards {
		idx = numShards - 1
	}
	return idx
}

// builtinPartitioner returns the built-in partitioner for a config name.
func builtinPartitioner(name string) Partitioner {
	switch name {
	case PartitionerRange:
		return RangePartitioner{}
	case PartitionerMurmur3:
		return Murmur3Partitioner{}
	default:
		return HashPartitioner{}
	}
}

// resolvePartitioner returns the effective partitioner: the custom one if set,
// otherwise the named built-in.
func (o *Options) resolvePartitioner() Partitioner {
	if o.CustomPartitioner != nil {
		return o.CustomPartitioner
	}
	return builtinPartitioner(o.Partitioner)
}
