// The bloom filter is embedded in each SSTable. It
// answers "could this key be in this table?" so a point lookup can skip a table
// with zero disk reads on a definite miss. This matters most under size-tiered
// compaction, where several overlapping tables may hold a key.

package levisdb

import (
	"fmt"
	"math/bits"
)

// bloomFilter builds a serialized bloom filter using double hashing. The bit
// array length is rounded up to a power of two so each probe reduces the hash
// with a mask (h & (bits-1)) instead of a modulo, keeping the build and query
// hot loops division-free. The serialized form appends the probe count k as a
// trailing byte so a reader knows how many probes to run.
type bloomFilter struct {
	bitsPerKey int
}

// newBloom returns a bloom builder with the given bits per key.
func newBloom(bitsPerKey int) *bloomFilter {
	if bitsPerKey < 1 {
		bitsPerKey = 1
	}
	return &bloomFilter{bitsPerKey: bitsPerKey}
}

// probes returns the optimal number of hash probes k = round(bits/key * ln2),
// clamped to a sane range.
func (b *bloomFilter) probes() int {
	k := int(float64(b.bitsPerKey)*0.69 + 0.5) // ln2 ~= 0.69
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return k
}

// pow2Bytes returns the smallest power-of-two byte count that holds at least
// minBits bits, with a floor of 8 bytes (64 bits).
func pow2Bytes(minBits int) int {
	nbytes := (minBits + 7) / 8
	if nbytes < 8 {
		nbytes = 8
	}
	// Round up to the next power of two so the bit count is a power of two too.
	if nbytes&(nbytes-1) != 0 {
		nbytes = 1 << bits.Len(uint(nbytes))
	}
	return nbytes
}

// build returns a serialized filter covering the given 32-bit key hashes.
func (b *bloomFilter) build(hashes []uint32) []byte {
	k := b.probes()

	nbytes := pow2Bytes(len(hashes) * b.bitsPerKey)
	nbits := uint32(nbytes * 8)
	mask := nbits - 1 // nbits is a power of two, so this is a valid modulo mask

	out := make([]byte, nbytes+1)
	out[nbytes] = byte(k)

	for _, h := range hashes {
		delta := bits.RotateLeft32(h, 15) // second hash for double hashing
		for i := 0; i < k; i++ {
			bit := h & mask
			out[bit>>3] |= 1 << (bit & 7)
			h += delta
		}
	}
	return out
}

// validateBloomFilter checks the private on-disk encoding produced by build.
// A malformed filter must fail table open: treating a truncated filter as a
// definite miss could silently hide keys that are present in the data blocks.
func validateBloomFilter(filter []byte) error {
	if len(filter) < 9 { // minimum 8-byte bit array plus the probe-count trailer
		return fmt.Errorf("table: bloom filter too short")
	}
	nbytes := len(filter) - 1
	if nbytes&(nbytes-1) != 0 {
		return fmt.Errorf("table: bloom filter size is not a power of two")
	}
	k := filter[len(filter)-1]
	if k < 1 || k > 30 {
		return fmt.Errorf("table: invalid bloom probe count %d", k)
	}
	return nil
}

// bloomMayContain reports whether hash might be present according to filter. A
// false result is definitive; a true result may be a false positive.
func bloomMayContain(filter []byte, hash uint32) bool {
	if len(filter) < 2 {
		return false
	}
	k := int(filter[len(filter)-1])
	if k > 30 {
		// Reserved encoding; treat as always-maybe to stay correct.
		return true
	}
	nbytes := len(filter) - 1
	nbits := uint32(nbytes * 8)
	// A filter written by build always has a power-of-two bit count, so masking
	// is exact. Fall back to modulo for any externally-produced filter whose
	// length is not a power of two (never happens for our tables, but keeps the
	// function correct on arbitrary input).
	if nbytes&(nbytes-1) == 0 {
		mask := nbits - 1
		h := hash
		delta := bits.RotateLeft32(h, 15)
		for i := 0; i < k; i++ {
			bit := h & mask
			if filter[bit>>3]&(1<<(bit&7)) == 0 {
				return false
			}
			h += delta
		}
		return true
	}
	h := hash
	delta := bits.RotateLeft32(h, 15)
	for i := 0; i < k; i++ {
		bit := h % nbits
		if filter[bit>>3]&(1<<(bit&7)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

// bloomHash is the 32-bit hash used for bloom keys. It mixes each byte with a
// multiply-xor step (an FNV-1a variant with an extra rotate for avalanche) and
// is allocation-free for a []byte key.
func bloomHash(key []byte) uint32 {
	const (
		offset = 2166136261
		prime  = 16777619
	)
	h := uint32(offset)
	for _, c := range key {
		h ^= uint32(c)
		h *= prime
	}
	// A final avalanche step spreads low-entropy keys across the bit array,
	// which matters now that positions are taken by masking the low bits.
	h ^= h >> 15
	h *= 0x2c1b3c6d
	h ^= h >> 12
	return h
}
