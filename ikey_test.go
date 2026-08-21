package levisdb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIkeyEncodeDecode(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		key  []byte
		seq  uint64
		kind ikeyKind
	}{
		{"set", []byte("foo"), 42, ikeyKindSet},
		{"delete", []byte("bar"), 7, ikeyKindDelete},
		{"empty key", []byte{}, 1, ikeyKindSet},
		{"large seq", []byte("k"), 1<<40 + 3, ikeyKindDelete},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ik := ikeyEncode(nil, tc.key, tc.seq, tc.kind)
			assert.Equal(t, len(tc.key)+trailerLen, len(ik))
			assert.Equal(t, tc.key, ikeyUserKey(ik))

			seq, kind := ikeySeqKind(ik)
			assert.Equal(t, tc.seq, seq)
			assert.Equal(t, tc.kind, kind)
		})
	}
}

func TestIkeyEncodeAppendsToDst(t *testing.T) {
	t.Parallel()
	prefix := []byte("HEAD")
	ik := ikeyEncode(prefix, []byte("k"), 1, ikeyKindSet)
	assert.Equal(t, []byte("HEAD"), ik[:4])
	assert.Equal(t, []byte("k"), ikeyUserKey(ik[4:]))
}

func TestIkeyCompare(t *testing.T) {
	t.Parallel()
	t.Run("user key ascending", func(t *testing.T) {
		a := ikeyEncode(nil, []byte("a"), 1, ikeyKindSet)
		b := ikeyEncode(nil, []byte("b"), 1, ikeyKindSet)
		assert.Negative(t, ikeyCompare(a, b))
		assert.Positive(t, ikeyCompare(b, a))
	})

	t.Run("newer seq sorts first", func(t *testing.T) {
		older := ikeyEncode(nil, []byte("k"), 1, ikeyKindSet)
		newer := ikeyEncode(nil, []byte("k"), 9, ikeyKindSet)
		assert.Negative(t, ikeyCompare(newer, older))
		assert.Positive(t, ikeyCompare(older, newer))
	})

	t.Run("delete before set at equal seq", func(t *testing.T) {
		set := ikeyEncode(nil, []byte("k"), 5, ikeyKindSet)
		del := ikeyEncode(nil, []byte("k"), 5, ikeyKindDelete)
		// Set has higher trailer (kind 1 > 0), so it sorts first.
		assert.Negative(t, ikeyCompare(set, del))
	})

	t.Run("equal", func(t *testing.T) {
		a := ikeyEncode(nil, []byte("k"), 5, ikeyKindSet)
		b := ikeyEncode(nil, []byte("k"), 5, ikeyKindSet)
		assert.Zero(t, ikeyCompare(a, b))
	})
}

func TestIkeyLookupKey(t *testing.T) {
	t.Parallel()
	lk := ikeyLookupKey(nil, []byte("foo"), 100)
	assert.Equal(t, []byte("foo"), ikeyUserKey(lk))

	seq, kind := ikeySeqKind(lk)
	assert.Equal(t, uint64(100), seq)
	assert.Equal(t, ikeyKindSetTTL, kind)

	// Lookup key at seq N sorts at or before the newest stored version <= N.
	stored := ikeyEncode(nil, []byte("foo"), 100, ikeyKindSetTTL)
	require.Zero(t, ikeyCompare(lk, stored))
	plain := ikeyEncode(nil, []byte("foo"), 100, ikeyKindSet)
	assert.Negative(t, ikeyCompare(lk, plain))

	older := ikeyEncode(nil, []byte("foo"), 50, ikeyKindSet)
	assert.Negative(t, ikeyCompare(lk, older))
}

func FuzzIkeyEncodeParse(f *testing.F) {
	f.Add([]byte("foo"), uint64(42), uint8(1))
	f.Add([]byte{}, uint64(0), uint8(0))
	f.Add([]byte{0xff, 0x00}, uint64(1)<<55, uint8(1))

	f.Fuzz(func(t *testing.T, userKey []byte, seq uint64, kindByte uint8) {
		// ikey parsers require a valid (>=8 byte) internal key by contract, so
		// fuzz them only through the encoder. Round-trip must be lossless for
		// any user key and any seq in the representable range.
		seq &= ikeyMaxSeqMask
		kind := ikeyKindSet
		if kindByte&1 == 0 {
			kind = ikeyKindDelete
		}
		ik := ikeyEncode(nil, userKey, seq, kind)

		gotSeq, gotKind := ikeySeqKind(ik)
		assert.Equal(t, seq, gotSeq)
		assert.Equal(t, kind, gotKind)
		assert.True(t, bytes.Equal(userKey, ikeyUserKey(ik)))

		// A key compares equal to itself and encodes deterministically.
		assert.Zero(t, ikeyCompare(ik, ik))
		assert.Equal(t, ik, ikeyEncode(nil, userKey, seq, kind))
	})
}

// ikeyMaxSeqMask bounds a fuzzed seq to the 56 bits the trailer stores.
const ikeyMaxSeqMask = (uint64(1) << 56) - 1

func FuzzIkeyOrdering(f *testing.F) {
	f.Add([]byte("a"), uint64(1), []byte("a"), uint64(2))
	f.Add([]byte("a"), uint64(5), []byte("b"), uint64(1))
	f.Add([]byte(""), uint64(0), []byte(""), uint64(0))

	f.Fuzz(func(t *testing.T, ua []byte, sa uint64, ub []byte, sb uint64) {
		sa &= ikeyMaxSeqMask
		sb &= ikeyMaxSeqMask
		a := ikeyEncode(nil, ua, sa, ikeyKindSet)
		b := ikeyEncode(nil, ub, sb, ikeyKindSet)

		cmp := ikeyCompare(a, b)

		// Antisymmetry: compare(a,b) == -compare(b,a).
		assert.Equal(t, cmp, -ikeyCompare(b, a))
		// Reflexivity.
		assert.Zero(t, ikeyCompare(a, a))

		// Ordering matches (user key ascending, then seq descending).
		byUser := bytes.Compare(ua, ub)
		switch {
		case byUser < 0:
			assert.Negative(t, cmp, "smaller user key must sort first")
		case byUser > 0:
			assert.Positive(t, cmp, "larger user key must sort last")
		default: // same user key: higher seq sorts first
			switch {
			case sa > sb:
				assert.Negative(t, cmp)
			case sa < sb:
				assert.Positive(t, cmp)
			default:
				assert.Zero(t, cmp)
			}
		}
	})
}
