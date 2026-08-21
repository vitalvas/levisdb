// Internal keys are shared by the memtable and
// SSTables. An internal key is the user key followed by an 8-byte trailer
// packing the sequence number and value kind:
//
//	userkey | (seq << 8) | kind   (trailer little-endian, 8 bytes)
//
// Internal keys sort by user key ascending, then by sequence number
// descending, so the newest version of a key sorts first.

package levisdb

import (
	"bytes"
	"encoding/binary"
)

// Kind is the value kind stored in an internal key trailer.
type ikeyKind uint8

const (
	// KindDelete marks a tombstone. WAL sequence numbers are unique, so valid
	// database entries never need a kind tie-break at the same sequence.
	ikeyKindDelete ikeyKind = iota
	// KindSet marks a live value.
	ikeyKindSet
	// ikeyKindSetTTL marks a live value whose stored bytes begin with an
	// absolute expiration timestamp.
	ikeyKindSetTTL
)

func validIKeyKind(kind ikeyKind) bool {
	return kind == ikeyKindDelete || kind == ikeyKindSet || kind == ikeyKindSetTTL
}

// trailerLen is the size of the packed seq+kind trailer.
const trailerLen = 8

// maxIKeySeq is the largest sequence representable in the 56 high bits of an
// internal-key trailer.
const maxIKeySeq = uint64(1<<56) - 1

// Encode appends to dst the internal key for userKey at seq with kind. It
// reserves the exact final length up front so an empty dst produces a single
// allocation rather than growing twice.
func ikeyEncode(dst, userKey []byte, seq uint64, kind ikeyKind) []byte {
	need := len(userKey) + trailerLen
	if cap(dst)-len(dst) < need {
		grown := make([]byte, len(dst), len(dst)+need)
		copy(grown, dst)
		dst = grown
	}
	dst = append(dst, userKey...)
	var t [trailerLen]byte
	binary.LittleEndian.PutUint64(t[:], (seq<<8)|uint64(kind))
	return append(dst, t[:]...)
}

// UserKey returns the user-key portion of an internal key.
func ikeyUserKey(ik []byte) []byte {
	return ik[:len(ik)-trailerLen]
}

// SeqKind returns the sequence number and kind packed in an internal key.
func ikeySeqKind(ik []byte) (uint64, ikeyKind) {
	t := binary.LittleEndian.Uint64(ik[len(ik)-trailerLen:])
	return t >> 8, ikeyKind(t & 0xff)
}

// Compare orders valid internal keys: user key ascending, then trailer
// descending (higher seq first). Callers must provide keys with an 8-byte
// trailer; on-disk readers validate this before comparing corrupt input.
func ikeyCompare(a, b []byte) int {
	ua, ub := ikeyUserKey(a), ikeyUserKey(b)
	if c := bytes.Compare(ua, ub); c != 0 {
		return c
	}
	ta := binary.LittleEndian.Uint64(a[len(a)-trailerLen:])
	tb := binary.LittleEndian.Uint64(b[len(b)-trailerLen:])
	// Higher trailer (newer seq) sorts first.
	switch {
	case ta > tb:
		return -1
	case ta < tb:
		return 1
	default:
		return 0
	}
}

// LookupKey builds an internal key for seeking: userKey at seq with the maximum
// persisted kind, so it lands at or before the newest version at or under seq.
func ikeyLookupKey(dst, userKey []byte, seq uint64) []byte {
	return ikeyEncode(dst, userKey, seq, ikeyKindSetTTL)
}
