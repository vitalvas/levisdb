// Range tombstones delete every key in a half-open user-key range [start, end)
// as of a sequence number, so a bulk or prefix delete costs one record instead
// of one tombstone per key. A range tombstone at sequence S shadows a point key
// K when start <= K < end and the point version's sequence is below S, subject to
// the reader's snapshot visibility (only tombstones with seq <= the read sequence
// apply). They are stored in the memtable, persisted in a per-table meta-block,
// carried through compaction, and dropped once they reach the bottom tier where
// no older version can survive to be un-shadowed.

package levisdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// rangeTombstone deletes keys in [start, end) as of seq. start and end are user
// keys; end is exclusive and must be strictly greater than start.
type rangeTombstone struct {
	start []byte
	end   []byte
	seq   uint64
}

// covers reports whether this tombstone deletes userKey at a read taken at
// readSeq for a point version at versionSeq: the key must fall in [start, end),
// the tombstone must be visible (seq <= readSeq), and it must be newer than the
// point version (seq > versionSeq).
func (rt rangeTombstone) covers(userKey []byte, versionSeq, readSeq uint64) bool {
	return rt.seq <= readSeq && rt.seq > versionSeq && rt.contains(userKey)
}

// contains reports whether userKey falls in the half-open range [start, end).
func (rt rangeTombstone) contains(userKey []byte) bool {
	return bytes.Compare(userKey, rt.start) >= 0 && bytes.Compare(userKey, rt.end) < 0
}

// rangeDeleted reports whether any tombstone in rts deletes userKey for a point
// version at versionSeq read at readSeq. rts need not be sorted.
func rangeDeleted(rts []rangeTombstone, userKey []byte, versionSeq, readSeq uint64) bool {
	for i := range rts {
		if rts[i].covers(userKey, versionSeq, readSeq) {
			return true
		}
	}
	return false
}

// maxCoveringRangeDelSeqLE returns the highest sequence of any tombstone in rts
// that contains userKey and has seq <= limit, or 0 if none. Compaction uses it to
// decide whether a range tombstone that no snapshot needs (limit = retainSeq)
// deletes a point version.
func maxCoveringRangeDelSeqLE(rts []rangeTombstone, userKey []byte, limit uint64) uint64 {
	var best uint64
	for i := range rts {
		if rts[i].seq <= limit && rts[i].seq > best && rts[i].contains(userKey) {
			best = rts[i].seq
		}
	}
	return best
}

// encodeRangeDelBlock serializes range tombstones into a table meta-block:
// [count(uvarint)] then per tombstone [startLen][start][endLen][end][seq(uvarint)].
// The block is stored uncompressed (like the filter block) for direct decode.
func encodeRangeDelBlock(dst []byte, rts []rangeTombstone) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(rts)))
	for i := range rts {
		dst = binary.AppendUvarint(dst, uint64(len(rts[i].start)))
		dst = append(dst, rts[i].start...)
		dst = binary.AppendUvarint(dst, uint64(len(rts[i].end)))
		dst = append(dst, rts[i].end...)
		dst = binary.AppendUvarint(dst, rts[i].seq)
	}
	return dst
}

// decodeRangeDelBlock parses a range-del meta-block. The returned tombstones
// alias the payload buffer, so the caller must copy them to retain past its life.
func decodeRangeDelBlock(payload []byte) ([]rangeTombstone, error) {
	count, n := binary.Uvarint(payload)
	if n <= 0 {
		return nil, fmt.Errorf("table: bad range-del count")
	}
	rest := payload[n:]
	if count > uint64(len(rest)) {
		return nil, fmt.Errorf("table: range-del count %d exceeds block size", count)
	}
	out := make([]rangeTombstone, 0, count)
	for i := uint64(0); i < count; i++ {
		start, r, err := readBytes(rest)
		if err != nil {
			return nil, fmt.Errorf("table: range-del start: %w", err)
		}
		end, r2, err := readBytes(r)
		if err != nil {
			return nil, fmt.Errorf("table: range-del end: %w", err)
		}
		seq, sn := binary.Uvarint(r2)
		if sn <= 0 {
			return nil, fmt.Errorf("table: bad range-del seq")
		}
		rest = r2[sn:]
		if bytes.Compare(start, end) >= 0 {
			return nil, fmt.Errorf("table: range-del start >= end")
		}
		out = append(out, rangeTombstone{start: start, end: end, seq: seq})
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("table: range-del trailing bytes")
	}
	return out, nil
}
