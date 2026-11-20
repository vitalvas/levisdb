package levisdb

import (
	"bytes"
	"fmt"
	"time"
)

// reverseEngineIterator yields the live user key/value pairs of the engine at a
// snapshot sequence in DESCENDING user-key order (newest-first by key). It is
// the reverse twin of engineIterator and shares its visibility rules: for each
// user key it keeps the newest version at or below seq, skips tombstones,
// expired TTL entries, and keys shadowed by a visible range tombstone.
//
// Internal keys sort a user key's newest version first in ASCENDING order, so a
// descending merge delivers each user key's versions oldest-first (ascending
// seq within the key). The deciding version is therefore the LAST visible one
// seen before the user key changes, which forces a one-key-deferred emit: the
// iterator accumulates the newest visible version of the current user key and
// only finalizes it when the merge advances to a smaller user key (or ends).
type reverseEngineIterator struct {
	eng       *engineT
	merge     *mergeIter
	seq       uint64
	readTime  int64
	start     []byte // inclusive lower bound, nil for unbounded
	end       []byte // exclusive upper bound, nil for unbounded
	key       []byte
	value     []byte
	expiresAt int64
	rts       []rangeTombstone
	refs      []*tableMeta

	// Accumulator for the user key currently being resolved.
	accActive bool   // an accumulation is in progress for accKey
	accKey    []byte // user key being accumulated
	accValRaw []byte // raw stored value of the newest visible version so far
	accKind   ikeyKind
	accSeq    uint64
	accHas    bool // a visible version has been recorded for accKey

	err    error
	closed bool
}

func (s *engineT) newReverseRangeIteratorAt(seq uint64, start, end []byte, readTime int64) *reverseEngineIterator {
	g := s.gatherSources(start, end, true)
	it := &reverseEngineIterator{
		eng:      s,
		merge:    newReverseMergeIter(g.sources...),
		seq:      seq,
		readTime: readTime,
		start:    bytes.Clone(start),
		end:      bytes.Clone(end),
		rts:      g.rts,
		refs:     g.refs,
	}
	it.err = g.rtErr
	return it
}

// Next advances to the next live user key in descending order.
func (it *reverseEngineIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}
	for it.merge.Next() {
		ik := it.merge.internalKey()
		user := ikeyUserKey(ik)

		if it.end != nil && bytes.Compare(user, it.end) >= 0 {
			continue // above the half-open upper bound
		}
		if it.start != nil && bytes.Compare(user, it.start) < 0 {
			// Below the inclusive lower bound; keys only decrease from here, so the
			// scan is done once the accumulated key (if any) is emitted. The current
			// below-bound key is dropped (it is outside the range and never resumed).
			if it.finalizeAcc() {
				return true
			}
			_ = it.Close()
			return false
		}

		kseq, kind := ikeySeqKind(ik)
		if !validIKeyKind(kind) {
			it.err = fmt.Errorf("iterator: unknown internal-key kind %d", kind)
			_ = it.Close()
			return false
		}

		// A new (smaller) user key: finalize the one we were accumulating first.
		if it.accActive && !bytes.Equal(user, it.accKey) {
			emit := it.finalizeAcc()
			it.startAcc(user)
			it.recordVersion(kseq, kind)
			if emit {
				return true
			}
			continue
		}
		if !it.accActive {
			it.startAcc(user)
		}
		it.recordVersion(kseq, kind)
	}
	if it.err = it.merge.Error(); it.err != nil {
		_ = it.Close()
		return false
	}
	// Drain the last accumulated key.
	if it.finalizeAcc() {
		return true
	}
	_ = it.Close()
	return false
}

// startAcc begins accumulating a new user key, clearing any prior version state.
func (it *reverseEngineIterator) startAcc(user []byte) {
	it.accActive = true
	it.accKey = append(it.accKey[:0], user...)
	it.accHas = false
	it.accSeq = 0
}

// recordVersion keeps the newest version at or below seq for the current key.
// Versions arrive ascending-seq within a key, so a visible version always
// supersedes the previous record.
func (it *reverseEngineIterator) recordVersion(kseq uint64, kind ikeyKind) {
	if kseq > it.seq {
		return // newer than the snapshot: invisible
	}
	it.accValRaw = append(it.accValRaw[:0], it.merge.Value()...)
	it.accKind = kind
	it.accSeq = kseq
	it.accHas = true
}

// finalizeAcc resolves the accumulated key into it.key/it.value and reports
// whether it produced a live entry to emit. It always clears the accumulator.
func (it *reverseEngineIterator) finalizeAcc() bool {
	if !it.accActive {
		return false
	}
	ok := false
	if it.accHas {
		ok = it.resolveDecided()
	}
	it.accActive = false
	it.accHas = false
	return ok
}

// resolveDecided applies tombstone, range-tombstone, and TTL/blob rules to the
// newest visible version recorded for the accumulated key, setting it.key and
// it.value when the key is live.
func (it *reverseEngineIterator) resolveDecided() bool {
	if it.accKind == ikeyKindDelete {
		return false
	}
	if rangeDeleted(it.rts, it.accKey, it.accSeq, it.seq) {
		return false
	}
	value := it.accValRaw
	var entryExpiry int64
	switch it.accKind {
	case ikeyKindBlobRef:
		ptr, derr := decodeBlobPointer(value)
		if derr != nil {
			it.err = fmt.Errorf("iterator: %w", derr)
			_ = it.Close()
			return false
		}
		if ptr.expiresAt != 0 && ptr.expiresAt <= it.readTime {
			return false
		}
		resolved, rerr := it.eng.resolveBlob(ptr)
		if rerr != nil {
			it.err = fmt.Errorf("iterator: %w", rerr)
			_ = it.Close()
			return false
		}
		value = resolved
		entryExpiry = ptr.expiresAt
	case ikeyKindSetTTL:
		decoded, expiresAt, err := decodeExpiringValue(value)
		if err != nil {
			it.err = fmt.Errorf("iterator: %w", err)
			_ = it.Close()
			return false
		}
		if expiresAt <= it.readTime {
			return false
		}
		value = decoded
		entryExpiry = expiresAt
	}
	it.key = append(it.key[:0], it.accKey...)
	it.value = append(it.value[:0], value...)
	it.expiresAt = entryExpiry
	return true
}

func (it *reverseEngineIterator) Key() []byte { return it.key }

func (it *reverseEngineIterator) Value() []byte { return it.value }

func (it *reverseEngineIterator) valueExpiresAt() int64 { return it.expiresAt }

func (it *reverseEngineIterator) Error() error { return it.err }

func (it *reverseEngineIterator) Close() error {
	if it.closed {
		return it.err
	}
	it.closed = true
	for _, t := range it.refs {
		if err := t.releaseRef(); err != nil && it.err == nil {
			it.err = err
		}
	}
	it.refs = nil
	return it.err
}

// NewReverseIterator returns a descending iterator over the engine at snapshot seq.
func (s *engineT) NewReverseIterator(seq uint64) *reverseEngineIterator {
	return s.NewReverseRangeIterator(seq, nil, nil)
}

// NewReverseRangeIterator returns a descending iterator over [start, end) at
// snapshot seq. A nil bound is unbounded on that side.
func (s *engineT) NewReverseRangeIterator(seq uint64, start, end []byte) *reverseEngineIterator {
	return s.newReverseRangeIteratorAt(seq, start, end, time.Now().Unix())
}
