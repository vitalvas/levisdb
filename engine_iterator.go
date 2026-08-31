package levisdb

import (
	"bytes"
	"fmt"
	"time"
)

// engineIterator yields the live user key/value pairs of the engine at a
// snapshot sequence, in ascending user-key order. It merges the memtable, the
// flushing memtable, and all tables, keeps the newest version at or below seq
// for each user key, and skips tombstones.
type engineIterator struct {
	merge    *mergeIter
	seq      uint64
	readTime int64
	start    []byte // inclusive lower bound, nil for unbounded
	end      []byte // exclusive upper bound, nil for unbounded
	key      []byte
	value    []byte
	lastKey  []byte
	primed   bool
	refs     []*tableMeta
	// rts are the range tombstones from every source, captured at creation so the
	// scan is snapshot-consistent. A key's deciding version is skipped when a
	// visible range tombstone newer than it covers the key.
	rts    []rangeTombstone
	err    error
	closed bool
}

// NewIterator returns an iterator over the engine at snapshot seq.
func (s *engineT) NewIterator(seq uint64) *engineIterator {
	return s.NewRangeIterator(seq, nil, nil)
}

// NewRangeIterator returns an iterator over [start, end) at snapshot seq. A nil
// bound is unbounded on that side.
func (s *engineT) NewRangeIterator(seq uint64, start, end []byte) *engineIterator {
	return s.newRangeIteratorAt(seq, start, end, time.Now().UnixNano())
}

func (s *engineT) newRangeIteratorAt(seq uint64, start, end []byte, readTime int64) *engineIterator {
	s.mu.RLock()
	sources := make([]entrySource, 0, len(s.tables)+len(s.recoveryMems)+2)
	refs := make([]*tableMeta, 0, len(s.tables))
	var rts []rangeTombstone
	sources = append(sources, s.mem.newSnapshotIterator())
	rts = append(rts, s.mem.rangeTombstones()...)
	if s.imm != nil {
		sources = append(sources, s.imm.newIterator())
		rts = append(rts, s.imm.rangeTombstones()...)
	}
	for _, recovered := range s.recoveryMems {
		sources = append(sources, recovered.newIterator())
		rts = append(rts, recovered.rangeTombstones()...)
	}
	var rtErr error
	for _, t := range s.tables {
		// Skip a table whose key range does not intersect [start, end); end is
		// treated inclusively here, which is conservative (never wrongly skipped).
		if !t.overlapsRange(start, end) {
			continue
		}
		if t.acquire() {
			refs = append(refs, t)
			sources = append(sources, t.reader.newIterator())
			if trts, err := t.reader.rangeTombstones(); err != nil {
				rtErr = err
			} else {
				rts = append(rts, trts...)
			}
		}
	}
	s.mu.RUnlock()
	// bytes.Clone preserves the nil/non-nil distinction: a non-nil empty end
	// bound stays non-nil (an exclusive upper bound of "" matching nothing),
	// whereas append([]byte(nil), end...) would collapse it to nil (unbounded).
	it := &engineIterator{
		merge:    newMergeIter(sources...),
		seq:      seq,
		readTime: readTime,
		start:    bytes.Clone(start),
		end:      bytes.Clone(end),
		refs:     refs,
		rts:      rts,
	}
	// Surface a range-del read error on the first Next rather than silently
	// dropping tombstones (which could reveal a deleted key).
	it.err = rtErr
	return it
}

// Next advances to the next live user key and reports whether one exists.
func (it *engineIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}
	for it.merge.Next() {
		ik := it.merge.internalKey()
		user := ikeyUserKey(ik)

		// Range bounds on the user key.
		if it.start != nil && bytes.Compare(user, it.start) < 0 {
			continue
		}
		if it.end != nil && bytes.Compare(user, it.end) >= 0 {
			_ = it.Close()
			return false // past the upper bound; keys only increase from here
		}

		if it.primed && bytes.Equal(user, it.lastKey) {
			continue // an older version of a key already decided
		}

		kseq, kind := ikeySeqKind(ik)
		if !validIKeyKind(kind) {
			it.err = fmt.Errorf("iterator: unknown internal-key kind %d", kind)
			_ = it.Close()
			return false
		}
		if kseq > it.seq {
			// Version newer than the snapshot: skip without marking the key
			// decided, so an older visible version can still surface.
			continue
		}

		// This is the newest visible version of user; it decides the key.
		it.primed = true
		it.lastKey = append(it.lastKey[:0], user...)

		if kind == ikeyKindDelete {
			continue // deleted at this snapshot; move on
		}
		// A range tombstone visible at the snapshot and newer than this version
		// deletes the key, even though the point version is a live value.
		if rangeDeleted(it.rts, user, kseq, it.seq) {
			continue
		}
		value := it.merge.Value()
		if kind == ikeyKindSetTTL {
			var expiresAt int64
			var err error
			value, expiresAt, err = decodeExpiringValue(value)
			if err != nil {
				it.err = fmt.Errorf("iterator: %w", err)
				_ = it.Close()
				return false
			}
			if expiresAt <= it.readTime {
				continue
			}
		}
		// Return the merge iterator's buffers directly rather than copying: they are
		// stable until this iterator advances again (it.merge.Next in the loop
		// above), which is exactly the "valid until the next Next" contract Key and
		// Value promise. The dbIterator wraps this and copies out before it
		// advances, so the public value the caller sees is still stable.
		it.key = user
		it.value = value
		return true
	}
	it.err = it.merge.Error()
	_ = it.Close()
	return false
}

// Key returns the current user key. The slice is valid only until the next call
// to Next; copy it to retain.
func (it *engineIterator) Key() []byte { return it.key }

// Value returns the current value. The slice is valid only until the next call
// to Next; copy it to retain.
func (it *engineIterator) Value() []byte { return it.value }

func (it *engineIterator) Error() error { return it.err }

func (it *engineIterator) Close() error {
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

// *memtableIterator satisfies entrySource directly (Next/internalKey/Value), so
// no adapter is needed to merge a memtable alongside table iterators.
