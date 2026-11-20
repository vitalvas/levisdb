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
	eng       *engineT // for resolving out-of-line (blob) values; nil-safe for inline-only
	merge     *mergeIter
	seq       uint64
	readTime  int64
	start     []byte // inclusive lower bound, nil for unbounded
	end       []byte // exclusive upper bound, nil for unbounded
	key       []byte
	value     []byte
	expiresAt int64 // absolute deadline (Unix seconds) of the current TTL entry, 0 otherwise
	lastKey   []byte
	primed    bool
	refs      []*tableMeta
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
	return s.newRangeIteratorAt(seq, start, end, time.Now().Unix())
}

// iteratorSources is the set of merge inputs an iterator is built from, gathered
// once under the engine lock so the forward and reverse constructors share the
// source-collection logic.
type iteratorSources struct {
	sources []entrySource
	refs    []*tableMeta
	rts     []rangeTombstone
	rtErr   error
}

// gatherSources collects the memtable, immutable, recovery, and overlapping-table
// iterators for a scan over [start, end). descending selects the reverse source
// of each so the merge receives descending internal keys. It acquires a ref on
// each table it includes (released via the returned refs).
func (s *engineT) gatherSources(start, end []byte, descending bool) iteratorSources {
	s.mu.RLock()
	defer s.mu.RUnlock()
	g := iteratorSources{
		sources: make([]entrySource, 0, len(s.tables)+len(s.recoveryMems)+2),
		refs:    make([]*tableMeta, 0, len(s.tables)),
	}
	memSource := func(m *memtableT) entrySource {
		if descending {
			return m.newReverseSnapshotIterator()
		}
		return m.newSnapshotIterator()
	}
	g.sources = append(g.sources, memSource(s.mem))
	g.rts = append(g.rts, s.mem.rangeTombstones()...)
	if s.imm != nil {
		g.sources = append(g.sources, memSource(s.imm))
		g.rts = append(g.rts, s.imm.rangeTombstones()...)
	}
	for _, recovered := range s.recoveryMems {
		g.sources = append(g.sources, memSource(recovered))
		g.rts = append(g.rts, recovered.rangeTombstones()...)
	}
	for _, t := range s.tables {
		// Skip a table whose key range does not intersect [start, end); end is
		// treated inclusively here, which is conservative (never wrongly skipped).
		if !t.overlapsRange(start, end) {
			continue
		}
		if !t.acquire() {
			continue
		}
		g.refs = append(g.refs, t)
		if descending {
			g.sources = append(g.sources, t.reader.newReverseIterator())
		} else {
			g.sources = append(g.sources, t.reader.newIterator())
		}
		if trts, err := t.reader.rangeTombstones(); err != nil {
			g.rtErr = err
		} else {
			g.rts = append(g.rts, trts...)
		}
	}
	return g
}

func (s *engineT) newRangeIteratorAt(seq uint64, start, end []byte, readTime int64) *engineIterator {
	g := s.gatherSources(start, end, false)
	// bytes.Clone preserves the nil/non-nil distinction: a non-nil empty end
	// bound stays non-nil (an exclusive upper bound of "" matching nothing),
	// whereas append([]byte(nil), end...) would collapse it to nil (unbounded).
	it := &engineIterator{
		eng:      s,
		merge:    newMergeIter(g.sources...),
		seq:      seq,
		readTime: readTime,
		start:    bytes.Clone(start),
		end:      bytes.Clone(end),
		refs:     g.refs,
		rts:      g.rts,
	}
	// Surface a range-del read error on the first Next rather than silently
	// dropping tombstones (which could reveal a deleted key).
	it.err = g.rtErr
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
		var entryExpiry int64
		switch kind {
		case ikeyKindBlobRef:
			ptr, derr := decodeBlobPointer(value)
			if derr != nil {
				it.err = fmt.Errorf("iterator: %w", derr)
				_ = it.Close()
				return false
			}
			if ptr.expiresAt != 0 && ptr.expiresAt <= it.readTime {
				continue
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
			entryExpiry = expiresAt
		}
		// Return the merge iterator's buffers directly rather than copying: they are
		// stable until this iterator advances again (it.merge.Next in the loop
		// above), which is exactly the "valid until the next Next" contract Key and
		// Value promise. The DBIterator wraps this and copies out before it
		// advances, so the public value the caller sees is still stable.
		it.key = user
		it.value = value
		it.expiresAt = entryExpiry
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

// valueExpiresAt returns the absolute expiration (Unix seconds) of the current
// entry, or 0 when it has no TTL. Valid only until the next call to Next.
func (it *engineIterator) valueExpiresAt() int64 { return it.expiresAt }

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
