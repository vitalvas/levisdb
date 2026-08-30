// A memtable is an in-memory, sorted write buffer backed by a skiplist.
// Entries are keyed by internal key (user key + seq + kind) so a lookup returns
// the newest version of a user key, and tombstones shadow older values. When
// the tracked size crosses a threshold the memtable is flushed to an SSTable.

package levisdb

import (
	"bytes"
	"sync"
	"time"
)

// Memtable is a sorted in-memory buffer. A RWMutex guards the skiplist so many
// readers may run concurrently with a single writer.
//
// ponytail: RWMutex over the skiplist; swap for an atomic lock-free skiplist
// only if benchmarks show the lock is the write-path bottleneck.
type memtableT struct {
	mu   sync.RWMutex
	list *skiplist
	size int64
}

// New returns an empty memtable. seed varies the skiplist RNG per memtable.
func newMemtable(seed int64) *memtableT {
	return &memtableT{list: newSkiplist(seed)}
}

// Size returns the approximate encoded size of buffered entries in bytes.
func (m *memtableT) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// Empty reports whether the memtable holds no entries.
func (m *memtableT) empty() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.list.first() == nilNode
}

// overlapsUserRange reports whether any buffered key's user portion falls within
// [minKey, maxKey] inclusive. Used by SST ingest to reject a file that overlaps
// unflushed writes. It seeks to the first internal key at or after minKey (using
// the highest possible trailer so any version of minKey qualifies) and checks its
// user key against maxKey.
func (m *memtableT) overlapsUserRange(minKey, maxKey []byte) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	seek := ikeyEncode(nil, minKey, maxIKeySeq, ikeyKindSetTTL)
	off := m.list.seek(seek)
	if off == nilNode {
		return false
	}
	return bytes.Compare(ikeyUserKey(m.list.key(off)), maxKey) <= 0
}

// Put buffers a set of key to value at seq.
func (m *memtableT) Put(seq uint64, key, value []byte) {
	m.add(seq, ikeyKindSet, key, value)
}

// putTTL buffers a set with an absolute Unix-nanosecond expiration.
func (m *memtableT) putTTL(seq uint64, key, value []byte, expiresAt int64) {
	m.add(seq, ikeyKindSetTTL, key, encodeExpiringValue(value, expiresAt))
}

// Delete buffers a tombstone for key at seq.
func (m *memtableT) del(seq uint64, key []byte) {
	m.add(seq, ikeyKindDelete, key, nil)
}

func (m *memtableT) add(seq uint64, kind ikeyKind, key, value []byte) {
	ik := ikeyEncode(nil, key, seq, kind)
	m.mu.Lock()
	m.list.insert(ik, value)
	m.size += int64(len(ik) + len(value))
	m.mu.Unlock()
}

// Get returns the value for key considering only versions at or below seq. The
// found bool reports whether any version was seen; deleted reports whether the
// newest such version is a tombstone (in which case value is nil).
func (m *memtableT) get(seq uint64, key []byte) (value []byte, found, deleted bool) {
	value, _, found, deleted = m.getVersionAt(seq, key, time.Now().UnixNano())
	return value, found, deleted
}

func (m *memtableT) getVersionAt(seq uint64, key []byte, now int64) (value []byte, versionSeq uint64, found, deleted bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Stack buffer for the lookup key: it is only used to seek and never
	// retained, so it need not escape to the heap for typical key lengths.
	var buf [64]byte
	lookup := ikeyLookupKey(buf[:0], key, seq)
	n := m.list.seek(lookup)
	if n == nilNode {
		return nil, 0, false, false
	}
	nkey := m.list.key(n)
	if !bytes.Equal(ikeyUserKey(nkey), key) {
		return nil, 0, false, false
	}
	// n is the newest version with seq <= the lookup seq, since the skiplist is
	// ordered newest-first within a user key and LookupKey uses the query seq.
	nseq, kind := ikeySeqKind(nkey)
	if nseq > seq {
		// All versions of this key are newer than the snapshot seq.
		return nil, 0, false, false
	}
	if kind == ikeyKindDelete {
		return nil, nseq, true, true
	}
	if kind == ikeyKindSetTTL {
		value, expiresAt, err := decodeExpiringValue(m.list.value(n))
		if err != nil || expiresAt <= now {
			return nil, nseq, true, true
		}
		return value, nseq, true, false
	}
	return m.list.value(n), nseq, true, false
}

// Iterator iterates internal entries in sorted order (user key ascending, seq
// descending). It exposes the raw internal key so the flush path can write
// every version and tombstone into the SSTable. It is intended for the flush
// path, which runs after the memtable is sealed and no longer written, so it
// does not lock.
type memtableIterator struct {
	list    *skiplist
	n       uint32 // current node offset; nilNode before start / at end
	started bool
}

// NewIterator returns an iterator over the memtable, positioned before the
// first entry. Call Next to advance to the first entry.
func (m *memtableT) newIterator() *memtableIterator {
	return &memtableIterator{list: m.list}
}

// Next advances to the next entry and reports whether one is available.
func (it *memtableIterator) Next() bool {
	if !it.started {
		it.started = true
		it.n = it.list.first()
	} else if it.n != nilNode {
		it.n = it.list.next(it.n, 0)
	}
	return it.n != nilNode
}

// InternalKey returns the current internal key.
func (it *memtableIterator) internalKey() []byte { return it.list.key(it.n) }

// Value returns the current value.
func (it *memtableIterator) Value() []byte { return it.list.value(it.n) }

func (it *memtableIterator) Error() error { return nil }

type memtableSnapshotIterator struct {
	arena []byte   // arena backing array pinned at snapshot time
	offs  []uint32 // node offsets present at snapshot, in ascending key order
	index int
}

// newSnapshotIterator captures a stable view of a (possibly still mutable)
// memtable without copying key/value bytes. It records the node offsets present
// now and pins the arena's backing array; the scan then runs lock-free while
// writers keep appending. This is safe because: node data bytes are immutable
// once written (a re-Put appends a new node, never overwrites); the arena only
// grows by append, so a growth reallocates into a NEW array and leaves the pinned
// one stable; and offsets are captured under the lock, so a concurrent write's
// node (offset >= boundary) is excluded. Reading links lock-free would race with
// setNext, so only the offset walk is done under the lock; afterward the iterator
// touches immutable data via the pinned arena only.
func (m *memtableT) newSnapshotIterator() *memtableSnapshotIterator {
	m.mu.RLock()
	defer m.mu.RUnlock()
	pinned := m.list.arena
	boundary := uint32(len(pinned))
	var offs []uint32
	for n := m.list.first(); n != nilNode; n = m.list.next(n, 0) {
		if n >= boundary {
			break // a node appended after the snapshot boundary; exclude it
		}
		offs = append(offs, n)
	}
	return &memtableSnapshotIterator{arena: pinned, offs: offs, index: -1}
}

func (it *memtableSnapshotIterator) Next() bool {
	it.index++
	return it.index < len(it.offs)
}

func (it *memtableSnapshotIterator) internalKey() []byte {
	return skiplistKeyAt(it.arena, it.offs[it.index])
}

func (it *memtableSnapshotIterator) Value() []byte {
	return skiplistValueAt(it.arena, it.offs[it.index])
}
func (it *memtableSnapshotIterator) Error() error { return nil }
