package levisdb

import (
	"bytes"
	"container/heap"
	"time"
)

// dbIterator merges per-shard iterators into a single ascending user-key
// stream. TTL is evaluated at iterator creation, so a long scan is internally
// consistent even if a value's deadline passes while it runs. Each shard
// already yields deduplicated, live entries at the snapshot,
// and shards own disjoint key spaces under both built-in partitioners, so a
// simple key-ordered merge across shards is sufficient.
type dbIterator struct {
	h           iterHeap
	curK        []byte
	curV        []byte
	err         error
	snaps       *snapshots
	seq         uint64
	readTime    int64
	pinReleased bool
}

func (db *DB) newRangeIterator(seq uint64, start, end []byte) (Iterator, error) {
	readTime := time.Now().UnixNano()
	db.snaps.acquireIteratorTime(readTime)
	it := &dbIterator{snaps: db.snaps, seq: seq, readTime: readTime}
	for _, s := range db.shards {
		si := s.newRangeIteratorAt(seq, start, end, readTime)
		if si.Next() {
			it.h = append(it.h, si)
		} else if err := si.Error(); err != nil {
			_ = it.Close()
			return nil, err
		} else {
			_ = si.Close()
		}
	}
	heap.Init(&it.h)
	return it, nil
}

// Next advances to the next key and reports whether one exists.
func (it *dbIterator) Next() bool {
	if it.err != nil || len(it.h) == 0 {
		it.releasePin()
		return false
	}
	top := it.h[0]
	it.curK = append(it.curK[:0], top.Key()...)
	it.curV = append(it.curV[:0], top.Value()...)
	if top.Next() {
		heap.Fix(&it.h, 0)
	} else {
		heap.Pop(&it.h)
		if err := top.Error(); err != nil {
			it.err = err
			_ = it.Close()
		}
	}
	return true
}

func (it *dbIterator) Key() []byte   { return it.curK }
func (it *dbIterator) Value() []byte { return it.curV }
func (it *dbIterator) Error() error  { return it.err }
func (it *dbIterator) Close() error {
	for len(it.h) > 0 {
		si := heap.Pop(&it.h).(*shardIterator)
		if err := si.Close(); err != nil && it.err == nil {
			it.err = err
		}
	}
	it.releasePin()
	return it.err
}

func (it *dbIterator) releasePin() {
	if !it.pinReleased {
		it.pinReleased = true
		it.snaps.release(it.seq)
		it.snaps.releaseIteratorTime(it.readTime)
	}
}

// iterHeap orders shard iterators by their current key ascending.
type iterHeap []*shardIterator

func (h iterHeap) Len() int           { return len(h) }
func (h iterHeap) Less(i, j int) bool { return bytes.Compare(h[i].Key(), h[j].Key()) < 0 }
func (h iterHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *iterHeap) Push(x any)        { *h = append(*h, x.(*shardIterator)) }
func (h *iterHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
