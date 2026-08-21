package levisdb

import "sync/atomic"

// Allocator is a monotonic uint32 file-number source shared across all shards,
// so every table, WAL segment, and manifest gets a globally-unique number.
//
// The uint32 namespace is exhausted after ~4.3B lifetime files; allocation
// then stops rather than wrapping. Widen to uint64 before approaching it.
type allocatorT struct {
	n atomic.Uint32
}

// NewAllocator returns an allocator whose first Next returns start+1.
func newAllocator(start uint32) *allocatorT {
	a := &allocatorT{}
	a.n.Store(start)
	return a
}

// Next returns the next file number, or zero once the uint32 namespace is
// exhausted. Zero is reserved as the exhaustion sentinel and is never a valid
// allocated file number.
func (a *allocatorT) Next() uint32 {
	for {
		current := a.n.Load()
		if current == ^uint32(0) {
			return 0
		}
		if a.n.CompareAndSwap(current, current+1) {
			return current + 1
		}
	}
}
