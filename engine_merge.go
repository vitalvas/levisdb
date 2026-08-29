package levisdb

import (
	"container/heap"
)

// entrySource yields internal-key/value pairs in ascending internal-key order.
type entrySource interface {
	Next() bool
	internalKey() []byte
	Value() []byte
	Error() error
}

// mergeIter is a k-way merge over several entrySources, emitting entries in
// ascending internal-key order. Because internal keys sort a user key's newest
// version first, a consumer that keeps the first entry per user key sees the
// newest write, which is how compaction collapses versions.
type mergeIter struct {
	h      mergeHeap
	curKey []byte
	curVal []byte
	err    error
}

type mergeSource struct {
	src entrySource
	key []byte
	val []byte
}

// newMergeIter builds a merger over the given sources, priming each.
func newMergeIter(sources ...entrySource) *mergeIter {
	m := &mergeIter{}
	for _, s := range sources {
		ms := &mergeSource{src: s}
		if s.Next() {
			ms.key = s.internalKey()
			ms.val = s.Value()
			m.h = append(m.h, ms)
		} else if err := s.Error(); err != nil && m.err == nil {
			m.err = err
		}
	}
	heap.Init(&m.h)
	return m
}

// Next advances to the next smallest entry and reports whether one exists.
func (m *mergeIter) Next() bool {
	if m.err != nil || len(m.h) == 0 {
		return false
	}
	// Copy the current entry into our own buffers before advancing. The source's
	// key/value slices point into its block payload, which a source may reuse
	// across a block boundary when it advances (table iterators decompress into a
	// reused scratch buffer), so holding the raw slices across top.src.Next would
	// read overwritten bytes.
	top := m.h[0]
	m.curKey = append(m.curKey[:0], top.key...)
	m.curVal = append(m.curVal[:0], top.val...)

	// Advance the source and update its heap node in place rather than
	// allocating a new mergeSource on every step; a compaction calls Next
	// millions of times.
	if top.src.Next() {
		top.key = top.src.internalKey()
		top.val = top.src.Value()
		heap.Fix(&m.h, 0)
	} else {
		if err := top.src.Error(); err != nil && m.err == nil {
			m.err = err
		}
		heap.Pop(&m.h)
	}
	return true
}

func (m *mergeIter) internalKey() []byte { return m.curKey }
func (m *mergeIter) Value() []byte       { return m.curVal }
func (m *mergeIter) Error() error        { return m.err }

// mergeHeap orders sources by internal key ascending.
type mergeHeap []*mergeSource

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return ikeyCompare(h[i].key, h[j].key) < 0 }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)        { *h = append(*h, x.(*mergeSource)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
