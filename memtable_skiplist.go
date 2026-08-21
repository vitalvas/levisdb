package levisdb

import (
	"encoding/binary"
	"math/rand"
)

const (
	maxHeight = 12
	branching = 4
)

// The skiplist stores every node, its key, and its value contiguously in a
// single growing byte arena, addressing them by uint32 offset instead of Go
// pointers. This collapses the three heap objects and three slice headers a
// pointer-based node would need into one packed region, cutting per-entry
// memory overhead and near-eliminating per-node GC objects and pointer scanning
// for a memtable that may hold millions of entries.
//
// Node layout in the arena (little-endian):
//
//	[keyLen u32][valLen u32][height u16][pad u16][next: height x u32][key bytes][value bytes]
//
// A node is referenced by the offset of its first byte. Offset 0 is reserved as
// the nil link, so the arena always begins with a one-byte pad.

const (
	nodeHeaderLen = 4 + 4 + 2 + 2 // keyLen, valLen, height, pad
	nilNode       = 0
)

// skiplist is a single-writer, concurrent-reader ordered map over internal
// keys. levisdb serializes writes through the shard lock; readers traverse via
// offsets that, once written, are never moved (the arena only grows by append),
// so a reader never observes a torn node.
type skiplist struct {
	arena  []byte
	head   uint32 // offset of the head node (fixed height maxHeight)
	height int    // current max height in use
	rng    *rand.Rand
}

func newSkiplist(seed int64) *skiplist {
	s := &skiplist{
		arena: make([]byte, 1, 4096), // index 0 reserved as nil link
		rng:   rand.New(rand.NewSource(seed)),
	}
	// The head node has no key/value and full height so any level can start.
	s.head = s.allocNode(nil, nil, maxHeight)
	s.height = 1
	return s
}

// allocNode appends a node with the given key/value and height and returns its
// offset. The arena may reallocate (grow) here; that is safe because only the
// single writer calls allocNode and existing offsets stay valid.
func (s *skiplist) allocNode(key, value []byte, height int) uint32 {
	off := uint32(len(s.arena))
	size := nodeHeaderLen + height*4 + len(key) + len(value)
	s.arena = append(s.arena, make([]byte, size)...)

	binary.LittleEndian.PutUint32(s.arena[off:], uint32(len(key)))
	binary.LittleEndian.PutUint32(s.arena[off+4:], uint32(len(value)))
	binary.LittleEndian.PutUint16(s.arena[off+8:], uint16(height))
	// next pointers (height x u32) are zero == nilNode by default.

	dataOff := off + nodeHeaderLen + uint32(height)*4
	copy(s.arena[dataOff:], key)
	copy(s.arena[dataOff+uint32(len(key)):], value)
	return off
}

func (s *skiplist) keyLen(off uint32) uint32 { return binary.LittleEndian.Uint32(s.arena[off:]) }
func (s *skiplist) valLen(off uint32) uint32 { return binary.LittleEndian.Uint32(s.arena[off+4:]) }
func (s *skiplist) nodeHeight(off uint32) int {
	return int(binary.LittleEndian.Uint16(s.arena[off+8:]))
}

// key returns the node's key as a subslice of the arena (read-only).
func (s *skiplist) key(off uint32) []byte {
	h := s.nodeHeight(off)
	dataOff := off + nodeHeaderLen + uint32(h)*4
	return s.arena[dataOff : dataOff+s.keyLen(off)]
}

// value returns the node's value as a subslice of the arena (read-only).
func (s *skiplist) value(off uint32) []byte {
	h := s.nodeHeight(off)
	kl := s.keyLen(off)
	dataOff := off + nodeHeaderLen + uint32(h)*4 + kl
	return s.arena[dataOff : dataOff+s.valLen(off)]
}

// next returns the offset of the next node at the given level, or nilNode.
func (s *skiplist) next(off uint32, level int) uint32 {
	return binary.LittleEndian.Uint32(s.arena[off+nodeHeaderLen+uint32(level)*4:])
}

func (s *skiplist) setNext(off uint32, level int, target uint32) {
	binary.LittleEndian.PutUint32(s.arena[off+nodeHeaderLen+uint32(level)*4:], target)
}

func (s *skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && s.rng.Intn(branching) == 0 {
		h++
	}
	return h
}

// findGE returns the offset of the first node whose key is >= key, filling prev
// with the predecessor offset at each level (used for insertion).
func (s *skiplist) findGE(key []byte, prev *[maxHeight]uint32) uint32 {
	x := s.head
	for level := s.height - 1; level >= 0; level-- {
		for {
			nxt := s.next(x, level)
			if nxt == nilNode || ikeyCompare(s.key(nxt), key) >= 0 {
				break
			}
			x = nxt
		}
		if prev != nil {
			prev[level] = x
		}
	}
	return s.next(x, 0)
}

// insert adds key/value. Caller guarantees no concurrent writers.
func (s *skiplist) insert(key, value []byte) {
	var prev [maxHeight]uint32
	s.findGE(key, &prev)

	h := s.randomHeight()
	if h > s.height {
		for i := s.height; i < h; i++ {
			prev[i] = s.head
		}
		s.height = h
	}

	n := s.allocNode(key, value, h)
	for i := 0; i < h; i++ {
		s.setNext(n, i, s.next(prev[i], i))
		s.setNext(prev[i], i, n)
	}
}

// seek returns the offset of the first node with key >= target, or nilNode.
func (s *skiplist) seek(target []byte) uint32 {
	return s.findGE(target, nil)
}

// first returns the offset of the smallest node, or nilNode if empty.
func (s *skiplist) first() uint32 {
	return s.next(s.head, 0)
}
