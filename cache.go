// The block cache is a size-bounded LRU for decoded SSTable blocks, keyed
// by table number and block offset. Keeping hot index, filter, and data blocks
// in memory avoids re-reading and re-decompressing them from a slow disk, which
// is where the balanced-workload read speed comes from.
//
// The cache is striped into independent LRU shards, each with its own mutex, so
// concurrent readers across the database's key shards do not serialize on one
// lock (even a cache hit mutates LRU order). A block maps to a stripe by
// hashing its (table, offset) key.

package levisdb

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
)

// cacheStripes is the number of independent LRU shards. A power of two keeps the
// stripe index a cheap mask. 16 matches LevelDB's ShardedLRUCache.
const cacheStripes = 16

// Key identifies a cached block by its table and offset within that table.
type blockCacheKey struct {
	Table  uint32
	Offset uint64
}

// entry is an intrusive LRU list node: prev/next are embedded so the cache
// needs one heap object per block instead of a separate container/list element,
// and lookups avoid interface boxing. The list is circular through a sentinel
// so head/tail operations need no nil checks.
type entry struct {
	key        blockCacheKey
	value      []byte
	prev, next *entry
}

// cacheShard is one striped LRU, bounded by total value bytes.
type cacheShard struct {
	mu       sync.Mutex
	capacity int64
	size     int64
	root     entry // sentinel; root.next is most-recently-used, root.prev is LRU
	items    map[blockCacheKey]*entry
}

func newCacheShard(capacity int64) *cacheShard {
	s := &cacheShard{capacity: capacity, items: map[blockCacheKey]*entry{}}
	s.root.prev = &s.root
	s.root.next = &s.root
	return s
}

// unlink removes e from the list.
func (s *cacheShard) unlink(e *entry) {
	e.prev.next = e.next
	e.next.prev = e.prev
}

// pushFront inserts e at the most-recently-used position.
func (s *cacheShard) pushFront(e *entry) {
	e.prev = &s.root
	e.next = s.root.next
	s.root.next.prev = e
	s.root.next = e
}

// get returns the cached block for key, marking it most-recently-used.
func (s *cacheShard) get(key blockCacheKey) ([]byte, bool) {
	s.mu.Lock()
	if e, ok := s.items[key]; ok {
		s.unlink(e)
		s.pushFront(e)
		v := e.value
		s.mu.Unlock()
		return v, true
	}
	s.mu.Unlock()
	return nil, false
}

// add inserts a block, evicting the least-recently-used entries until within
// this stripe's capacity.
func (s *cacheShard) add(key blockCacheKey, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.items[key]; ok {
		s.size += int64(len(value)) - int64(len(e.value))
		e.value = value
		s.unlink(e)
		s.pushFront(e)
	} else {
		e := &entry{key: key, value: value}
		s.pushFront(e)
		s.items[key] = e
		s.size += int64(len(value))
	}
	for s.size > s.capacity {
		s.evictOldest()
	}
}

func (s *cacheShard) evictOldest() {
	lru := s.root.prev
	if lru == &s.root {
		return
	}
	s.unlink(lru)
	delete(s.items, lru.key)
	s.size -= int64(len(lru.value))
}

func (s *cacheShard) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

func (s *cacheShard) bytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// blockCacheT is a concurrency-safe LRU bounded by total value bytes, striped
// across cacheStripes independent shards.
type blockCacheT struct {
	capacity int64
	shards   []*cacheShard
	seed     maphash.Seed

	// hits/misses are cumulative since open. On a slow disk a miss is a physical
	// seek, so the hit rate is the primary read-latency and cache-sizing signal.
	hits   atomic.Int64
	misses atomic.Int64
}

// newBlockCache returns a cache holding up to capacity bytes of block data,
// split evenly across stripes. A capacity of zero disables caching (get always
// misses, Add is a no-op).
func newBlockCache(capacity int64) *blockCacheT {
	c := &blockCacheT{capacity: capacity, seed: maphash.MakeSeed()}
	if capacity == 0 {
		return c
	}
	// Divide capacity across stripes and distribute the remainder one byte at a
	// time. Some stripes intentionally have zero capacity when the entire cache
	// is smaller than cacheStripes; forcing every stripe to one byte would exceed
	// the caller's configured hard bound.
	per := capacity / cacheStripes
	remainder := capacity % cacheStripes
	c.shards = make([]*cacheShard, cacheStripes)
	for i := range c.shards {
		shardCap := per
		if int64(i) < remainder {
			shardCap++
		}
		c.shards[i] = newCacheShard(shardCap)
	}
	return c
}

// shardFor hashes key to its stripe.
func (c *blockCacheT) shardFor(key blockCacheKey) *cacheShard {
	var h maphash.Hash
	h.SetSeed(c.seed)
	var buf [12]byte
	buf[0] = byte(key.Table)
	buf[1] = byte(key.Table >> 8)
	buf[2] = byte(key.Table >> 16)
	buf[3] = byte(key.Table >> 24)
	for i := 0; i < 8; i++ {
		buf[4+i] = byte(key.Offset >> (8 * i))
	}
	_, _ = h.Write(buf[:])
	return c.shards[h.Sum64()&(cacheStripes-1)]
}

// get returns the cached block for key and marks it most-recently-used. The
// returned slice must not be modified by the caller.
func (c *blockCacheT) get(key blockCacheKey) ([]byte, bool) {
	if c.capacity == 0 {
		return nil, false
	}
	if v, ok := c.shardFor(key).get(key); ok {
		c.hits.Add(1)
		return v, true
	}
	c.misses.Add(1)
	return nil, false
}

// Add inserts a block, evicting least-recently-used entries in its stripe until
// within the stripe's capacity. A block larger than a stripe's capacity is not
// cached.
func (c *blockCacheT) Add(key blockCacheKey, value []byte) {
	if c.capacity == 0 {
		return
	}
	s := c.shardFor(key)
	if int64(len(value)) > s.capacity {
		return
	}
	s.add(key, value)
}

// stats returns cumulative hit/miss counts across all stripes.
func (c *blockCacheT) stats() (hits, misses int64) {
	return c.hits.Load(), c.misses.Load()
}

// Len returns the number of cached blocks across all stripes.
func (c *blockCacheT) Len() int {
	n := 0
	for _, s := range c.shards {
		n += s.len()
	}
	return n
}

// Size returns the total cached bytes across all stripes.
func (c *blockCacheT) Size() int64 {
	var n int64
	for _, s := range c.shards {
		n += s.bytes()
	}
	return n
}
