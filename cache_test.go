package levisdb

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockCacheAddAndGet(t *testing.T) {
	t.Parallel()
	c := newBlockCache(1024)
	key := blockCacheKey{Table: 1, Offset: 8}
	val := []byte("block")

	c.Add(key, val)
	got, ok := c.get(key)
	require.True(t, ok)
	assert.Equal(t, val, got)
	assert.Equal(t, 1, c.Len())
	assert.Equal(t, int64(len(val)), c.Size())

	_, ok = c.get(blockCacheKey{Table: 2, Offset: 0})
	assert.False(t, ok)
}

func TestBlockCacheZeroCapacityDisabled(t *testing.T) {
	t.Parallel()
	c := newBlockCache(0)
	key := blockCacheKey{Table: 1}
	c.Add(key, []byte("x"))
	_, ok := c.get(key)
	assert.False(t, ok)
	assert.Equal(t, 0, c.Len())
	assert.Equal(t, int64(0), c.Size())
}

func TestBlockCacheTinyCapacityHonorsGlobalBound(t *testing.T) {
	t.Parallel()
	for capacity := int64(1); capacity < cacheStripes; capacity++ {
		c := newBlockCache(capacity)
		var allocated int64
		for _, s := range c.shards {
			allocated += s.capacity
		}
		assert.Equal(t, capacity, allocated, "capacity %d", capacity)

		for i := 0; i < 1000; i++ {
			c.Add(blockCacheKey{Table: uint32(i + 1)}, []byte{byte(i)})
		}
		assert.LessOrEqual(t, c.Size(), capacity, "capacity %d", capacity)
	}
}

func TestBlockCacheUpdateExisting(t *testing.T) {
	t.Parallel()
	c := newBlockCache(1024)
	key := blockCacheKey{Table: 1, Offset: 0}

	c.Add(key, []byte("aa"))
	c.Add(key, []byte("bbbb"))

	got, ok := c.get(key)
	require.True(t, ok)
	assert.Equal(t, []byte("bbbb"), got)
	assert.Equal(t, 1, c.Len())
	assert.Equal(t, int64(4), c.Size())
}

// LRU eviction semantics live on cacheShard (one stripe), so test them there:
// a single stripe with a small capacity behaves like the old unstriped cache.
func TestCacheShardEviction(t *testing.T) {
	t.Parallel()
	// Capacity fits two 4-byte blocks but not three.
	s := newCacheShard(8)
	k1 := blockCacheKey{Table: 1}
	k2 := blockCacheKey{Table: 2}
	k3 := blockCacheKey{Table: 3}

	s.add(k1, []byte("1111"))
	s.add(k2, []byte("2222"))
	// Touch k1 so it becomes most-recently-used; k2 is now the LRU.
	_, ok := s.get(k1)
	require.True(t, ok)

	s.add(k3, []byte("3333"))

	_, ok = s.get(k2)
	assert.False(t, ok, "k2 was least-recently-used and should be evicted")
	_, ok = s.get(k1)
	assert.True(t, ok)
	_, ok = s.get(k3)
	assert.True(t, ok)
	assert.Equal(t, int64(8), s.bytes())
	assert.Equal(t, 2, s.len())
}

func TestBlockCacheOversizedNotCached(t *testing.T) {
	t.Parallel()
	// A block larger than a stripe's capacity is not cached. With cacheStripes
	// stripes, a stripe holds capacity/cacheStripes bytes, so size the block
	// above the whole capacity to guarantee it exceeds any stripe.
	c := newBlockCache(64)
	key := blockCacheKey{Table: 1}
	c.Add(key, make([]byte, 128))
	_, ok := c.get(key)
	assert.False(t, ok)
	assert.Equal(t, 0, c.Len())
	assert.Equal(t, int64(0), c.Size())
}

func TestCacheShardEvictOldestEmpty(t *testing.T) {
	t.Parallel()
	// evictOldest on an empty shard is a no-op; the add path never calls it with
	// an empty list, so exercise the guard directly.
	s := newCacheShard(1024)
	s.evictOldest()
	assert.Equal(t, 0, s.len())
	assert.Equal(t, int64(0), s.bytes())
}

// TestBlockCacheStripingDistributes verifies distinct keys land across stripes
// and all remain retrievable when total capacity is ample.
func TestBlockCacheStripingDistributes(t *testing.T) {
	t.Parallel()
	c := newBlockCache(16 << 20)
	const n = 500
	block := make([]byte, 64)
	for i := 0; i < n; i++ {
		c.Add(blockCacheKey{Table: 1, Offset: uint64(i)}, block)
	}
	assert.Equal(t, n, c.Len(), "all blocks fit under ample capacity")
	// Verify at least two stripes received entries (striping actually spreads).
	nonEmpty := 0
	for _, s := range c.shards {
		if s.len() > 0 {
			nonEmpty++
		}
	}
	assert.Greater(t, nonEmpty, 1, "keys should distribute across multiple stripes")
}

func TestBlockCacheConcurrent(t *testing.T) {
	t.Parallel()
	c := newBlockCache(1 << 20)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := blockCacheKey{Table: uint32(n), Offset: uint64(n)}
			c.Add(key, []byte("payload"))
			c.get(key)
			c.Len()
			c.Size()
		}(i)
	}
	wg.Wait()
	assert.Equal(t, 50, c.Len())
}

func BenchmarkCacheGetHit(b *testing.B) {
	c := newBlockCache(64 << 20)
	block := make([]byte, 4096)
	for i := 0; i < 1000; i++ {
		c.Add(blockCacheKey{Table: 1, Offset: uint64(i)}, block)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.get(blockCacheKey{Table: 1, Offset: uint64(i % 1000)})
	}
}
