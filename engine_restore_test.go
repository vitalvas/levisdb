package levisdb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTablesSnapshot(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	assert.Empty(t, s.Tables())

	flushSingle(t, s, 1, "a", "1")
	flushSingle(t, s, 2, "b", "2")

	tabs := s.Tables()
	require.Len(t, tabs, 2)
	for _, tab := range tabs {
		assert.Equal(t, 0, tab.Depth, "fresh flush lands at L0")
		assert.Positive(t, tab.Size)
		assert.NotZero(t, tab.Num)
	}
	assert.NotEqual(t, tabs[0].Num, tabs[1].Num, "distinct file numbers")
}

func TestPickCompactionDelegates(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	assert.Equal(t, -1, s.pickCompaction(2, 0, 0))
	flushSingle(t, s, 1, "a", "1")
	flushSingle(t, s, 2, "b", "2")
	assert.Equal(t, 0, s.pickCompaction(2, 0, 0))
}

func TestOpenTableRestore(t *testing.T) {
	t.Parallel()
	// Build a shard, flush data, capture the table path, then close it.
	dir := t.TempDir()
	tablePath := func(num uint32) (string, error) {
		return filepath.Join(dir, fmt.Sprintf("%08x.sst", num)), nil
	}
	cfg := shardConfigT{MemtableSize: 1 << 20, BloomBits: 10, BlockSize: 256, FreshCodecName: "none"}

	src := newShard(cfg, newAllocator(0), tablePath, 1)
	for i := 0; i < 30; i++ {
		src.Put(uint64(i+1), []byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	require.NoError(t, src.Flush())
	snap := src.Tables()
	require.Len(t, snap, 1)
	path, err := tablePath(snap[0].Num)
	require.NoError(t, err)
	require.NoError(t, src.Close())

	t.Run("restore with known size", func(t *testing.T) {
		dst := newShard(cfg, newAllocator(0), tablePath, 1)
		t.Cleanup(func() { dst.Close() })
		require.NoError(t, dst.openTable(tableSpec{
			num:    snap[0].Num,
			depth:  snap[0].Depth,
			path:   path,
			size:   snap[0].Size,
			minKey: snap[0].MinKey,
			maxKey: snap[0].MaxKey,
		}))
		require.Len(t, dst.Tables(), 1)
		for i := 0; i < 30; i++ {
			assert.Equal(t, []byte(fmt.Sprintf("v%d", i)), mustGet(t, dst, 100, fmt.Sprintf("k%02d", i)))
		}
	})

	t.Run("size 0 stats the file", func(t *testing.T) {
		dst := newShard(cfg, newAllocator(0), tablePath, 1)
		t.Cleanup(func() { dst.Close() })
		require.NoError(t, dst.openTable(tableSpec{
			num:    snap[0].Num,
			depth:  snap[0].Depth,
			path:   path,
			minKey: snap[0].MinKey,
			maxKey: snap[0].MaxKey,
		}))
		assert.Equal(t, []byte("v0"), mustGet(t, dst, 100, "k00"))
	})

	t.Run("missing file errors", func(t *testing.T) {
		dst := newShard(cfg, newAllocator(0), tablePath, 1)
		t.Cleanup(func() { dst.Close() })
		err := dst.openTable(tableSpec{num: 999, depth: 0, path: filepath.Join(dir, "nope.sst")})
		assert.Error(t, err)
	})

	t.Run("non-table file errors", func(t *testing.T) {
		garbage := filepath.Join(dir, "garbage.sst")
		require.NoError(t, os.WriteFile(garbage, []byte("not a valid sstable file at all"), 0o644))

		dst := newShard(cfg, newAllocator(0), tablePath, 1)
		t.Cleanup(func() { dst.Close() })
		// size 0 => Stat succeeds, but newCachedTableReader rejects the bad magic.
		assert.Error(t, dst.openTable(tableSpec{num: 998, depth: 0, path: garbage}))
	})
}
