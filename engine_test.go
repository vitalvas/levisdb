package levisdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestEngine builds a standalone engine rooted in a temp dir, with a small
// memtable threshold so flushes are easy to trigger.
func newTestEngine(t *testing.T, memSize int64) *engineT {
	t.Helper()
	dir := t.TempDir()
	tablePath := func(num uint32) (string, error) {
		return filepath.Join(dir, fmt.Sprintf("%08x.sst", num)), nil
	}
	cfg := engineConfigT{
		MemtableSize:   memSize,
		BloomBits:      10,
		BlockSize:      256,
		FreshCodecName: "none",
	}
	s := newEngine(cfg, newAllocator(0), tablePath)
	t.Cleanup(func() { s.Close() })
	return s
}

func mustGet(t *testing.T, s *engineT, seq uint64, key string) []byte {
	t.Helper()
	v, found, deleted, err := s.get(seq, []byte(key))
	require.NoError(t, err)
	require.True(t, found, "key %q not found", key)
	require.False(t, deleted, "key %q deleted", key)
	return v
}

func TestEnginePutGet(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)

	t.Run("memtable read", func(t *testing.T) {
		s.Put(1, []byte("a"), []byte("1"))
		assert.Equal(t, []byte("1"), mustGet(t, s, 10, "a"))
	})

	t.Run("newest version wins", func(t *testing.T) {
		s.Put(2, []byte("a"), []byte("2"))
		assert.Equal(t, []byte("2"), mustGet(t, s, 10, "a"))
	})

	t.Run("snapshot hides newer", func(t *testing.T) {
		assert.Equal(t, []byte("1"), mustGet(t, s, 1, "a"))
	})

	t.Run("missing key", func(t *testing.T) {
		_, found, _, err := s.get(10, []byte("missing"))
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("delete", func(t *testing.T) {
		s.del(3, []byte("a"))
		_, found, deleted, err := s.get(10, []byte("a"))
		require.NoError(t, err)
		assert.True(t, found)
		assert.True(t, deleted)
	})
}

func TestEngineMemState(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 64)
	assert.True(t, s.memEmpty())
	assert.False(t, s.needFlush())

	// Write enough to cross the small threshold.
	for i := 0; i < 100; i++ {
		s.Put(uint64(i+1), []byte(fmt.Sprintf("k%03d", i)), []byte("value-payload"))
	}
	assert.False(t, s.memEmpty())
	assert.True(t, s.needFlush())
}

func TestEngineFlush(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)

	t.Run("empty flush is a no-op", func(t *testing.T) {
		require.NoError(t, s.Flush())
		assert.Empty(t, s.Tables())
	})

	t.Run("flush persists to a table", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			s.Put(uint64(i+1), []byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%d", i)))
		}
		require.NoError(t, s.Flush())
		require.Len(t, s.Tables(), 1)
		assert.True(t, s.memEmpty(), "memtable cleared after flush")

		// Reads now come from the table.
		for i := 0; i < 50; i++ {
			assert.Equal(t, []byte(fmt.Sprintf("v%d", i)), mustGet(t, s, 100, fmt.Sprintf("k%03d", i)))
		}
	})
}

func TestEngineReadThroughLayers(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)

	// v1 in a flushed table.
	s.Put(1, []byte("k"), []byte("v1"))
	require.NoError(t, s.Flush())
	// v2 in the live memtable; must shadow the table.
	s.Put(2, []byte("k"), []byte("v2"))

	assert.Equal(t, []byte("v2"), mustGet(t, s, 10, "k"))
	assert.Equal(t, []byte("v1"), mustGet(t, s, 1, "k"), "old snapshot reads the table version")
}

// TestEngineFlushErrors covers writeTable's reachable failure branches (bad
// codec, tablePath error, os.OpenFile error).
//
// note: writeTable's f.Sync, w.Add/w.finish, and newCachedTableReader error
// branches are unreachable in tests without syscall-failure injection, since
// they only fire on I/O faults writing/reading a valid, freshly-created file.
func TestEngineFlushErrors(t *testing.T) {
	t.Parallel()

	t.Run("bad codec surfaces from writeTable", func(t *testing.T) {
		dir := t.TempDir()
		tablePath := func(num uint32) (string, error) {
			return filepath.Join(dir, fmt.Sprintf("%08x.sst", num)), nil
		}
		cfg := engineConfigT{MemtableSize: 1 << 20, BloomBits: 10, BlockSize: 256, FreshCodecName: "bogus"}
		s := newEngine(cfg, newAllocator(0), tablePath)
		t.Cleanup(func() { s.Close() })

		s.Put(1, []byte("k"), []byte("v"))
		assert.Error(t, s.Flush())
	})

	t.Run("tablePath error", func(t *testing.T) {
		want := errors.New("no path")
		cfg := engineConfigT{MemtableSize: 1 << 20, BloomBits: 10, BlockSize: 256, FreshCodecName: "none"}
		s := newEngine(cfg, newAllocator(0), func(uint32) (string, error) { return "", want })
		t.Cleanup(func() { s.Close() })

		s.Put(1, []byte("k"), []byte("v"))
		assert.ErrorIs(t, s.Flush(), want)
	})

	t.Run("open error on bad path", func(t *testing.T) {
		dir := t.TempDir()
		// A path whose parent is a file, not a directory, so os.OpenFile fails.
		cfg := engineConfigT{MemtableSize: 1 << 20, BloomBits: 10, BlockSize: 256, FreshCodecName: "none"}
		s := newEngine(cfg, newAllocator(0), func(uint32) (string, error) {
			return filepath.Join(dir, "missing-dir", "x.sst"), nil
		})
		t.Cleanup(func() { s.Close() })

		s.Put(1, []byte("k"), []byte("v"))
		assert.Error(t, s.Flush())
	})
}

func TestEngineGetImmutable(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)

	// Seal the active memtable as immutable without flushing, then read through
	// the imm layer. rotate installs a fresh empty active memtable.
	s.Put(1, []byte("k"), []byte("v1"))
	require.True(t, s.rotate())
	require.True(t, s.memEmpty(), "active memtable is fresh after rotate")

	assert.Equal(t, []byte("v1"), mustGet(t, s, 10, "k"), "read served from immutable memtable")
}

func TestEngineGetCorruptTableErrors(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	var wrote string
	tablePath := func(num uint32) (string, error) {
		wrote = filepath.Join(dir, fmt.Sprintf("%08x.sst", num))
		return wrote, nil
	}
	cfg := engineConfigT{MemtableSize: 1 << 20, BloomBits: 10, BlockSize: 4096, FreshCodecName: "none"}
	s := newEngine(cfg, newAllocator(0), tablePath)
	defer s.Close()

	s.Put(1, []byte("present"), []byte("value"))
	require.NoError(t, s.Flush())

	// Reopen the table with a flipped byte inside the first data block so the
	// block CRC check fails when get reads it. The footer/index/filter are
	// untouched, so the table still opens.
	data, err := os.ReadFile(wrote)
	require.NoError(t, err)
	data[3] ^= 0xff
	require.NoError(t, os.WriteFile(wrote, data, 0o644))

	s2 := newEngine(cfg, newAllocator(100), tablePath)
	defer s2.Close()
	require.NoError(t, s2.openTable(tableSpec{num: 1, depth: 0, path: wrote}))

	// The bloom filter says "maybe" for a key that was inserted, forcing the
	// corrupt data block to be read and its CRC error to surface.
	value, found, deleted, gerr := s2.get(10, []byte("present"))
	require.Error(t, gerr)
	assert.Nil(t, value)
	assert.False(t, found)
	assert.False(t, deleted)
}

func TestTableMetaMayContainAndOverlaps(t *testing.T) {
	t.Parallel()
	tbl := &tableMeta{minKey: []byte("d"), maxKey: []byte("m")}
	assert.False(t, tbl.mayContain([]byte("a")), "below range")
	assert.True(t, tbl.mayContain([]byte("d")), "at min")
	assert.True(t, tbl.mayContain([]byte("h")), "inside")
	assert.True(t, tbl.mayContain([]byte("m")), "at max")
	assert.False(t, tbl.mayContain([]byte("z")), "above range")

	// Unknown bounds cover everything so a table is never wrongly skipped.
	unknown := &tableMeta{}
	assert.True(t, unknown.mayContain([]byte("anything")))

	// overlapsRange: [d,m] vs various [start,end].
	assert.True(t, tbl.overlapsRange([]byte("a"), []byte("e")))  // clips left
	assert.True(t, tbl.overlapsRange([]byte("k"), []byte("z")))  // clips right
	assert.True(t, tbl.overlapsRange(nil, nil))                  // unbounded
	assert.False(t, tbl.overlapsRange([]byte("n"), []byte("z"))) // entirely right
	assert.False(t, tbl.overlapsRange([]byte("a"), []byte("c"))) // entirely left (end exclusive-ish upper)
	assert.True(t, unknown.overlapsRange([]byte("x"), []byte("y")))
}

func TestEngineTableBoundsPopulatedAndReadSkip(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	defer s.Close()

	for _, k := range []string{"k10", "k20", "k30"} {
		s.Put(uint64(len(k)), []byte(k), []byte(fmt.Sprintf("v-%s", k)))
	}
	require.NoError(t, s.Flush())

	s.mu.RLock()
	require.Len(t, s.tables, 1)
	tbl := s.tables[0]
	s.mu.RUnlock()
	assert.Equal(t, []byte("k10"), tbl.minKey, "min bound is smallest user key")
	assert.Equal(t, []byte("k30"), tbl.maxKey, "max bound is largest user key")

	// A key outside the recorded range is skipped (mayContain=false) yet reads
	// still return not-found correctly.
	assert.False(t, tbl.mayContain([]byte("z99")))
	_, found, _, err := s.get(100, []byte("z99"))
	require.NoError(t, err)
	assert.False(t, found)
	// In-range key still found.
	assert.Equal(t, []byte("v-k20"), mustGet(t, s, 100, "k20"))
}

func TestOpenTableMetaConcurrentEviction(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	flushSingle(t, s, 1, "key", "value")
	src := s.tables[0]
	s.cfg.FDs = newFDPool(1)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 30 {
				m, err := s.openTableMeta(tableSpec{num: 100, path: src.path, size: src.size})
				if err != nil {
					errs <- err
					return
				}
				if err := m.releaseOwner(false); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
