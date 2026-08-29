package levisdb

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collect drains an engine iterator into key/value string slices.
func collect(it *engineIterator) (keys, vals []string) {
	for it.Next() {
		keys = append(keys, string(it.Key()))
		vals = append(vals, string(it.Value()))
	}
	return keys, vals
}

func TestEngineIterator(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	// b, d live in a flushed table; a, c, e in the live memtable.
	s.Put(1, []byte("b"), []byte("2"))
	s.Put(1, []byte("d"), []byte("4"))
	require.NoError(t, s.Flush())
	s.Put(2, []byte("a"), []byte("1"))
	s.Put(2, []byte("c"), []byte("3"))
	s.Put(2, []byte("e"), []byte("5"))

	t.Run("full ascending scan across layers", func(t *testing.T) {
		keys, vals := collect(s.NewIterator(10))
		assert.Equal(t, []string{"a", "b", "c", "d", "e"}, keys)
		assert.Equal(t, []string{"1", "2", "3", "4", "5"}, vals)
	})

	t.Run("range is half-open [start,end)", func(t *testing.T) {
		keys, _ := collect(s.NewRangeIterator(10, []byte("b"), []byte("d")))
		assert.Equal(t, []string{"b", "c"}, keys)
	})
}

func TestEngineIteratorSnapshotAndDeletes(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	s.Put(1, []byte("k"), []byte("v1"))
	s.Put(3, []byte("k"), []byte("v3"))
	s.Put(2, []byte("gone"), []byte("x"))
	s.del(4, []byte("gone"))

	t.Run("newest visible version per key", func(t *testing.T) {
		keys, vals := collect(s.NewIterator(10))
		assert.Equal(t, []string{"k"}, keys, "tombstoned key skipped")
		assert.Equal(t, []string{"v3"}, vals)
	})

	t.Run("old snapshot ignores newer writes", func(t *testing.T) {
		keys, vals := collect(s.NewIterator(2))
		assert.Equal(t, []string{"gone", "k"}, keys)
		assert.Equal(t, []string{"x", "v1"}, vals)
	})
}

func TestEngineIteratorMany(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	for i := 0; i < 100; i++ {
		s.Put(uint64(i+1), []byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	require.NoError(t, s.Flush())
	// Overwrite half in the memtable to exercise the merge dedup.
	for i := 0; i < 50; i++ {
		s.Put(uint64(i+200), []byte(fmt.Sprintf("k%03d", i)), []byte("new"))
	}

	keys, vals := collect(s.NewIterator(1000))
	require.Len(t, keys, 100)
	for i := 1; i < len(keys); i++ {
		assert.Less(t, keys[i-1], keys[i], "ascending order")
	}
	assert.Equal(t, "new", vals[0], "memtable overwrite wins")
}

func TestEngineIteratorWithImmutable(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	defer s.Close()

	s.Put(1, []byte("a"), []byte("1"))
	// Seal the active memtable as immutable without flushing, so the iterator
	// must include the imm source.
	require.True(t, s.rotate())
	s.Put(2, []byte("b"), []byte("2"))

	it := s.NewRangeIterator(100, nil, nil)
	var keys []string
	for it.Next() {
		keys = append(keys, string(it.Key()))
	}
	assert.Equal(t, []string{"a", "b"}, keys)
}

func BenchmarkEngineScan(b *testing.B) {
	dir := b.TempDir()
	tablePath := func(num uint32) (string, error) {
		return filepath.Join(dir, fmt.Sprintf("%08x.sst", num)), nil
	}
	cfg := engineConfigT{MemtableSize: 1 << 20, BloomBits: 10, BlockSize: 4096, FreshCodecName: "none"}
	s := newEngine(cfg, newAllocator(0), tablePath)
	defer s.Close()

	// Several flushed tables plus a live memtable, so the scan merges multiple
	// sources through mergeIter.
	const total = 20000
	val := make([]byte, 100)
	for i := 0; i < total; i++ {
		var uk [8]byte
		binary.BigEndian.PutUint64(uk[:], uint64(i))
		s.Put(uint64(i+1), uk[:], val)
		if i%5000 == 4999 {
			if err := s.Flush(); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		it := s.NewIterator(uint64(total + 1))
		count := 0
		for it.Next() {
			count++
		}
		if count != total {
			b.Fatalf("scanned %d, want %d", count, total)
		}
	}
}
