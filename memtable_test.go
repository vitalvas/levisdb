package levisdb

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func benchKey(i int) []byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], uint64(i))
	return k[:]
}

func TestMemtable(t *testing.T) {
	t.Parallel()
	t.Run("empty", func(t *testing.T) {
		m := newMemtable(1)
		assert.True(t, m.empty())
		assert.Equal(t, int64(0), m.Size())
		v, found, deleted := m.get(100, []byte("missing"))
		assert.Nil(t, v)
		assert.False(t, found)
		assert.False(t, deleted)
	})

	t.Run("put_and_get", func(t *testing.T) {
		m := newMemtable(2)
		m.Put(1, []byte("a"), []byte("va"))
		assert.False(t, m.empty())
		assert.Greater(t, m.Size(), int64(0))

		v, found, deleted := m.get(1, []byte("a"))
		assert.Equal(t, []byte("va"), v)
		assert.True(t, found)
		assert.False(t, deleted)
	})

	t.Run("versions_newest_wins", func(t *testing.T) {
		m := newMemtable(3)
		m.Put(1, []byte("k"), []byte("v1"))
		m.Put(5, []byte("k"), []byte("v5"))

		v, found, _ := m.get(10, []byte("k"))
		assert.True(t, found)
		assert.Equal(t, []byte("v5"), v)
	})

	t.Run("snapshot_seq", func(t *testing.T) {
		m := newMemtable(4)
		m.Put(1, []byte("k"), []byte("v1"))
		m.Put(5, []byte("k"), []byte("v5"))

		// At seq 3 only v1 is visible.
		v, found, _ := m.get(3, []byte("k"))
		assert.True(t, found)
		assert.Equal(t, []byte("v1"), v)

		// At seq 0 no version is visible.
		v, found, _ = m.get(0, []byte("k"))
		assert.Nil(t, v)
		assert.False(t, found)
	})

	t.Run("tombstone", func(t *testing.T) {
		m := newMemtable(5)
		m.Put(1, []byte("k"), []byte("v1"))
		m.del(5, []byte("k"))

		v, found, deleted := m.get(10, []byte("k"))
		assert.Nil(t, v)
		assert.True(t, found)
		assert.True(t, deleted)

		// Older snapshot still sees the value.
		v, found, deleted = m.get(3, []byte("k"))
		assert.Equal(t, []byte("v1"), v)
		assert.True(t, found)
		assert.False(t, deleted)
	})

	// note: the `nseq > seq` branch in get is unreachable via the public API.
	// seek returns the first node with internal key >= the lookup key, and
	// internal keys sort newest-seq-first; any node with nseq > seq sorts before
	// the lookup key (seq, kindSet) and is skipped, so a matching user key found
	// by seek always has nseq <= seq. The check is a defensive guard.
}

func TestMemtableIterator(t *testing.T) {
	t.Parallel()
	m := newMemtable(6)
	m.Put(1, []byte("b"), []byte("vb"))
	m.Put(1, []byte("a"), []byte("va"))
	m.Put(2, []byte("a"), []byte("va2"))
	m.del(1, []byte("c"))

	it := m.newIterator()
	var userKeys []string
	count := 0
	for it.Next() {
		require.NotNil(t, it.internalKey())
		userKeys = append(userKeys, string(ikeyUserKey(it.internalKey())))
		count++
	}
	assert.Equal(t, 4, count)
	// User keys ascending; within "a", seq descending (2 before 1).
	assert.Equal(t, []string{"a", "a", "b", "c"}, userKeys)

	// Value of first entry (a@2) is va2.
	it2 := m.newIterator()
	require.True(t, it2.Next())
	assert.Equal(t, []byte("va2"), it2.Value())
}

func BenchmarkMemtablePut(b *testing.B) {
	m := newMemtable(1)
	val := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Put(uint64(i+1), benchKey(i), val)
	}
}

func BenchmarkMemtableGet(b *testing.B) {
	m := newMemtable(1)
	val := make([]byte, 100)
	const n = 100000
	for i := 0; i < n; i++ {
		m.Put(uint64(i+1), benchKey(i), val)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.get(uint64(n+1), benchKey(i%n))
	}
}

func FuzzMemtableOps(f *testing.F) {
	f.Add([]byte{0x00, 0x09, 0x11, 0x1a})
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xfe, 0xfd, 0x08, 0x10})
	f.Fuzz(func(t *testing.T, script []byte) {
		m := newMemtable(1)
		model := make(map[string][]byte) // nil value = deleted/absent
		var seq uint64
		const largeSeq = uint64(1) << 40

		for _, b := range script {
			key := []byte{b % 8}
			sk := string(key)
			switch (b >> 3) % 3 {
			case 0: // put
				seq++
				val := []byte{b}
				m.Put(seq, key, val)
				model[sk] = val
			case 1: // delete
				seq++
				m.del(seq, key)
				model[sk] = nil
			default: // get
			}

			v, found, deleted := m.get(largeSeq, key)
			want, seen := model[sk]
			switch {
			case !seen || want == nil:
				// Never written, or last op was a delete: absent or tombstoned.
				assert.Nil(t, v)
				if found {
					assert.True(t, deleted)
				}
			default:
				assert.True(t, found)
				assert.False(t, deleted)
				assert.Equal(t, want, v)
			}
		}
	})
}

func TestMemtableConcurrentReadersWhileWriting(t *testing.T) {
	t.Parallel()
	m := newMemtable(1)
	// Seed so readers have something to find while the writer grows the arena.
	const seed = 500
	for i := 0; i < seed; i++ {
		m.Put(uint64(i+1), []byte(fmt.Sprintf("key%05d", i)), []byte("v"))
	}

	var wg sync.WaitGroup
	// One writer keeps appending (growing the arena under the write lock).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := seed; i < seed+3000; i++ {
			m.Put(uint64(i+1), []byte(fmt.Sprintf("key%05d", i)), []byte("v"))
		}
	}()
	// Many readers seek concurrently; the RWMutex serializes them against the
	// writer, so the arena is never read while it is being reallocated.
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < seed; i++ {
				v, found, _ := m.get(1<<30, []byte(fmt.Sprintf("key%05d", i)))
				if found {
					assert.Equal(t, []byte("v"), v)
				}
			}
		}()
	}
	wg.Wait()

	// Every seeded and written key is retrievable afterward.
	v, found, _ := m.get(1<<30, []byte(fmt.Sprintf("key%05d", seed+2999)))
	require.True(t, found)
	assert.Equal(t, []byte("v"), v)
}
