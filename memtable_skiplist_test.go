package levisdb

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func slKey(i int) []byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], uint64(i))
	return ikeyEncode(nil, k[:], uint64(i+1), ikeyKindSet)
}

func BenchmarkSkiplistInsert(b *testing.B) {
	s := newSkiplist(1)
	val := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.insert(slKey(i), val)
	}
}

func BenchmarkSkiplistSeek(b *testing.B) {
	s := newSkiplist(1)
	const n = 100000
	val := make([]byte, 100)
	for i := 0; i < n; i++ {
		s.insert(slKey(i), val)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.seek(slKey(i % n))
	}
}

func ik(user string, seq uint64) []byte {
	return ikeyEncode(nil, []byte(user), seq, ikeyKindSet)
}

func TestSkiplist(t *testing.T) {
	t.Parallel()
	t.Run("empty", func(t *testing.T) {
		s := newSkiplist(1)
		assert.Equal(t, uint32(nilNode), s.first())
		assert.Equal(t, uint32(nilNode), s.seek(ik("a", 1)))
	})

	t.Run("insert_and_first", func(t *testing.T) {
		s := newSkiplist(2)
		s.insert(ik("m", 1), []byte("vm"))
		s.insert(ik("a", 1), []byte("va"))
		s.insert(ik("z", 1), []byte("vz"))

		n := s.first()
		require.NotEqual(t, uint32(nilNode), n)
		assert.Equal(t, "a", string(ikeyUserKey(s.key(n))))
		assert.Equal(t, []byte("va"), s.value(n))
	})

	t.Run("ordering", func(t *testing.T) {
		s := newSkiplist(3)
		keys := []string{"d", "b", "e", "a", "c"}
		for _, k := range keys {
			s.insert(ik(k, 1), []byte(k))
		}
		var got []string
		for n := s.first(); n != nilNode; n = s.next(n, 0) {
			got = append(got, string(ikeyUserKey(s.key(n))))
		}
		assert.Equal(t, []string{"a", "b", "c", "d", "e"}, got)
	})

	t.Run("seek", func(t *testing.T) {
		s := newSkiplist(4)
		s.insert(ik("a", 1), []byte("va"))
		s.insert(ik("c", 1), []byte("vc"))

		// seek lands on first key >= target.
		n := s.seek(ik("b", 1))
		require.NotEqual(t, uint32(nilNode), n)
		assert.Equal(t, "c", string(ikeyUserKey(s.key(n))))

		n = s.seek(ik("a", 1))
		require.NotEqual(t, uint32(nilNode), n)
		assert.Equal(t, "a", string(ikeyUserKey(s.key(n))))

		// Past the last key returns nilNode.
		assert.Equal(t, uint32(nilNode), s.seek(ik("z", 1)))
	})

	t.Run("many_entries_sorted", func(t *testing.T) {
		s := newSkiplist(5)
		const n = 200
		for i := 0; i < n; i++ {
			// Insert in reverse to exercise the skiplist links.
			k := []byte{byte(n - 1 - i)}
			s.insert(ikeyEncode(nil, k, 1, ikeyKindSet), k)
		}
		count := 0
		prev := -1
		for node := s.first(); node != nilNode; node = s.next(node, 0) {
			cur := int(ikeyUserKey(s.key(node))[0])
			assert.Greater(t, cur, prev)
			prev = cur
			count++
		}
		assert.Equal(t, n, count)
	})

	t.Run("value_and_key_isolation", func(t *testing.T) {
		// Keys and values of different lengths must round-trip exactly from the
		// packed arena layout.
		s := newSkiplist(6)
		s.insert(ikeyEncode(nil, []byte("short"), 1, ikeyKindSet), []byte("v"))
		s.insert(ikeyEncode(nil, []byte("a-much-longer-key-value"), 2, ikeyKindSet), []byte("longer-value-here"))
		s.insert(ikeyEncode(nil, []byte("empty-val"), 3, ikeyKindDelete), nil)

		n := s.seek(ik("a-much-longer-key-value", 2))
		require.NotEqual(t, uint32(nilNode), n)
		assert.Equal(t, "a-much-longer-key-value", string(ikeyUserKey(s.key(n))))
		assert.Equal(t, []byte("longer-value-here"), s.value(n))
	})
}
