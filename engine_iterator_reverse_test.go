package levisdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collectReverse drains a reverse engine iterator into key/value string slices.
func collectReverse(it *reverseEngineIterator) (keys, vals []string) {
	for it.Next() {
		keys = append(keys, string(it.Key()))
		vals = append(vals, string(it.Value()))
	}
	return keys, vals
}

// reverseOf returns a copy of s reversed, so a descending result can be checked
// against the ascending expectation without restating it. An empty input yields
// nil so it compares equal to an iterator that produced no keys.
func reverseOf(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	out := make([]string, len(s))
	for i, v := range s {
		out[len(s)-1-i] = v
	}
	return out
}

func TestReverseEngineIteratorMemtableOnly(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	s.Put(1, []byte("a"), []byte("1"))
	s.Put(1, []byte("b"), []byte("2"))
	s.Put(1, []byte("c"), []byte("3"))
	s.Put(1, []byte("d"), []byte("4"))
	s.Put(1, []byte("e"), []byte("5"))

	t.Run("full descending scan is the forward scan reversed", func(t *testing.T) {
		fKeys, fVals := collect(s.NewIterator(10))
		rKeys, rVals := collectReverse(s.NewReverseIterator(10))
		assert.Equal(t, reverseOf(fKeys), rKeys)
		assert.Equal(t, reverseOf(fVals), rVals)
		assert.Equal(t, []string{"e", "d", "c", "b", "a"}, rKeys)
	})

	t.Run("range is half-open [start,end) in descending order", func(t *testing.T) {
		rKeys, _ := collectReverse(s.NewReverseRangeIterator(10, []byte("b"), []byte("e")))
		assert.Equal(t, []string{"d", "c", "b"}, rKeys)
	})

	t.Run("start bound is inclusive, end exclusive", func(t *testing.T) {
		rKeys, _ := collectReverse(s.NewReverseRangeIterator(10, []byte("a"), []byte("c")))
		assert.Equal(t, []string{"b", "a"}, rKeys)
	})
}

func TestReverseEngineIteratorVersionsAndDeletes(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	s.Put(1, []byte("k"), []byte("v1"))
	s.Put(3, []byte("k"), []byte("v3"))
	s.Put(2, []byte("gone"), []byte("x"))
	s.del(4, []byte("gone"))

	t.Run("newest visible version per key, tombstone skipped", func(t *testing.T) {
		rKeys, rVals := collectReverse(s.NewReverseIterator(10))
		assert.Equal(t, []string{"k"}, rKeys)
		assert.Equal(t, []string{"v3"}, rVals)
	})

	t.Run("old snapshot sees older versions in descending order", func(t *testing.T) {
		rKeys, rVals := collectReverse(s.NewReverseIterator(2))
		assert.Equal(t, []string{"k", "gone"}, rKeys)
		assert.Equal(t, []string{"v1", "x"}, rVals)
	})
}

func TestReverseEngineIteratorMatchesForwardReversed(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	for i := 0; i < 200; i++ {
		s.Put(uint64(i+1), []byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	// Overwrite and delete a few to exercise version resolution.
	s.Put(1000, []byte("k050"), []byte("new50"))
	s.del(1001, []byte("k100"))

	fKeys, fVals := collect(s.NewIterator(2000))
	rKeys, rVals := collectReverse(s.NewReverseIterator(2000))
	assert.Equal(t, reverseOf(fKeys), rKeys)
	assert.Equal(t, reverseOf(fVals), rVals)
}

func TestReverseEngineIteratorAcrossImmutable(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	s.Put(1, []byte("b"), []byte("2"))
	s.Put(1, []byte("d"), []byte("4"))
	require.True(t, s.rotate(), "active memtable sealed into imm")
	s.Put(2, []byte("a"), []byte("1"))
	s.Put(2, []byte("c"), []byte("3"))
	s.Put(3, []byte("b"), []byte("new")) // overwrite a key living in imm

	rKeys, rVals := collectReverse(s.NewReverseIterator(10))
	assert.Equal(t, []string{"d", "c", "b", "a"}, rKeys)
	assert.Equal(t, []string{"4", "3", "new", "1"}, rVals, "active memtable shadows imm")
}

func TestReverseEngineIteratorAcrossTables(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	// b, d, f land in a first flushed table; a, c in a second; e, g in the memtable.
	s.Put(1, []byte("b"), []byte("2"))
	s.Put(1, []byte("d"), []byte("4"))
	s.Put(1, []byte("f"), []byte("6"))
	require.NoError(t, s.Flush())
	s.Put(2, []byte("a"), []byte("1"))
	s.Put(2, []byte("c"), []byte("3"))
	require.NoError(t, s.Flush())
	s.Put(3, []byte("e"), []byte("5"))
	s.Put(3, []byte("g"), []byte("7"))
	// Overwrite a flushed key from the memtable to exercise cross-source resolution.
	s.Put(4, []byte("b"), []byte("new"))

	t.Run("full descending scan across tables and memtable", func(t *testing.T) {
		fKeys, fVals := collect(s.NewIterator(10))
		rKeys, rVals := collectReverse(s.NewReverseIterator(10))
		assert.Equal(t, reverseOf(fKeys), rKeys)
		assert.Equal(t, reverseOf(fVals), rVals)
		assert.Equal(t, []string{"g", "f", "e", "d", "c", "b", "a"}, rKeys)
		assert.Equal(t, "new", rVals[5], "memtable overwrite of a flushed key wins")
	})

	t.Run("descending range spanning tables", func(t *testing.T) {
		rKeys, _ := collectReverse(s.NewReverseRangeIterator(10, []byte("c"), []byte("g")))
		assert.Equal(t, []string{"f", "e", "d", "c"}, rKeys)
	})
}

func TestReverseEngineIteratorTTL(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	future := time.Now().Add(time.Hour).Unix()
	past := time.Now().Add(-time.Hour).Unix()
	s.Put(1, []byte("a"), []byte("1"))
	s.putTTL(2, []byte("b"), []byte("live"), future)
	s.putTTL(3, []byte("c"), []byte("dead"), past)
	s.Put(4, []byte("d"), []byte("4"))

	it := s.NewReverseIterator(10)
	var got []struct {
		key, val string
		exp      int64
	}
	for it.Next() {
		got = append(got, struct {
			key, val string
			exp      int64
		}{string(it.Key()), string(it.Value()), it.valueExpiresAt()})
	}
	require.NoError(t, it.Error())
	// "c" is expired and must be absent; the rest come back descending.
	require.Len(t, got, 3)
	assert.Equal(t, "d", got[0].key)
	assert.Equal(t, "b", got[1].key)
	assert.Equal(t, future, got[1].exp, "live TTL surfaces its deadline")
	assert.Equal(t, "a", got[2].key)
	assert.Zero(t, got[2].exp, "non-TTL value reports zero expiry")
}

func TestReverseEngineIteratorRangeTombstone(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		s.Put(1, []byte(k), []byte(k))
	}
	s.delRange(2, []byte("b"), []byte("d")) // deletes b, c (half-open)

	rKeys, _ := collectReverse(s.NewReverseIterator(10))
	assert.Equal(t, []string{"e", "d", "a"}, rKeys, "range tombstone shadows b and c")
}

func TestReverseEngineIteratorRangeTombstoneAcrossTables(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		s.Put(1, []byte(k), []byte(k))
	}
	require.NoError(t, s.Flush()) // keys live in a table now
	s.delRange(2, []byte("b"), []byte("d"))
	require.NoError(t, s.Flush()) // range tombstone in a second table

	rKeys, _ := collectReverse(s.NewReverseIterator(10))
	assert.Equal(t, []string{"e", "d", "a"}, rKeys, "cross-table range tombstone shadows b and c")
}

func TestReverseEngineIteratorRangeTombstoneSnapshot(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	for _, k := range []string{"a", "b", "c"} {
		s.Put(1, []byte(k), []byte(k))
	}
	s.delRange(5, []byte("a"), []byte("z")) // deletes everything at seq 5

	t.Run("snapshot before the range delete still sees all keys", func(t *testing.T) {
		rKeys, _ := collectReverse(s.NewReverseIterator(3))
		assert.Equal(t, []string{"c", "b", "a"}, rKeys)
	})
	t.Run("snapshot after the range delete sees nothing", func(t *testing.T) {
		rKeys, _ := collectReverse(s.NewReverseIterator(10))
		assert.Empty(t, rKeys)
	})
}

func FuzzReverseEngineIteratorMatchesForward(f *testing.F) {
	f.Add([]byte{0x01, 0x05, 0x02, 0x81, 0x03, 0x00})
	f.Add([]byte{0x01, 0x02, 0xff, 0x03, 0x81, 0xff, 0x04})
	f.Fuzz(func(t *testing.T, ops []byte) {
		s := newTestEngine(t, 1<<20)
		seq := uint64(0)
		wrote := false
		// Each op is a byte. 0xff flushes (so on-disk tables join the merge);
		// otherwise the high bit = delete and the low 7 bits = key id (0..127).
		for _, op := range ops {
			if op == 0xff {
				_ = s.Flush() // no-op when the memtable is empty
				continue
			}
			seq++
			key := []byte{op & 0x7f}
			if op&0x80 != 0 {
				s.del(seq, key)
			} else {
				s.Put(seq, key, []byte{byte(seq)})
			}
			wrote = true
		}
		if !wrote {
			return
		}
		snap := seq + 1
		fKeys, fVals := collect(s.NewIterator(snap))
		rKeys, rVals := collectReverse(s.NewReverseIterator(snap))
		assert.Equal(t, reverseOf(fKeys), rKeys)
		assert.Equal(t, reverseOf(fVals), rVals)
	})
}

func BenchmarkReverseEngineIteratorNext(b *testing.B) {
	s := newTestEngine(b, 1<<24)
	for i := 0; i < 10000; i++ {
		s.Put(uint64(i+1), []byte(fmt.Sprintf("k%08d", i)), []byte("v"))
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		it := s.NewReverseIterator(1 << 30)
		for it.Next() {
			_ = it.Key()
			_ = it.Value()
		}
	}
}
