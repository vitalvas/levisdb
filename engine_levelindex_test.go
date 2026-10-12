package levisdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// tbl builds a metadata-only table stub at depth with the given user-key bounds.
// nil bounds mean "unknown" and conservatively overlap everything.
func idxTbl(depth int, minKey, maxKey string) *tableMeta {
	var mn, mx []byte
	if minKey != "" {
		mn = []byte(minKey)
	}
	if maxKey != "" {
		mx = []byte(maxKey)
	}
	return &tableMeta{depth: depth, minKey: mn, maxKey: mx}
}

// collect returns the set of tables candidates visits for key.
func idxCollect(idx *levelIndex, key string) map[*tableMeta]bool {
	seen := map[*tableMeta]bool{}
	_ = idx.candidates([]byte(key), func(t *tableMeta) error {
		seen[t] = true
		return nil
	})
	return seen
}

func TestLevelIndexCandidates(t *testing.T) {
	t.Parallel()

	t.Run("disjoint level visits only the covering table", func(t *testing.T) {
		a := idxTbl(1, "a", "c")
		b := idxTbl(1, "e", "g")
		c := idxTbl(1, "m", "q")
		idx := buildLevelIndex([]*tableMeta{a, b, c})
		assert.True(t, idx.disjoint[1], "non-overlapping ranges are disjoint")

		assert.Equal(t, map[*tableMeta]bool{b: true}, idxCollect(idx, "f"), "only b covers f")
		assert.Equal(t, map[*tableMeta]bool{a: true}, idxCollect(idx, "a"), "boundary min")
		assert.Equal(t, map[*tableMeta]bool{c: true}, idxCollect(idx, "q"), "boundary max")
		assert.Empty(t, idxCollect(idx, "d"), "gap between tables")
		assert.Empty(t, idxCollect(idx, "z"), "above every table")
	})

	t.Run("overlapping level visits every covering table", func(t *testing.T) {
		// Two tables at one depth both cover "y": the index must not binary-search to
		// a single candidate here (size-tiered compaction can produce this).
		wide := idxTbl(1, "a", "z")
		narrow := idxTbl(1, "x", "x")
		idx := buildLevelIndex([]*tableMeta{wide, narrow})
		assert.False(t, idx.disjoint[1], "overlapping ranges are not disjoint")
		assert.Equal(t, map[*tableMeta]bool{wide: true, narrow: true}, idxCollect(idx, "x"),
			"both tables covering the key are visited")
		assert.Equal(t, map[*tableMeta]bool{wide: true}, idxCollect(idx, "b"),
			"only the wide table covers b")
	})

	t.Run("unknown bounds force a whole-level scan", func(t *testing.T) {
		open := idxTbl(1, "", "") // nil bounds: covers everything
		other := idxTbl(1, "m", "q")
		idx := buildLevelIndex([]*tableMeta{open, other})
		assert.False(t, idx.disjoint[1], "a nil-bound table makes the level non-disjoint")
		assert.Equal(t, map[*tableMeta]bool{open: true}, idxCollect(idx, "a"),
			"the open table is visited for a key outside the other's range")
	})

	t.Run("l0 is always scanned whole", func(t *testing.T) {
		a := idxTbl(0, "a", "c")
		b := idxTbl(0, "b", "d") // overlaps a, as fresh flushes may
		idx := buildLevelIndex([]*tableMeta{a, b})
		assert.Equal(t, map[*tableMeta]bool{a: true, b: true}, idxCollect(idx, "bb"),
			"overlapping L0 tables both visited")
	})

	t.Run("empty index visits nothing", func(t *testing.T) {
		idx := buildLevelIndex(nil)
		assert.Empty(t, idxCollect(idx, "a"))
	})
}

// TestPickCompactionOverlapGate is the cascade-stopper: a non-bottom tier of
// fully disjoint tables (append-only monotonic keys) must NOT trigger a
// count/byte compaction, because merging disjoint tables reclaims nothing and
// only rewrites data one tier deeper. A tier with real key overlap still fires,
// and disabling the gate restores the unconditional count trigger.
func TestPickCompactionOverlapGate(t *testing.T) {
	t.Parallel()

	stub := func(depth int, minKey, maxKey string) *tableMeta {
		m := idxTbl(depth, minKey, maxKey)
		m.size = 1 << 20
		m.entries = 1000
		return m
	}

	t.Run("disjoint non-bottom tier is not picked", func(t *testing.T) {
		s := newTestEngine(t, 1<<30)
		s.mu.Lock()
		// Four disjoint tables at depth 1 (below the cap): monotonic append-only.
		s.setTables([]*tableMeta{
			stub(1, "a", "b"),
			stub(1, "c", "d"),
			stub(1, "e", "f"),
			stub(1, "g", "h"),
		})
		s.mu.Unlock()
		t.Cleanup(func() { s.mu.Lock(); s.setTables(nil); s.mu.Unlock() })

		assert.Equal(t, -1, s.pickCompaction(4, 0, 0, true),
			"disjoint tables at count ratio are left alone with the gate on")
		assert.Equal(t, -1, s.pickCompaction(4, 1<<10, 0, true),
			"byte trigger is also gated on overlap")
		assert.Equal(t, 1, s.pickCompaction(4, 0, 0, false),
			"disabling the gate restores the unconditional count trigger")
	})

	t.Run("overlapping non-bottom tier is picked", func(t *testing.T) {
		s := newTestEngine(t, 1<<30)
		s.mu.Lock()
		// Two of the four overlap, so there is real work to resolve.
		s.setTables([]*tableMeta{
			stub(1, "a", "m"),
			stub(1, "h", "t"), // overlaps the first
			stub(1, "u", "v"),
			stub(1, "w", "x"),
		})
		s.mu.Unlock()
		t.Cleanup(func() { s.mu.Lock(); s.setTables(nil); s.mu.Unlock() })

		assert.Equal(t, 1, s.pickCompaction(4, 0, 0, true),
			"a tier with overlapping tables still triggers by count")
	})
}
