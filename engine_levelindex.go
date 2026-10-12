package levisdb

import (
	"bytes"
	"sort"
)

// levelIndex is a read-path acceleration structure over the engine's live tables.
// The flat s.tables slice remains the authority for lifecycle and iteration; this
// index is rebuilt from it at every mutation and lets a point lookup visit only the
// tables whose key range can contain the key, instead of scanning all of them.
//
// Each level below L0 is sorted by minKey. When a level's tables are disjoint (no
// two key ranges overlap) a lookup binary-searches to the single candidate --
// O(log n) per level instead of O(n) over every table, which is what keeps a
// multi-terabyte store (hundreds of thousands of tables) readable. Size-tiered
// compaction does NOT guarantee disjoint tables at a level: two compaction outputs
// can both land at one depth and overlap. Such a level is flagged non-disjoint at
// build time and scanned whole, so correctness never depends on the disjoint
// assumption; only the fast path does. L0 is always overlapping (fresh flushes),
// so it is kept as an unsorted bucket and scanned whole; write backpressure bounds
// its size.
type levelIndex struct {
	l0       []*tableMeta   // fresh tier, may overlap; scanned whole
	sorted   [][]*tableMeta // per depth >= 1, sorted by minKey
	disjoint []bool         // per depth >= 1: true when the level has no overlapping ranges
}

// buildLevelIndex groups tables by depth, sorts each level >= 1 by minKey, and
// records whether each level is disjoint. A table with an unknown (nil) minKey or
// maxKey makes its level non-disjoint (unknown bounds conservatively overlap
// everything), so it is scanned whole.
func buildLevelIndex(tables []*tableMeta) *levelIndex {
	idx := &levelIndex{}
	maxDepth := 0
	for _, t := range tables {
		if t.depth > maxDepth {
			maxDepth = t.depth
		}
	}
	idx.sorted = make([][]*tableMeta, maxDepth+1)
	idx.disjoint = make([]bool, maxDepth+1)
	for _, t := range tables {
		if t.depth == 0 {
			idx.l0 = append(idx.l0, t)
			continue
		}
		idx.sorted[t.depth] = append(idx.sorted[t.depth], t)
	}
	for d := 1; d <= maxDepth; d++ {
		lvl := idx.sorted[d]
		sort.Slice(lvl, func(i, j int) bool {
			return compareOptionalMin(lvl[i].minKey, lvl[j].minKey) < 0
		})
		idx.disjoint[d] = levelIsDisjoint(lvl)
	}
	return idx
}

// levelIsDisjoint reports whether a minKey-sorted level has no overlapping key
// ranges. Any unknown (nil) bound overlaps everything, so a level containing one
// is not disjoint. Otherwise each table must start strictly after the previous
// table's maxKey.
func levelIsDisjoint(lvl []*tableMeta) bool {
	var prevMax []byte
	for i, t := range lvl {
		if t.minKey == nil || t.maxKey == nil {
			return false
		}
		if i > 0 && bytes.Compare(t.minKey, prevMax) <= 0 {
			return false
		}
		prevMax = t.maxKey
	}
	return true
}

// candidates calls visit for every table whose recorded key range may contain key:
// all of L0, the single binary-searched candidate in a disjoint level, or every
// matching table in a non-disjoint level. visit returns an error to stop early.
// Order is not significant -- the caller resolves the newest visible version
// across all candidates.
func (idx *levelIndex) candidates(key []byte, visit func(*tableMeta) error) error {
	for _, t := range idx.l0 {
		if t.mayContain(key) {
			if err := visit(t); err != nil {
				return err
			}
		}
	}
	for d := 1; d < len(idx.sorted); d++ {
		lvl := idx.sorted[d]
		if len(lvl) == 0 {
			continue
		}
		if !idx.disjoint[d] {
			// Overlapping or unknown-bound level: scan it whole.
			for _, t := range lvl {
				if t.mayContain(key) {
					if err := visit(t); err != nil {
						return err
					}
				}
			}
			continue
		}
		// Disjoint level: the only table that can hold key is the last one whose
		// minKey <= key. Binary-search the cutoff (first minKey > key) and check the
		// table just below it.
		cutoff := sort.Search(len(lvl), func(i int) bool {
			return bytes.Compare(lvl[i].minKey, key) > 0
		})
		if cutoff == 0 {
			continue // every table starts above key
		}
		t := lvl[cutoff-1]
		if t.mayContain(key) {
			if err := visit(t); err != nil {
				return err
			}
		}
	}
	return nil
}
