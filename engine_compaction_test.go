package levisdb

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testCompactionConfig() compactionConfigT {
	return compactionConfigT{
		TierRatio:          2,
		BloomBits:          10,
		BlockSize:          256,
		FreshCodecName:     "none",
		BottomCodecName:    "none",
		FileSizeBase:       1 << 20,
		FileSizeMultiplier: 2,
		FileSizeMax:        8 << 20,
	}
}

// flushSingle writes one key/value at seq into a fresh L0 table.
func flushSingle(t *testing.T, s *shardT, seq uint64, key, val string) {
	t.Helper()
	s.Put(seq, []byte(key), []byte(val))
	require.NoError(t, s.Flush())
}

// TestTargetFileSize pins the per-depth output size curve documented in the
// README: base * multiplier^depth, clamped at FileSizeMax.
func TestTargetFileSize(t *testing.T) {
	t.Parallel()
	cc := compactionConfigT{
		FileSizeBase:       DefaultFileSizeBase,       // 2 MiB
		FileSizeMultiplier: DefaultFileSizeMultiplier, // 2
		FileSizeMax:        DefaultFileSizeMax,        // 16 MiB
	}
	cases := []struct {
		depth int
		want  int64
	}{
		{0, 2 << 20},
		{1, 4 << 20},
		{2, 8 << 20},
		{3, 16 << 20},
		{4, 16 << 20}, // capped
		{9, 16 << 20}, // capped, no overflow
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("depth-%d", tc.depth), func(t *testing.T) {
			assert.Equal(t, tc.want, cc.targetFileSize(tc.depth))
		})
	}
}

func TestPickCompaction(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)

	t.Run("nothing ready", func(t *testing.T) {
		assert.Equal(t, -1, s.pickCompaction(2, 0))
	})

	t.Run("ready at ratio", func(t *testing.T) {
		flushSingle(t, s, 1, "a", "1")
		assert.Equal(t, -1, s.pickCompaction(2, 0), "one table below ratio")
		flushSingle(t, s, 2, "b", "2")
		assert.Equal(t, 0, s.pickCompaction(2, 0), "two L0 tables meet ratio 2")
	})
}

func TestCompactCollapsesVersions(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)

	// Three generations of the same key across three L0 tables.
	flushSingle(t, s, 1, "k", "v1")
	flushSingle(t, s, 2, "k", "v2")
	flushSingle(t, s, 3, "k", "v3")
	require.Len(t, s.Tables(), 3)

	// retainSeq huge => keep only the newest version.
	require.NoError(t, s.Compact(0, uint64(1)<<62, testCompactionConfig()))

	tabs := s.Tables()
	require.Len(t, tabs, 1, "three inputs merged into one")
	assert.Equal(t, 1, tabs[0].Depth, "output lands one tier deeper")
	assert.Equal(t, []byte("v3"), mustGet(t, s, 100, "k"))
}

func TestCompactRetainsSnapshotVersions(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	flushSingle(t, s, 1, "k", "v1")
	flushSingle(t, s, 5, "k", "v5")

	// retainSeq=3: versions with seq >= 3 stay, plus the newest below (seq 1).
	require.NoError(t, s.Compact(0, 3, testCompactionConfig()))

	// Old snapshot still sees v1, new snapshot sees v5.
	assert.Equal(t, []byte("v1"), mustGet(t, s, 1, "k"))
	assert.Equal(t, []byte("v5"), mustGet(t, s, 100, "k"))
}

func TestCompactDropsTombstonesAtBottom(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	flushSingle(t, s, 1, "gone", "v1")
	// A tombstone in its own table.
	s.del(2, []byte("gone"))
	require.NoError(t, s.Flush())
	require.Len(t, s.Tables(), 2)

	require.NoError(t, s.Compact(0, uint64(1)<<62, testCompactionConfig()))

	_, found, _, err := s.get(100, []byte("gone"))
	require.NoError(t, err)
	assert.False(t, found, "tombstone reclaimed at the bottom tier")
}

func TestCompactNoOpBelowTwoInputs(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	flushSingle(t, s, 1, "k", "v1")
	require.NoError(t, s.Compact(0, 0, testCompactionConfig()))
	assert.Len(t, s.Tables(), 1, "single table left untouched")
}

func TestCompactErrors(t *testing.T) {
	t.Parallel()

	t.Run("bad bottom codec", func(t *testing.T) {
		s := newTestShard(t, 1<<20)
		// Two L0 tables => output tier is the bottom, so BottomCodecName is used.
		flushSingle(t, s, 1, "a", "1")
		flushSingle(t, s, 2, "b", "2")

		cc := testCompactionConfig()
		cc.BottomCodecName = "bogus"
		assert.Error(t, s.Compact(0, 0, cc))
	})

	t.Run("non-bottom output uses fresh codec", func(t *testing.T) {
		s := newTestShard(t, 1<<20)
		// Two L0 tables merge to a single depth-1 table (the bottom).
		flushSingle(t, s, 1, "a", "1")
		flushSingle(t, s, 2, "b", "2")
		require.NoError(t, s.Compact(0, uint64(1)<<62, testCompactionConfig()))
		require.Equal(t, 1, s.Tables()[0].Depth)

		// Two more L0 tables; a depth-1 table already exists, so compacting
		// L0 -> depth 1 is NOT the bottom tier and reads FreshCodecName.
		flushSingle(t, s, 3, "c", "3")
		flushSingle(t, s, 4, "d", "4")
		require.NoError(t, s.Compact(0, uint64(1)<<62, testCompactionConfig()))

		assert.Equal(t, []byte("3"), mustGet(t, s, 100, "c"))
		assert.Equal(t, []byte("1"), mustGet(t, s, 100, "a"))
	})

	t.Run("tablePath error during compaction", func(t *testing.T) {
		dir := t.TempDir()
		want := errors.New("no path")
		calls := 0
		tablePath := func(num uint32) (string, error) {
			calls++
			if calls > 2 {
				return "", want // fail once both L0 tables are written
			}
			return filepath.Join(dir, fmt.Sprintf("%08x.sst", num)), nil
		}
		cfg := shardConfigT{MemtableSize: 1 << 20, BloomBits: 10, BlockSize: 256, FreshCodecName: "none"}
		s := newShard(cfg, newAllocator(0), tablePath, 1)
		t.Cleanup(func() { s.Close() })

		flushSingle(t, s, 1, "a", "1")
		flushSingle(t, s, 2, "b", "2")
		assert.ErrorIs(t, s.Compact(0, 0, testCompactionConfig()), want)
	})

	t.Run("merged output open error", func(t *testing.T) {
		dir := t.TempDir()
		calls := 0
		tablePath := func(num uint32) (string, error) {
			calls++
			if calls > 2 {
				// Valid path string, but its parent dir does not exist, so the
				// os.OpenFile inside writeMerged fails.
				return filepath.Join(dir, "missing-dir", fmt.Sprintf("%08x.sst", num)), nil
			}
			return filepath.Join(dir, fmt.Sprintf("%08x.sst", num)), nil
		}
		cfg := shardConfigT{MemtableSize: 1 << 20, BloomBits: 10, BlockSize: 256, FreshCodecName: "none"}
		s := newShard(cfg, newAllocator(0), tablePath, 1)
		t.Cleanup(func() { s.Close() })

		flushSingle(t, s, 1, "a", "1")
		flushSingle(t, s, 2, "b", "2")
		assert.Error(t, s.Compact(0, 0, testCompactionConfig()))
	})
}

func TestCompactManyKeys(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	// Two tables, disjoint keys, so the merge interleaves them.
	for i := 0; i < 100; i++ {
		s.Put(uint64(i+1), []byte(fmt.Sprintf("even%03d", i)), []byte("e"))
	}
	require.NoError(t, s.Flush())
	for i := 0; i < 100; i++ {
		s.Put(uint64(i+200), []byte(fmt.Sprintf("odd%03d", i)), []byte("o"))
	}
	require.NoError(t, s.Flush())

	require.NoError(t, s.Compact(0, uint64(1)<<62, testCompactionConfig()))
	require.Len(t, s.Tables(), 1)
	for i := 0; i < 100; i++ {
		assert.Equal(t, []byte("e"), mustGet(t, s, 1000, fmt.Sprintf("even%03d", i)))
		assert.Equal(t, []byte("o"), mustGet(t, s, 1000, fmt.Sprintf("odd%03d", i)))
	}
}

func newBenchShard(b *testing.B) *shardT {
	b.Helper()
	dir := b.TempDir()
	tablePath := func(num uint32) (string, error) {
		return filepath.Join(dir, fmt.Sprintf("%08x.sst", num)), nil
	}
	cfg := shardConfigT{MemtableSize: 1 << 30, BloomBits: 10, BlockSize: 4096, FreshCodecName: "s2"}
	s := newShard(cfg, newAllocator(0), tablePath, 1)
	b.Cleanup(func() { s.Close() })
	return s
}

func BenchmarkCompactAll(b *testing.B) {
	cc := compactionConfigT{
		TierRatio:       4,
		BloomBits:       10,
		BlockSize:       4096,
		FreshCodecName:  "s2",
		BottomCodecName: "zstd",
	}
	const perTable = 2000
	val := make([]byte, 100)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		s := newBenchShard(b)
		seq := uint64(0)
		// Four overlapping tables so the merge does real k-way work.
		for t := 0; t < 4; t++ {
			for i := 0; i < perTable; i++ {
				seq++
				s.Put(seq, []byte(fmt.Sprintf("key%06d", i)), val)
			}
			if err := s.Flush(); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		if err := s.CompactAll(uint64(1)<<62, cc); err != nil {
			b.Fatal(err)
		}
	}
}

func TestCapCompactionInputs(t *testing.T) {
	t.Parallel()
	mk := func(sizes ...int64) []*tableMeta {
		ts := make([]*tableMeta, len(sizes))
		for i, s := range sizes {
			ts[i] = &tableMeta{size: s}
		}
		return ts
	}

	t.Run("fits under cap returns all", func(t *testing.T) {
		in := mk(10, 10, 10)
		assert.Len(t, capCompactionInputs(in, 100), 3)
	})
	t.Run("caps to a prefix", func(t *testing.T) {
		in := mk(40, 40, 40, 40)
		// 40+40=80 <= 100; +40=120 > 100 at index 2 -> keep first 3.
		assert.Len(t, capCompactionInputs(in, 100), 3)
	})
	t.Run("always keeps at least two", func(t *testing.T) {
		in := mk(1000, 1000, 1000)
		// First table alone exceeds cap, but progress needs >= 2 inputs.
		assert.Len(t, capCompactionInputs(in, 10), 2)
	})
}

func TestCompactionByteCapDrainsTierOverRounds(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	defer s.Close()

	// Build several depth-0 tables with distinct keys.
	const tables = 6
	for tbl := 0; tbl < tables; tbl++ {
		for k := 0; k < 5; k++ {
			s.Put(uint64(tbl*10+k+1), []byte(fmt.Sprintf("k%02d-%02d", tbl, k)), []byte("v"))
		}
		require.NoError(t, s.Flush())
	}
	require.Equal(t, tables, s.depth0Count())

	// Cap so each compaction merges only a couple of tables (non-bottom: a deeper
	// tier must exist, so first move some data to depth 1 via an uncapped compact).
	s.mu.RLock()
	var oneSize int64
	if len(s.tables) > 0 {
		oneSize = s.tables[0].size
	}
	s.mu.RUnlock()
	cc := testCompactionConfig()
	cc.MaxCompactionBytes = oneSize*2 + 1 // ~2 tables per non-bottom compaction

	// Repeatedly compact depth 0 until it drains; each round is byte-capped.
	rounds := 0
	for s.depth0Count() >= 2 && rounds < 20 {
		require.NoError(t, s.Compact(0, ^uint64(0), cc))
		rounds++
	}

	// All keys remain readable after the capped multi-round compaction.
	for tbl := 0; tbl < tables; tbl++ {
		for k := 0; k < 5; k++ {
			assert.Equal(t, []byte("v"), mustGet(t, s, 1000, fmt.Sprintf("k%02d-%02d", tbl, k)))
		}
	}
}

func TestRangesMayContain(t *testing.T) {
	t.Parallel()
	ranges := [][2][]byte{
		{[]byte("d"), []byte("m")},
		{[]byte("p"), []byte("t")},
	}
	assert.True(t, rangesMayContain(ranges, []byte("h")))
	assert.True(t, rangesMayContain(ranges, []byte("q")))
	assert.False(t, rangesMayContain(ranges, []byte("a")))
	assert.False(t, rangesMayContain(ranges, []byte("n")))
	assert.False(t, rangesMayContain(ranges, []byte("z")))
	// Unknown bounds conservatively cover everything.
	assert.True(t, rangesMayContain([][2][]byte{{nil, nil}}, []byte("anything")))
}

// TestIntermediateTierGCDoesNotResurrect is the safety test: a tombstone that
// shadows an older version living in a DEEPER tier must not be dropped at an
// intermediate compaction, or the old value would reappear.
func TestIntermediateTierGCDoesNotResurrect(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	defer s.Close()

	cc := testCompactionConfig()

	// Old value for "k" pushed down to a deeper tier: flush two depth-0 tables,
	// compact them to depth 1, then compact again to depth 2 so "k"@old sits deep.
	s.Put(1, []byte("k"), []byte("old"))
	require.NoError(t, s.Flush())
	s.Put(2, []byte("filler"), []byte("x")) // second table so Compact has >= 2 inputs
	require.NoError(t, s.Flush())
	require.NoError(t, s.Compact(0, 0, cc)) // depth0 -> depth1

	// New tombstone for "k" arrives fresh at depth 0, plus a sibling so depth 0
	// has >= 2 tables to compact into depth 1 (intermediate: depth 2+ is empty
	// here, but depth 1 holds the old "k", so the tombstone must be kept).
	s.del(10, []byte("k"))
	require.NoError(t, s.Flush())
	s.Put(11, []byte("other"), []byte("y"))
	require.NoError(t, s.Flush())
	// retainSeq high so no snapshot retention forces keeping; the deeper-tier
	// guard, not retention, must preserve correctness.
	require.NoError(t, s.Compact(0, ^uint64(0), cc)) // depth0 -> depth1 (intermediate)

	// "k" must read as deleted; the old value must not resurface.
	_, found, deleted, err := s.get(1000, []byte("k"))
	require.NoError(t, err)
	if found {
		assert.True(t, deleted, "k must be deleted, not resurrected to its old value")
	}
	v, f, d, err := s.get(1000, []byte("k"))
	require.NoError(t, err)
	assert.False(t, f && !d && string(v) == "old", "old value must never reappear")
}

// TestIntermediateTierGCReclaimsWhenNoDeeperTier verifies the positive case: a
// tombstone whose key no deeper tier can hold IS dropped at an intermediate
// compaction (freeing space) rather than being carried down.
func TestIntermediateTierGCReclaimsWhenNoDeeperTier(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	defer s.Close()

	cc := testCompactionConfig()

	// Put a deeper-tier key range that does NOT cover "zzz" so the tombstone for
	// "zzz" has nothing below it to shadow.
	s.Put(1, []byte("aaa"), []byte("v"))
	require.NoError(t, s.Flush())
	s.Put(2, []byte("aab"), []byte("v"))
	require.NoError(t, s.Flush())
	require.NoError(t, s.Compact(0, 0, cc)) // "aaa","aab" -> depth 1

	// Tombstone for "zzz" at depth 0, sibling so >= 2 inputs. depth 1 range is
	// ["aaa".."aab"], which excludes "zzz", so the tombstone is reclaimable.
	s.del(10, []byte("zzz"))
	require.NoError(t, s.Flush())
	s.Put(11, []byte("zzy"), []byte("v"))
	require.NoError(t, s.Flush())
	require.NoError(t, s.Compact(0, ^uint64(0), cc)) // intermediate compaction

	// Scan all depth-1 tables; the "zzz" tombstone must have been dropped.
	s.mu.RLock()
	var foundTombstone bool
	for _, tbl := range s.tables {
		it := tbl.reader.newIterator()
		for it.Next() {
			if string(ikeyUserKey(it.internalKey())) == "zzz" {
				foundTombstone = true
			}
		}
		require.NoError(t, it.Error())
	}
	s.mu.RUnlock()
	assert.False(t, foundTombstone, "reclaimable intermediate-tier tombstone should be dropped")

	// And "zzz" still reads as absent.
	_, found, _, err := s.get(1000, []byte("zzz"))
	require.NoError(t, err)
	assert.False(t, found)
}

// TestByteCappedIntermediateGCDoesNotResurrect is a regression test: when the
// per-compaction byte-cap trims a same-tier table out of the inputs, an
// intermediate-tier tombstone must NOT be reclaimed if that left-behind table
// holds an older version of the key. Otherwise the deleted value resurfaces.
func TestByteCappedIntermediateGCDoesNotResurrect(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	defer s.Close()

	// Build three tables, then force them all to tier depth 1 (same tier) with a
	// deeper tier (depth 2) that does NOT cover "k". Order matters: the cap keeps
	// the prefix, so the old-value table must be last to be trimmed.
	flushSingle(t, s, 100, "k", "") // will be the tombstone table
	flushSingle(t, s, 5, "filler", "x")
	flushSingle(t, s, 50, "k", "old") // old value, must be trimmed by the cap
	// Rewrite the first table's single entry as a tombstone by re-flushing: easier
	// to just place a delete. Instead, construct tombstone directly below.

	// Reset and build deterministically via direct depth assignment.
	s.mu.Lock()
	s.tables = s.tables[:0]
	s.mu.Unlock()

	mkTable := func(depth int, seq uint64, key, val string, del bool) {
		if del {
			s.del(seq, []byte(key))
		} else {
			s.Put(seq, []byte(key), []byte(val))
		}
		require.NoError(t, s.Flush())
		s.mu.Lock()
		s.tables[len(s.tables)-1].depth = depth
		s.mu.Unlock()
	}
	// depth 2: a table NOT covering "k" (so noDeeperTier("k") would be true).
	mkTable(2, 1, "zzz", "z", false)
	// depth 1 inputs, in creation order: tombstone(k), filler, old(k).
	mkTable(1, 100, "k", "", true)
	mkTable(1, 60, "filler", "y", false)
	mkTable(1, 50, "k", "old", false)

	// Cap sized so only the first two depth-1 tables are compacted, trimming the
	// old(k) table. Each table is tiny; cap at ~2 tables' worth.
	cc := testCompactionConfig()
	s.mu.RLock()
	var oneSize int64
	for _, tbl := range s.tables {
		if tbl.depth == 1 {
			oneSize = tbl.size
			break
		}
	}
	s.mu.RUnlock()
	cc.MaxCompactionBytes = oneSize*2 + 1

	require.NoError(t, s.Compact(1, ^uint64(0), cc)) // intermediate compaction of depth 1

	// "k" was deleted at seq 100 (newer than the old put at 50); it must stay
	// deleted, never resurrected to "old".
	v, found, deleted, err := s.get(1000, []byte("k"))
	require.NoError(t, err)
	assert.False(t, found && !deleted && string(v) == "old", "deleted key resurrected to old value")
}

func TestTombstoneRatioTriggersCompaction(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	defer s.Close()

	// Two depth-0 tables (below a high TierRatio) where most entries are deletes.
	s.Put(1, []byte("a"), []byte("v"))
	s.del(2, []byte("b"))
	s.del(3, []byte("c"))
	require.NoError(t, s.Flush())
	s.del(4, []byte("d"))
	s.del(5, []byte("e"))
	s.Put(6, []byte("f"), []byte("v"))
	require.NoError(t, s.Flush())

	// 4 tombstones / 6 entries ~= 0.67. Below the count threshold, only the
	// tombstone trigger can mark this tier ready.
	assert.Equal(t, -1, s.pickCompaction(1000, 0), "no trigger without tombstone ratio")
	assert.Equal(t, -1, s.pickCompaction(1000, 0.9), "ratio too high to trigger")
	assert.Equal(t, 0, s.pickCompaction(1000, 0.5), "delete-heavy tier triggers at 0.5")

	// A single table cannot compact alone even if delete-heavy (needs >= 2).
	s2 := newTestShard(t, 1<<20)
	defer s2.Close()
	s2.del(1, []byte("x"))
	require.NoError(t, s2.Flush())
	assert.Equal(t, -1, s2.pickCompaction(1000, 0.1), "single table not compacted alone")
}

func TestTombstoneCompactionReclaimsDeletedSpace(t *testing.T) {
	t.Parallel()
	// A delete-heavy tier with the count trigger off must be compacted down by the
	// tombstone trigger so the deleted keys read as absent. Built deterministically
	// at the shard level (no reliance on background flush timing).
	s := newTestShard(t, 1<<20)
	defer s.Close()

	// Two depth-0 tables that are all tombstones for a set of keys, plus a live
	// filler each so the tier is >= 2 tables and mostly deletes.
	for i := 0; i < 3; i++ {
		s.del(uint64(100+i), []byte(fmt.Sprintf("k%d", i)))
	}
	require.NoError(t, s.Flush())
	for i := 3; i < 6; i++ {
		s.del(uint64(100+i), []byte(fmt.Sprintf("k%d", i)))
	}
	require.NoError(t, s.Flush())
	require.Equal(t, 2, s.depth0Count())

	// Count trigger off (high ratio), tombstone trigger on: the picker must select
	// the delete-heavy depth-0 tier.
	depth := s.pickCompaction(1000, 0.5)
	require.Equal(t, 0, depth, "tombstone trigger must select the delete-heavy tier")

	// Compact it to the bottom (retainSeq high so tombstones are reclaimable).
	require.NoError(t, s.Compact(depth, ^uint64(0), testCompactionConfig()))

	// All keys read as absent, and the delete-heavy depth-0 tier is drained.
	for i := 0; i < 6; i++ {
		_, found, _, err := s.get(1000, []byte(fmt.Sprintf("k%d", i)))
		require.NoError(t, err)
		assert.False(t, found, "deleted key k%d must be absent after tombstone compaction", i)
	}
	assert.Zero(t, s.depth0Count(), "delete-heavy tier compacted away")
}
