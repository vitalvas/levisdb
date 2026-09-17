package levisdb

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPickCompactionCapTierIsTerminal guards the fix for the compaction
// depth-runaway (which hung Close forever on large, compressible values). The
// cap tier (maxTierDepth) is terminal: there is nowhere deeper to push, so
// count/byte triggers there must NOT fire - re-merging its distinct data
// reclaims nothing and loops forever. Only a tombstone-heavy cap tier is picked.
// Below the cap, the count trigger still fires normally.
func TestPickCompactionCapTierIsTerminal(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<30)
	// Stub TierRatio (=8) tables at the cap tier with distinct data, no tombstones.
	// These are metadata-only stubs (no file handle); clear them before the engine's
	// Cleanup Close so it does not try to release a nil handle.
	s.mu.Lock()
	for i := 0; i < 8; i++ {
		s.tables = append(s.tables, &tableMeta{depth: maxTierDepth, size: 1 << 20, entries: 1000})
	}
	s.mu.Unlock()
	t.Cleanup(func() { s.mu.Lock(); s.tables = nil; s.mu.Unlock() })
	assert.Equal(t, -1, s.pickCompaction(4, 1<<10, 0),
		"cap tier must not be picked by count or bytes (terminal, would loop)")

	// A cap tier that is tombstone-heavy IS worth compacting (it shrinks).
	s.mu.Lock()
	for _, tb := range s.tables {
		tb.tombstones = tb.entries // all deletes
	}
	s.mu.Unlock()
	assert.Equal(t, maxTierDepth, s.pickCompaction(4, 0, 0.5),
		"tombstone-heavy cap tier is still reclaimed")

	// A tier BELOW the cap with >= ratio tables is picked by count as usual.
	s2 := newTestEngine(t, 1<<30)
	s2.mu.Lock()
	for i := 0; i < 4; i++ {
		s2.tables = append(s2.tables, &tableMeta{depth: maxTierDepth - 1, size: 1 << 20, entries: 1000})
	}
	s2.mu.Unlock()
	t.Cleanup(func() { s2.mu.Lock(); s2.tables = nil; s2.mu.Unlock() })
	assert.Equal(t, maxTierDepth-1, s2.pickCompaction(4, 0, 0),
		"a non-cap tier at the count ratio is still picked")
}

// TestCompactOutputDepthCapped verifies the output-depth clamp: compacting a tier
// at (or past) the cap merges IN PLACE at maxTierDepth instead of creating an
// ever-deeper tier. Two real tables are placed at the cap tier via flush+compact,
// then a compaction there must keep every output table at the cap, never deeper.
func TestCompactOutputDepthCapped(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tablePath := func(num uint32) (string, error) {
		return filepath.Join(dir, fmt.Sprintf("%08x.sst", num)), nil
	}
	cfg := engineConfigT{MemtableSize: 1 << 30, BloomBits: 10, BlockSize: 256, FreshCodecName: "none"}
	s := newEngine(cfg, newAllocator(0), tablePath)
	t.Cleanup(func() { s.Close() })

	// Write two real L0 tables, then relocate them to the cap tier with openTable so
	// they are genuine (mergeable) tables sitting AT maxTierDepth.
	flushSingle(t, s, 1, "a", "1")
	flushSingle(t, s, 2, "b", "2")
	s.mu.Lock()
	for _, tb := range s.tables {
		tb.depth = maxTierDepth
	}
	s.mu.Unlock()

	// Compact the cap tier: it must merge in place, output staying at the cap.
	require.NoError(t, s.Compact(maxTierDepth, uint64(1)<<62, testCompactionConfig()))
	s.mu.RLock()
	maxd := 0
	for _, tb := range s.tables {
		if tb.depth > maxd {
			maxd = tb.depth
		}
	}
	s.mu.RUnlock()
	assert.LessOrEqual(t, maxd, maxTierDepth, "compaction must not produce a tier deeper than the cap")
	assert.Equal(t, []byte("1"), mustGet(t, s, 100, "a"))
	assert.Equal(t, []byte("2"), mustGet(t, s, 100, "b"))
}

// TestCompactionRollsOnCompressedSize verifies compaction rolls output on the
// COMPRESSED on-disk size, not raw key+value bytes: highly compressible data
// whose RAW size is many multiples of the target must still fit in ONE output
// table because its compressed size stays under target. Rolling on raw bytes
// would spray many tiny/near-empty SSTs. (LevelDB rolls on Writer.BytesLen, the
// file offset - the same basis.)
func TestCompactionRollsOnCompressedSize(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<30)

	// Two L0 tables of all-zero values: raw bytes far exceed the target, but they
	// compress to almost nothing, so the merged output must be a single table.
	cc := testCompactionConfig()
	cc.FreshCodecName = "s2"
	cc.BottomCodecName = "s2"
	cc.FileSizeBase = 64 << 10 // small target so raw size would force many rolls
	cc.FileSizeMax = 64 << 10
	zero := make([]byte, 2048)
	for f := 0; f < 2; f++ {
		for i := 0; i < 500; i++ { // 500*2KB = 1 MB raw per table, >> 64 KiB target
			s.Put(uint64(f*1000+i+1), []byte(fmt.Sprintf("k%02d%05d", f, i)), zero)
		}
		require.NoError(t, s.Flush())
	}
	require.NoError(t, s.Compact(0, uint64(1)<<62, cc))

	tabs := s.Tables()
	require.Len(t, tabs, 1,
		"1 MB+ of zero data compresses under the 64 KiB target, so it must roll into one table, not many")
	assert.Less(t, tabs[0].Size, int64(64<<10),
		"the single output table's on-disk size is under target (compressed)")
}

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
func flushSingle(t *testing.T, s *engineT, seq uint64, key, val string) {
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
	s := newTestEngine(t, 1<<20)

	t.Run("nothing ready", func(t *testing.T) {
		assert.Equal(t, -1, s.pickCompaction(2, 0, 0))
	})

	t.Run("ready at ratio", func(t *testing.T) {
		flushSingle(t, s, 1, "a", "1")
		assert.Equal(t, -1, s.pickCompaction(2, 0, 0), "one table below ratio")
		flushSingle(t, s, 2, "b", "2")
		assert.Equal(t, 0, s.pickCompaction(2, 0, 0), "two L0 tables meet ratio 2")
	})
}

// flushLarge writes many rows so the resulting L0 table is a few hundred KiB,
// then flushes it. Repeated calls build a tier of few-but-large tables.
func flushLarge(t *testing.T, s *engineT, seqBase uint64, keyPrefix string, rows int) {
	t.Helper()
	val := make([]byte, 512)
	for i := 0; i < rows; i++ {
		s.Put(seqBase+uint64(i), []byte(fmt.Sprintf("%s%06d", keyPrefix, i)), val)
	}
	require.NoError(t, s.Flush())
}

// TestPickCompactionLargeTableStall documents the size-tiered picker's baseline
// weakness (the UCS "large SSTable accumulation" problem): a tier holding a few
// large tables never reaches the count threshold, so it is never compacted no
// matter how many bytes it holds. This is the before-side the density trigger
// (roadmap milestone 3) must fix; when TierByteTrigger lands, an equivalent tier
// must instead be picked. Baseline numbers on this workload: 3 tables at depth 0,
// ~1.5 MiB of tier bytes, picker returns -1 at ratio 4.
func TestPickCompactionLargeTableStall(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<30) // large memtable so only explicit Flush rolls a table

	const tables = 3 // below the default TierRatio of 4
	for i := 0; i < tables; i++ {
		flushLarge(t, s, uint64(i*1000+1), fmt.Sprintf("t%d-k", i), 1000)
	}

	s.mu.RLock()
	var depth0 int
	var bytes int64
	for _, tbl := range s.tables {
		if tbl.depth == 0 {
			depth0++
			bytes += tbl.size
		}
	}
	s.mu.RUnlock()

	require.Equal(t, tables, depth0, "each Flush produced one L0 table")
	assert.Greater(t, bytes, int64(1<<20), "the tier holds well over a MiB")
	assert.Equal(t, -1, s.pickCompaction(4, 0, 0),
		"count-based picker stalls: a few large tables never reach the ratio")
}

// TestPickCompactionDensityTrigger is the after-side of the large-table stall:
// with TierByteTrigger set below the tier's bytes, the same few-but-large tier
// that the count trigger ignores is now picked (roadmap milestone 3). A sparse
// tier below the byte trigger still returns -1, and a single large table is never
// compacted alone.
func TestPickCompactionDensityTrigger(t *testing.T) {
	t.Parallel()

	t.Run("large tier picked below count ratio", func(t *testing.T) {
		s := newTestEngine(t, 1<<30)
		for i := 0; i < 3; i++ { // below ratio 4
			flushLarge(t, s, uint64(i*1000+1), fmt.Sprintf("t%d-k", i), 1000)
		}
		s.mu.RLock()
		var bytes int64
		for _, tbl := range s.tables {
			bytes += tbl.size
		}
		s.mu.RUnlock()

		// Count trigger alone still stalls; the density trigger fires.
		assert.Equal(t, -1, s.pickCompaction(4, 0, 0), "count trigger stalls")
		assert.Equal(t, 0, s.pickCompaction(4, bytes, 0),
			"density trigger picks the tier once its bytes reach the threshold")
	})

	t.Run("sparse tier below trigger stays idle", func(t *testing.T) {
		s := newTestEngine(t, 1<<30)
		flushSingle(t, s, 1, "a", "1")
		flushSingle(t, s, 2, "b", "2") // two tiny tables, well under any real trigger
		assert.Equal(t, -1, s.pickCompaction(4, 1<<30, 0),
			"a couple of tiny tables are below the byte trigger")
	})

	t.Run("single table never compacted alone", func(t *testing.T) {
		s := newTestEngine(t, 1<<30)
		flushLarge(t, s, 1, "k", 2000) // one big table
		s.mu.RLock()
		bytes := s.tables[0].size
		s.mu.RUnlock()
		assert.Equal(t, -1, s.pickCompaction(4, bytes, 0),
			"byte trigger requires >= 2 tables, so a lone table is left")
	})
}

// tbl builds a tableMeta with only the key bounds set (empty = nil = unknown).
func tbl(lo, hi string) *tableMeta {
	var mn, mx []byte
	if lo != "" {
		mn = []byte(lo)
	}
	if hi != "" {
		mx = []byte(hi)
	}
	return &tableMeta{minKey: mn, maxKey: mx}
}

func TestOverlapSets(t *testing.T) {
	t.Parallel()

	// setKeys renders each group as its members' "min-max" for stable comparison.
	setKeys := func(sets [][]*tableMeta) [][]string {
		out := make([][]string, len(sets))
		for i, g := range sets {
			for _, m := range g {
				out[i] = append(out[i], fmt.Sprintf("%s-%s", m.minKey, m.maxKey))
			}
		}
		return out
	}

	t.Run("empty", func(t *testing.T) {
		assert.Nil(t, overlapSets(nil))
	})

	t.Run("single", func(t *testing.T) {
		sets := overlapSets([]*tableMeta{tbl("a", "z")})
		assert.Len(t, sets, 1)
		assert.Len(t, sets[0], 1)
	})

	t.Run("disjoint ranges split", func(t *testing.T) {
		// a-c | e-g | i-k: three non-overlapping tables -> three groups.
		sets := overlapSets([]*tableMeta{tbl("i", "k"), tbl("a", "c"), tbl("e", "g")})
		assert.Equal(t, [][]string{{"a-c"}, {"e-g"}, {"i-k"}}, setKeys(sets),
			"non-overlapping tables never share a group; sorted by min")
	})

	t.Run("transitive chain groups together", func(t *testing.T) {
		// UCS worked example A:0-3, B:2-7, C:6-9, D:1-8. All chain-overlap, so for
		// COMPACTION SELECTION they form one connected group (this differs from UCS's
		// shared-boundary read-amp sets {A,B,D},{B,C,D} by design).
		a, b, c, d := tbl("0", "3"), tbl("2", "7"), tbl("6", "9"), tbl("1", "8")
		sets := overlapSets([]*tableMeta{c, a, d, b})
		require.Len(t, sets, 1, "A-D-B-C all connect through overlaps")
		assert.Len(t, sets[0], 4)
	})

	t.Run("two clusters separated by a gap", func(t *testing.T) {
		// {a-c, b-d} overlap; gap; {m-p, n-q} overlap.
		sets := overlapSets([]*tableMeta{tbl("a", "c"), tbl("b", "d"), tbl("m", "p"), tbl("n", "q")})
		assert.Equal(t, [][]string{{"a-c", "b-d"}, {"m-p", "n-q"}}, setKeys(sets))
	})

	t.Run("abutting ranges overlap", func(t *testing.T) {
		// c is both max of the first and min of the second -> they connect.
		sets := overlapSets([]*tableMeta{tbl("a", "c"), tbl("c", "e")})
		assert.Len(t, sets, 1, "shared boundary key counts as overlap")
	})

	t.Run("nil low bound overlaps up to its max", func(t *testing.T) {
		// ("",b) has an unknown start but a known end b, so it overlaps a-c (which
		// starts at a <= b) yet not m-p (which starts above b).
		sets := overlapSets([]*tableMeta{tbl("m", "p"), tbl("a", "c"), tbl("", "b")})
		require.Len(t, sets, 2, "unknown-low table joins the low cluster, not the far one")
		assert.Len(t, sets[0], 2, "(,b) and a-c connect")
		assert.Equal(t, []*tableMeta{tbl("m", "p")}[0].minKey, sets[1][0].minKey)
	})

	t.Run("nil high bound absorbs everything after", func(t *testing.T) {
		// An open-ended table covers all higher keys, so later tables join its group.
		sets := overlapSets([]*tableMeta{tbl("a", ""), tbl("m", "p"), tbl("x", "z")})
		require.Len(t, sets, 1, "unknown high bound is conservatively treated as overlapping")
		assert.Len(t, sets[0], 3)
	})
}

// TestOverlapScopedCompaction verifies that with OverlapSelection on, a tier
// holding two disjoint key ranges compacts only one overlapping group, leaving
// the unrelated range untouched (roadmap milestone 5). Two tables share key "a"
// (overlap) and two others share key "z" (a separate overlap); the compaction
// merges one group and leaves the other tables in place.
func TestOverlapScopedCompaction(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<30)

	// Seed a deeper tier so the depth-0 compaction below is NOT the bottom (overlap
	// selection is exempt at the bottom, which must merge wholly for tombstone GC).
	flushSingle(t, s, 1, "m", "seed1")
	flushSingle(t, s, 2, "m", "seed2")
	require.NoError(t, s.Compact(0, uint64(1)<<62, testCompactionConfig())) // -> one depth-1 table

	// Now two disjoint overlap groups at depth 0: {a,a} and {z,z}.
	flushSingle(t, s, 3, "a", "a1")
	flushSingle(t, s, 4, "a", "a2")
	flushSingle(t, s, 5, "z", "z1")
	flushSingle(t, s, 6, "z", "z2")

	var d0 int
	for _, tb := range s.Tables() {
		if tb.Depth == 0 {
			d0++
		}
	}
	require.Equal(t, 4, d0, "four L0 tables before compaction")

	cc := testCompactionConfig()
	cc.OverlapSelection = true
	require.NoError(t, s.Compact(0, uint64(1)<<62, cc))

	// Only the largest overlap group (2 tables) merged; the other 2 L0 tables stay.
	var depth0 int
	for _, tb := range s.Tables() {
		if tb.Depth == 0 {
			depth0++
		}
	}
	assert.Equal(t, 2, depth0, "the unrelated range's L0 tables stay; only one group merged")

	// Every key still reads correctly (newest version wins).
	assert.Equal(t, []byte("a2"), mustGet(t, s, 100, "a"))
	assert.Equal(t, []byte("z2"), mustGet(t, s, 100, "z"))
	assert.Equal(t, []byte("seed2"), mustGet(t, s, 100, "m"))
}

// TestOverlapScopedCompactionSurvivesCrash ensures overlap-scoped compaction is
// crash-safe end to end: after a DB-level compaction that used overlap selection,
// a crash and reopen returns every key.
func TestOverlapScopedCompactionSurvivesCrash(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := func() Options {
		o := DefaultOptions(dir)
		o.NoSync = true
		o.MemtableSize = 4096 // small so writes flush to several L0 tables
		return o
	}
	db, err := Open(opts())
	require.NoError(t, err)

	// Two interleaved key ranges so the tier has both overlap groups.
	const n = 400
	for start := 0; start < n; start += 100 {
		var batch Batch
		for i := start; i < start+100; i++ {
			batch.Put(PutOptions{Key: []byte(fmt.Sprintf("lo-%05d", i)), Value: []byte("v")})
			batch.Put(PutOptions{Key: []byte(fmt.Sprintf("hi-%05d", i)), Value: []byte("v")})
		}
		require.NoError(t, db.Write(&batch))
		db.sched.drain()
	}
	require.NoError(t, db.CompactRange(nil, nil)) // exercises the overlap-scoped path
	db.crash()

	db, err = Open(opts())
	require.NoError(t, err)
	defer db.Close()
	for i := 0; i < n; i++ {
		_, e1 := db.Get([]byte(fmt.Sprintf("lo-%05d", i)))
		_, e2 := db.Get([]byte(fmt.Sprintf("hi-%05d", i)))
		require.NoErrorf(t, e1, "lost lo-%05d", i)
		require.NoErrorf(t, e2, "lost hi-%05d", i)
	}
}

func TestCompactCollapsesVersions(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)

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
	s := newTestEngine(t, 1<<20)
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
	s := newTestEngine(t, 1<<20)
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
	s := newTestEngine(t, 1<<20)
	flushSingle(t, s, 1, "k", "v1")
	require.NoError(t, s.Compact(0, 0, testCompactionConfig()))
	assert.Len(t, s.Tables(), 1, "single table left untouched")
}

func TestCompactErrors(t *testing.T) {
	t.Parallel()

	t.Run("bad bottom codec", func(t *testing.T) {
		s := newTestEngine(t, 1<<20)
		// Two L0 tables => output tier is the bottom, so BottomCodecName is used.
		flushSingle(t, s, 1, "a", "1")
		flushSingle(t, s, 2, "b", "2")

		cc := testCompactionConfig()
		cc.BottomCodecName = "bogus"
		assert.Error(t, s.Compact(0, 0, cc))
	})

	t.Run("non-bottom output uses fresh codec", func(t *testing.T) {
		s := newTestEngine(t, 1<<20)
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
		cfg := engineConfigT{MemtableSize: 1 << 20, BloomBits: 10, BlockSize: 256, FreshCodecName: "none"}
		s := newEngine(cfg, newAllocator(0), tablePath)
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
		cfg := engineConfigT{MemtableSize: 1 << 20, BloomBits: 10, BlockSize: 256, FreshCodecName: "none"}
		s := newEngine(cfg, newAllocator(0), tablePath)
		t.Cleanup(func() { s.Close() })

		flushSingle(t, s, 1, "a", "1")
		flushSingle(t, s, 2, "b", "2")
		assert.Error(t, s.Compact(0, 0, testCompactionConfig()))
	})
}

func TestCompactManyKeys(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
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

func newBenchEngine(b *testing.B) *engineT {
	b.Helper()
	dir := b.TempDir()
	tablePath := func(num uint32) (string, error) {
		return filepath.Join(dir, fmt.Sprintf("%08x.sst", num)), nil
	}
	cfg := engineConfigT{MemtableSize: 1 << 30, BloomBits: 10, BlockSize: 4096, FreshCodecName: "s2"}
	s := newEngine(cfg, newAllocator(0), tablePath)
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
		s := newBenchEngine(b)
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

// benchCompactConfig returns a compaction config with overlap selection toggled.
func benchCompactConfig(overlap bool) compactionConfigT {
	return compactionConfigT{
		TierRatio:        4,
		BloomBits:        10,
		BlockSize:        4096,
		FreshCodecName:   "s2",
		BottomCodecName:  "zstd",
		OverlapSelection: overlap,
	}
}

// buildDisjointTier builds a non-bottom L0 tier of `ranges` disjoint key ranges
// (each in its own flushed table) over a pre-existing depth-1 seed table, so a
// depth-0 compaction is intermediate (overlap selection applies) and only one
// range's tables actually overlap. Returns the engine ready to Compact(0,...).
func buildDisjointTier(b *testing.B, ranges, tablesPerRange, rows int) *engineT {
	b.Helper()
	s := newBenchEngine(b)
	val := make([]byte, 100)
	var seq uint64
	put := func(prefix string) {
		for i := 0; i < rows; i++ {
			seq++
			s.Put(seq, []byte(fmt.Sprintf("%s%06d", prefix, i)), val)
		}
		if err := s.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	// Seed a depth-1 table so the depth-0 compaction below is not the bottom tier.
	put("seed")
	put("seed")
	if err := s.Compact(0, uint64(1)<<62, benchCompactConfig(false)); err != nil {
		b.Fatal(err)
	}
	// Now `ranges` disjoint key ranges at depth 0, each with tablesPerRange
	// overlapping tables (same prefix => same key span => one overlap group).
	for r := 0; r < ranges; r++ {
		prefix := fmt.Sprintf("r%02d-", r)
		for t := 0; t < tablesPerRange; t++ {
			put(prefix)
		}
	}
	return s
}

// BenchmarkCompactOverlapSelection compares a non-bottom tier compaction with
// overlap-scoped selection on vs off. With several disjoint ranges present,
// overlap selection merges only one range's tables while the off case rewrites
// the whole tier, so "on" moves far fewer bytes per compaction. mergedBytes is
// reported so the difference in work is visible, not just wall time.
func BenchmarkCompactOverlapSelection(b *testing.B) {
	const ranges, tablesPerRange, rows = 6, 2, 3000
	for _, overlap := range []bool{true, false} {
		name := "overlap-off"
		if overlap {
			name = "overlap-on"
		}
		b.Run(name, func(b *testing.B) {
			cc := benchCompactConfig(overlap)
			var mergedBytes int64
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				s := buildDisjointTier(b, ranges, tablesPerRange, rows)
				var before int64
				for _, t := range s.Tables() {
					if t.Depth == 0 {
						before += t.Size
					}
				}
				b.StartTimer()

				if err := s.Compact(0, uint64(1)<<62, cc); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				var after int64
				for _, t := range s.Tables() {
					if t.Depth == 0 {
						after += t.Size
					}
				}
				mergedBytes += before - after // L0 bytes consumed by this compaction
				b.StartTimer()
			}
			b.ReportMetric(float64(mergedBytes)/float64(b.N), "mergedBytes/op")
		})
	}
}

// BenchmarkPickCompaction measures the picker itself (count + density + tombstone
// scan over a tier) since it runs on the compaction hot path.
func BenchmarkPickCompaction(b *testing.B) {
	s := newBenchEngine(b)
	val := make([]byte, 100)
	var seq uint64
	for t := 0; t < 8; t++ {
		for i := 0; i < 1000; i++ {
			seq++
			s.Put(seq, []byte(fmt.Sprintf("k%06d", i)), val)
		}
		if err := s.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = s.pickCompaction(4, 128<<20, 0.5)
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
	s := newTestEngine(t, 1<<20)
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
	s := newTestEngine(t, 1<<20)
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
	s := newTestEngine(t, 1<<20)
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
	s := newTestEngine(t, 1<<20)
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
	s := newTestEngine(t, 1<<20)
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
	assert.Equal(t, -1, s.pickCompaction(1000, 0, 0), "no trigger without tombstone ratio")
	assert.Equal(t, -1, s.pickCompaction(1000, 0, 0.9), "ratio too high to trigger")
	assert.Equal(t, 0, s.pickCompaction(1000, 0, 0.5), "delete-heavy tier triggers at 0.5")

	// A single table cannot compact alone even if delete-heavy (needs >= 2).
	s2 := newTestEngine(t, 1<<20)
	defer s2.Close()
	s2.del(1, []byte("x"))
	require.NoError(t, s2.Flush())
	assert.Equal(t, -1, s2.pickCompaction(1000, 0, 0.1), "single table not compacted alone")
}

func TestTombstoneCompactionReclaimsDeletedSpace(t *testing.T) {
	t.Parallel()
	// A delete-heavy tier with the count trigger off must be compacted down by the
	// tombstone trigger so the deleted keys read as absent. Built deterministically
	// at the engine level (no reliance on background flush timing).
	s := newTestEngine(t, 1<<20)
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
	depth := s.pickCompaction(1000, 0, 0.5)
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

func TestRegressionTTLReplayCompactionConflict(t *testing.T) {
	t.Parallel()
	opts := DefaultOptions(t.TempDir())
	opts.MemtableSize = 1 << 30
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("old")}))
	require.NoError(t, db.eng.Flush())
	require.NoError(t, db.Put(PutOptions{Key: []byte("z"), Value: []byte("v")}))
	require.NoError(t, db.eng.Flush())
	require.NoError(t, db.eng.Compact(0, db.LatestSeq(), db.compactionConfig()))
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("new"), TTL: time.Nanosecond}))
	require.NoError(t, db.eng.Flush())
	require.NoError(t, db.Put(PutOptions{Key: []byte("y"), Value: []byte("v")}))
	require.NoError(t, db.eng.Flush())
	require.NoError(t, db.eng.Compact(0, db.LatestSeq(), db.compactionConfig()))
	db.crash()
	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()
	if err := db.CompactRange(nil, nil); err != nil {
		t.Fatalf("TTL conversion conflicts with WAL replay: %v", err)
	}
}

func TestTTLReplayDoesNotHidePayloadConflicts(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	for _, value := range []string{"first", "second"} {
		s.putTTL(1, []byte("k"), []byte(value), 1)
		require.NoError(t, s.Flush())
	}
	require.ErrorContains(t, s.CompactAll(maxIKeySeq, testCompactionConfig()), "conflicting values")
}
