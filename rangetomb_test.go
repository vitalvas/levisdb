package levisdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mustNotFound asserts key is absent.
func mustNotFound(t *testing.T, db *DB, key string) {
	t.Helper()
	_, err := db.Get([]byte(key))
	assert.ErrorIsf(t, err, ErrNotFound, "key %q should be deleted", key)
}

// mustGetEq asserts key reads back as want.
func mustGetEq(t *testing.T, db *DB, key, want string) {
	t.Helper()
	got, err := db.Get([]byte(key))
	require.NoErrorf(t, err, "key %q", key)
	assert.Equalf(t, want, string(got), "key %q", key)
}

func TestDeleteRangeBasic(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 }) // stay in memtable
	for i := 0; i < 10; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%02d", i)), Value: []byte("v")}))
	}
	// Delete [k03, k07): k03..k06 gone, k02 and k07 survive.
	require.NoError(t, db.DeleteRange([]byte("k03"), []byte("k07")))

	mustGetEq(t, db, "k02", "v")
	for i := 3; i < 7; i++ {
		mustNotFound(t, db, fmt.Sprintf("k%02d", i))
	}
	mustGetEq(t, db, "k07", "v")
	mustGetEq(t, db, "k09", "v")
}

func TestDeleteRangeThenReinsert(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })
	require.NoError(t, db.Put(PutOptions{Key: []byte("m"), Value: []byte("old")}))
	require.NoError(t, db.DeleteRange([]byte("a"), []byte("z")))
	mustNotFound(t, db, "m")
	// A write after the range delete is newer and must be visible.
	require.NoError(t, db.Put(PutOptions{Key: []byte("m"), Value: []byte("new")}))
	mustGetEq(t, db, "m", "new")
}

func TestDeleteRangeSnapshotUnaffected(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))

	snap, err := db.Snapshot()
	require.NoError(t, err)
	defer snap.Release()

	// Range-delete after the snapshot: the snapshot must still see the key.
	require.NoError(t, db.DeleteRange([]byte("a"), []byte("z")))
	mustNotFound(t, db, "k") // latest view: deleted

	got, err := snap.Get([]byte("k"))
	require.NoError(t, err, "snapshot taken before the range delete still sees the key")
	assert.Equal(t, []byte("v"), got)
}

func TestDeleteRangeSurvivesFlush(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })
	for i := 0; i < 20; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%02d", i)), Value: []byte("v")}))
	}
	require.NoError(t, db.DeleteRange([]byte("k05"), []byte("k15")))
	// Flush moves both the points and the range tombstone to a table.
	require.NoError(t, db.eng.Flush())

	for i := 5; i < 15; i++ {
		mustNotFound(t, db, fmt.Sprintf("k%02d", i))
	}
	mustGetEq(t, db, "k04", "v")
	mustGetEq(t, db, "k15", "v")
}

func TestDeleteRangeShadowsOlderTableFromMemtable(t *testing.T) {
	t.Parallel()
	// A point value flushed to a table, then a range delete in the memtable must
	// shadow it (range tomb in a newer source deletes a point in an older one).
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
	require.NoError(t, db.eng.Flush()) // k now in a table
	require.NoError(t, db.DeleteRange([]byte("a"), []byte("z")))
	mustNotFound(t, db, "k")
}

func TestDeleteRangeSurvivesCompaction(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })
	for i := 0; i < 20; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%02d", i)), Value: []byte("v")}))
	}
	require.NoError(t, db.eng.Flush())
	require.NoError(t, db.DeleteRange([]byte("k05"), []byte("k15")))
	require.NoError(t, db.eng.Flush())
	// Full compaction: the range tombstone at the bottom reclaims the covered keys.
	require.NoError(t, db.CompactRange(nil, nil))

	for i := 5; i < 15; i++ {
		mustNotFound(t, db, fmt.Sprintf("k%02d", i))
	}
	mustGetEq(t, db, "k04", "v")
	mustGetEq(t, db, "k15", "v")
}

func TestDeleteRangeReopenPersists(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	open := func() *DB {
		o := DefaultOptions(dir)
		o.NoSync = true
		db, err := Open(o)
		require.NoError(t, err)
		return db
	}
	db := open()
	for i := 0; i < 10; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%02d", i)), Value: []byte("v")}))
	}
	require.NoError(t, db.DeleteRange([]byte("k02"), []byte("k08")))
	require.NoError(t, db.Close()) // flush-on-close persists the range tombstone

	db2 := open()
	defer db2.Close()
	for i := 2; i < 8; i++ {
		mustNotFound(t, db2, fmt.Sprintf("k%02d", i))
	}
	mustGetEq(t, db2, "k01", "v")
	mustGetEq(t, db2, "k08", "v")
}

func TestDeleteRangeRecoversFromWALAfterCrash(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	o := func() Options {
		opt := DefaultOptions(dir)
		opt.NoSync = true
		opt.MemtableSize = 1 << 30 // keep everything in the WAL until the crash
		return opt
	}
	db, err := Open(o())
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%02d", i)), Value: []byte("v")}))
	}
	require.NoError(t, db.DeleteRange([]byte("k03"), []byte("k06")))
	db.crash()

	db, err = Open(o())
	require.NoError(t, err)
	defer db.Close()
	for i := 3; i < 6; i++ {
		mustNotFound(t, db, fmt.Sprintf("k%02d", i))
	}
	mustGetEq(t, db, "k02", "v")
	mustGetEq(t, db, "k06", "v")
}

func TestDeleteRangeIterator(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })
	for i := 0; i < 10; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%02d", i)), Value: []byte("v")}))
	}
	require.NoError(t, db.DeleteRange([]byte("k03"), []byte("k07")))

	it, err := db.NewIterator()
	require.NoError(t, err)
	defer it.Close()
	var got []string
	for it.Next() {
		got = append(got, string(it.Key()))
	}
	require.NoError(t, it.Error())
	want := []string{"k00", "k01", "k02", "k07", "k08", "k09"}
	assert.Equal(t, want, got, "iterator skips range-deleted keys")
}

func TestDeleteRangeValidation(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	assert.ErrorIs(t, db.DeleteRange([]byte("b"), []byte("a")), ErrInvalidRange, "end < start")
	assert.ErrorIs(t, db.DeleteRange([]byte("a"), []byte("a")), ErrInvalidRange, "end == start")
	assert.ErrorIs(t, db.DeleteRange([]byte("a"), nil), ErrInvalidRange, "empty end")
	assert.ErrorIs(t, db.DeleteRange(nil, []byte("z")), ErrEmptyKey, "empty start")
}

func TestDeleteRangeObserved(t *testing.T) {
	t.Parallel()
	var seen []WALEntry
	obs := observerFunc(func(batch []WALEntry) {
		for _, e := range batch {
			cp := e
			cp.Key = append([]byte(nil), e.Key...)
			cp.Value = append([]byte(nil), e.Value...)
			seen = append(seen, cp)
		}
	})
	db := openTestDB(t, func(o *Options) { o.WALObserver = obs })
	require.NoError(t, db.DeleteRange([]byte("a"), []byte("m")))

	require.Len(t, seen, 1)
	assert.Equal(t, EntryDeleteRange, seen[0].Kind)
	assert.Equal(t, []byte("a"), seen[0].Key)
	assert.Equal(t, []byte("m"), seen[0].Value, "range delete carries the end key in Value")
}

// engineGet is an engine-level Get returning "" for absent/deleted, for tests
// that drive tiers directly.
func engineGet(t *testing.T, s *engineT, seq uint64, key string) (string, bool) {
	t.Helper()
	v, found, deleted, err := s.get(seq, []byte(key))
	require.NoError(t, err)
	if !found || deleted {
		return "", false
	}
	return string(v), true
}

// TestDeleteRangeCarriedAcrossNonBottomCompaction (F2): a range tombstone flushed
// into an L0 table is carried into the L1 output by a non-bottom Compact(0), and
// still shadows a covered point that lives in a deeper (non-input) table.
func TestDeleteRangeCarriedAcrossNonBottomCompaction(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<30)
	// Deeper table at L1 holding an OLD point for "k" (seq 1).
	flushSingle(t, s, 1, "k", "old")
	require.NoError(t, s.Compact(0, uint64(1)<<62, testCompactionConfig())) // -> L1

	// Two L0 tables so Compact(0) has >= 2 inputs: one with a range delete over
	// "k" (seq 10), one with an unrelated point (seq 11) so the tier isn't bottom.
	s.delRange(10, []byte("j"), []byte("l"))
	require.NoError(t, s.Flush())
	flushSingle(t, s, 11, "z", "v")

	// Non-bottom compaction of L0 -> L1. The range tombstone is carried into the
	// output (not dropped: L1 is not the deepest here) and must keep shadowing the
	// deeper "k". retainSeq huge so nothing is snapshot-pinned.
	require.NoError(t, s.Compact(0, uint64(1)<<62, testCompactionConfig()))

	_, live := engineGet(t, s, maxIKeySeq, "k")
	assert.False(t, live, "range tombstone carried through non-bottom compaction still deletes k")
	v, ok := engineGet(t, s, maxIKeySeq, "z")
	require.True(t, ok)
	assert.Equal(t, "v", v)
}

// TestDeleteRangeBottomReclaimRespectsSnapshot (F2): a snapshot taken before a
// DeleteRange keeps seeing the pre-delete value even after a full compaction that
// would otherwise reclaim the covered key, because retainSeq pins it.
func TestDeleteRangeBottomReclaimRespectsSnapshot(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))

	snap, err := db.Snapshot()
	require.NoError(t, err)
	defer snap.Release()

	require.NoError(t, db.DeleteRange([]byte("a"), []byte("z")))
	require.NoError(t, db.eng.Flush())
	// Full compaction while the snapshot is held: retainSeq = snapshot seq, so the
	// range tombstone (newer than the snapshot) must be retained, not reclaimed.
	require.NoError(t, db.CompactRange(nil, nil))

	got, err := snap.Get([]byte("k"))
	require.NoError(t, err, "snapshot below the range delete still sees the pre-delete value after compaction")
	assert.Equal(t, []byte("v"), got)
	mustNotFound(t, db, "k") // latest view: deleted
}

// TestDeleteRangeCrossOutputShadowing (F2): when a compaction rolls more than one
// output table, the range tombstone is attached to the first output only, but its
// widened bounds must let a read find it for a covered point that landed in a
// later output table.
func TestDeleteRangeCrossOutputShadowing(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<30)
	cc := testCompactionConfig()
	// Tiny target so the bottom compaction rolls several output tables.
	cc.FileSizeBase = 512
	cc.FileSizeMax = 512

	// Enough distinct points across a wide key span to force multiple output
	// tables, plus a range delete covering a slice of them.
	const n = 200
	for i := 0; i < n; i++ {
		s.Put(uint64(i+1), []byte(fmt.Sprintf("k%05d", i)), []byte("payload-payload-payload"))
	}
	require.NoError(t, s.Flush())
	// Range-delete a middle slice at a high seq so it shadows the flushed points.
	s.delRange(uint64(n+1), []byte("k00050"), []byte("k00150"))
	require.NoError(t, s.Flush())

	// Full compaction to the bottom, rolling multiple outputs. retainSeq huge so
	// the tomb reclaims (drops) the covered points outright.
	require.NoError(t, s.CompactAll(uint64(1)<<62, cc))

	for i := 50; i < 150; i++ {
		_, live := engineGet(t, s, maxIKeySeq, fmt.Sprintf("k%05d", i))
		assert.Falsef(t, live, "k%05d in a later output table must still be range-deleted", i)
	}
	// Boundaries survive.
	_, ok := engineGet(t, s, maxIKeySeq, "k00049")
	assert.True(t, ok)
	_, ok = engineGet(t, s, maxIKeySeq, "k00150")
	assert.True(t, ok)
}
