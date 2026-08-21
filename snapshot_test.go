package levisdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotIsolation(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v1")}))

	snap, err := db.Snapshot()
	require.NoError(t, err)
	defer snap.Release()

	// Overwrite after the snapshot was taken.
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v2")}))

	// Live read sees the new value.
	v, err := db.Get([]byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), v)

	// Snapshot still sees the old value.
	sv, err := snap.Get([]byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), sv)
}

func TestSnapshotSurvivesCompaction(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) {
		o.ShardCount = 1
		o.MemtableSize = 256
	})

	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("original")}))
	snap, err := db.Snapshot()
	require.NoError(t, err)
	defer snap.Release()

	// Churn: overwrite k many times and add keys to force flush+compaction
	// while the snapshot is held.
	// One shard makes 60 generations sufficient to produce several tables and
	// force compaction without spending the package budget on unrelated I/O.
	for i := 0; i < 60; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte(fmt.Sprintf("update%d", i))}))
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("pad%04d", i)), Value: []byte("p")}))
	}
	db.sched.drain()

	// The snapshot must still read the original value, retained through
	// compaction because the snapshot pinned its sequence.
	sv, err := snap.Get([]byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("original"), sv)
}

func TestSnapshotIterator(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	require.NoError(t, db.Put(PutOptions{Key: []byte("a"), Value: []byte("1")}))
	require.NoError(t, db.Put(PutOptions{Key: []byte("b"), Value: []byte("2")}))

	snap, err := db.Snapshot()
	require.NoError(t, err)
	defer snap.Release()

	require.NoError(t, db.Put(PutOptions{Key: []byte("c"), Value: []byte("3")}))

	it, err := snap.NewIterator()
	require.NoError(t, err)
	defer it.Close()

	var keys []string
	for it.Next() {
		keys = append(keys, string(it.Key()))
	}
	// c was written after the snapshot; it is not visible.
	assert.Equal(t, []string{"a", "b"}, keys)
}

func TestRangeIterator(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 512 })
	for i := 0; i < 100; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%03d", i)), Value: []byte("v")}))
	}

	it, err := db.NewRangeIterator([]byte("k010"), []byte("k020"))
	require.NoError(t, err)
	defer it.Close()

	var keys []string
	for it.Next() {
		keys = append(keys, string(it.Key()))
	}
	require.Len(t, keys, 10)
	assert.Equal(t, "k010", keys[0])
	assert.Equal(t, "k019", keys[9])
}

func TestRangeIteratorUnboundedEnd(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	for _, k := range []string{"a", "b", "c", "d"} {
		require.NoError(t, db.Put(PutOptions{Key: []byte(k), Value: []byte("v")}))
	}
	it, err := db.NewRangeIterator([]byte("c"), nil)
	require.NoError(t, err)
	defer it.Close()

	var keys []string
	for it.Next() {
		keys = append(keys, string(it.Key()))
	}
	assert.Equal(t, []string{"c", "d"}, keys)
}

func TestBatchAtomicAllOrNothing(t *testing.T) {
	t.Parallel()
	// A batch is one WAL append: either every op is durable or none is. Verify
	// all ops of a committed batch are visible together.
	db := openTestDB(t, nil)
	var b Batch
	for i := 0; i < 50; i++ {
		b.Put(PutOptions{Key: []byte(fmt.Sprintf("k%02d", i)), Value: []byte(fmt.Sprintf("v%d", i))})
	}
	require.NoError(t, db.Write(&b))

	for i := 0; i < 50; i++ {
		v, err := db.Get([]byte(fmt.Sprintf("k%02d", i)))
		require.NoError(t, err, i)
		assert.Equal(t, []byte(fmt.Sprintf("v%d", i)), v)
	}
}

func TestSnapshotAfterCloseFails(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	require.NoError(t, db.Close())
	_, err := db.Snapshot()
	assert.ErrorIs(t, err, ErrClosed)
}

func TestSnapshotGetEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("empty key rejected", func(t *testing.T) {
		db := openTestDB(t, nil)
		snap, err := db.Snapshot()
		require.NoError(t, err)
		defer snap.Release()
		_, err = snap.Get(nil)
		assert.ErrorIs(t, err, ErrEmptyKey)
	})

	t.Run("get after close rejected", func(t *testing.T) {
		db := openTestDB(t, nil)
		snap, err := db.Snapshot()
		require.NoError(t, err)
		require.NoError(t, db.Close())
		// getAt sees the closed db and returns ErrClosed.
		_, err = snap.Get([]byte("k"))
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestSnapshotNewIteratorAfterCloseFails(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	snap, err := db.Snapshot()
	require.NoError(t, err)
	require.NoError(t, db.Close())
	_, err = snap.NewIterator()
	assert.ErrorIs(t, err, ErrClosed)
}

func TestSnapshotReleaseSharedSeq(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))

	// Two snapshots at the same seq: releasing one decrements the count but the
	// seq stays pinned so oldest still reports it.
	s1, err := db.Snapshot()
	require.NoError(t, err)
	s2, err := db.Snapshot()
	require.NoError(t, err)
	require.Equal(t, s1.Seq(), s2.Seq())

	seq := s1.Seq()
	s1.Release()
	assert.Equal(t, seq, db.snaps.oldest(seq+100), "seq still pinned by s2")

	s2.Release()
	assert.Equal(t, seq+100, db.snaps.oldest(seq+100), "fallback once all released")
}

func TestSnapshotSeqAccessor(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
	snap, err := db.Snapshot()
	require.NoError(t, err)
	defer snap.Release()
	assert.Equal(t, db.readSeq.Load(), snap.Seq())
}

func TestSnapshotHas(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v1")}))

	snap, err := db.Snapshot()
	require.NoError(t, err)
	defer snap.Release()

	// Delete after the snapshot; the snapshot still sees the key present.
	require.NoError(t, db.Delete([]byte("k")))

	has, err := snap.Has([]byte("k"))
	require.NoError(t, err)
	assert.True(t, has, "snapshot predates the delete")

	// Live view sees it gone.
	has, err = db.Has([]byte("k"))
	require.NoError(t, err)
	assert.False(t, has)
}

func TestSnapshotRangeIterator(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, db.Put(PutOptions{Key: []byte(k), Value: []byte("v")}))
	}
	snap, err := db.Snapshot()
	require.NoError(t, err)
	defer snap.Release()

	// Writes after the snapshot are invisible to its iterator.
	require.NoError(t, db.Put(PutOptions{Key: []byte("f"), Value: []byte("v")}))

	it, err := snap.NewRangeIterator([]byte("b"), []byte("e"))
	require.NoError(t, err)
	defer it.Close()

	var keys []string
	for it.Next() {
		keys = append(keys, string(it.Key()))
	}
	assert.Equal(t, []string{"b", "c", "d"}, keys)
}
