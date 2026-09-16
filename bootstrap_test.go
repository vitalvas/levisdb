package levisdb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bootstrapImagePath returns a scratch path for a snapshot base image.
func bootstrapImagePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "base.sst")
}

func TestSnapshotWriteTo(t *testing.T) {
	t.Parallel()

	t.Run("RoundTrip", func(t *testing.T) {
		t.Parallel()
		// Small memtable so some data flushes to on-disk tables the scan must merge.
		src := openTestDB(t, func(o *Options) { o.MemtableSize = 2 << 10 })
		const n = 300
		for i := 0; i < n; i++ {
			require.NoError(t, src.Put(PutOptions{
				Key:   []byte(fmt.Sprintf("k%05d", i)),
				Value: []byte(fmt.Sprintf("value-%05d", i)),
			}))
		}
		src.sched.drain()

		snap, err := src.Snapshot()
		require.NoError(t, err)
		defer snap.Release()
		path := bootstrapImagePath(t)
		resume, err := snap.WriteTo(path, SstWriterOptions{})
		require.NoError(t, err)
		assert.Equal(t, snap.Seq(), resume)

		follower := openTestDB(t, nil)
		require.NoError(t, follower.IngestExternalFile(path))
		for i := 0; i < n; i++ {
			got, gerr := follower.Get([]byte(fmt.Sprintf("k%05d", i)))
			require.NoError(t, gerr, "i=%d", i)
			assert.Equal(t, []byte(fmt.Sprintf("value-%05d", i)), got, "i=%d", i)
		}
	})

	t.Run("ExactExpiresAt", func(t *testing.T) {
		t.Parallel()
		src := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })
		require.NoError(t, src.Put(PutOptions{Key: []byte("plain"), Value: []byte("v")}))
		require.NoError(t, src.Put(PutOptions{Key: []byte("ttl"), Value: []byte("v"), TTL: time.Hour}))

		// The exact stored deadline the source recorded for the TTL key.
		srcEntries := drainUpdates(t, src, 0)
		var srcExpiry int64
		for _, e := range srcEntries {
			if string(e.Key) == "ttl" {
				srcExpiry = e.ExpiresAt
			}
		}
		require.NotZero(t, srcExpiry)

		snap, err := src.Snapshot()
		require.NoError(t, err)
		defer snap.Release()
		// Sleep so a now+ttl re-derivation would visibly differ from the stored
		// deadline; PutWithExpiry must copy the exact value instead.
		time.Sleep(20 * time.Millisecond)
		path := bootstrapImagePath(t)
		_, err = snap.WriteTo(path, SstWriterOptions{})
		require.NoError(t, err)

		follower := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })
		require.NoError(t, follower.IngestExternalFile(path))

		// Ingest bypasses the WAL, so read the preserved deadline straight off the
		// ingested table via the engine iterator that surfaces ValueExpiresAt.
		var folExpiry int64
		it := follower.eng.NewIterator(maxIKeySeq)
		for it.Next() {
			if string(it.Key()) == "ttl" {
				folExpiry = it.valueExpiresAt()
			}
		}
		require.NoError(t, it.Close())
		assert.Equal(t, srcExpiry, folExpiry, "TTL deadline preserved exactly, not re-derived")

		// The plain key round-trips as a non-expiring value.
		got, gerr := follower.Get([]byte("plain"))
		require.NoError(t, gerr)
		assert.Equal(t, []byte("v"), got)
	})

	t.Run("TailConsistency", func(t *testing.T) {
		t.Parallel()
		src := openTestDB(t, func(o *Options) {
			o.MemtableSize = 1 << 30
			o.WALRetention = time.Hour
			o.WALRetentionBytes = 1 << 30
		})
		for i := 0; i < 5; i++ {
			require.NoError(t, src.Put(PutOptions{Key: []byte(fmt.Sprintf("base%d", i)), Value: []byte("v")}))
		}

		snap, err := src.Snapshot()
		require.NoError(t, err)
		defer snap.Release()

		// Writes after the snapshot must not appear in the base image.
		for i := 0; i < 3; i++ {
			require.NoError(t, src.Put(PutOptions{Key: []byte(fmt.Sprintf("tail%d", i)), Value: []byte("v")}))
		}

		path := bootstrapImagePath(t)
		resume, err := snap.WriteTo(path, SstWriterOptions{})
		require.NoError(t, err)
		assert.Equal(t, snap.Seq(), resume)

		follower := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })
		require.NoError(t, follower.IngestExternalFile(path))
		for i := 0; i < 3; i++ {
			_, gerr := follower.Get([]byte(fmt.Sprintf("tail%d", i)))
			assert.ErrorIs(t, gerr, ErrNotFound, "post-snapshot writes absent from base image")
		}

		// The tail from the resume seq yields exactly the post-snapshot writes.
		tail := drainUpdates(t, src, resume)
		require.Len(t, tail, 3)
		for i, e := range tail {
			assert.Equal(t, []byte(fmt.Sprintf("tail%d", i)), e.Key)
		}
	})

	t.Run("EmptySnapshot", func(t *testing.T) {
		t.Parallel()
		src := openTestDB(t, nil)
		require.NoError(t, src.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
		require.NoError(t, src.Delete([]byte("k")))

		snap, err := src.Snapshot()
		require.NoError(t, err)
		defer snap.Release()
		path := bootstrapImagePath(t)
		resume, err := snap.WriteTo(path, SstWriterOptions{})
		require.NoError(t, err)
		assert.Equal(t, snap.Seq(), resume)
		_, staterr := os.Stat(path)
		assert.True(t, os.IsNotExist(staterr), "empty snapshot writes no file")
	})

	t.Run("PinReleased", func(t *testing.T) {
		t.Parallel()
		src := openTestDB(t, nil)
		require.NoError(t, src.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
		snap, err := src.Snapshot()
		require.NoError(t, err)
		path := bootstrapImagePath(t)
		_, err = snap.WriteTo(path, SstWriterOptions{})
		require.NoError(t, err)
		snap.Release()

		assert.Equal(t, 0, src.snaps.live(), "WriteTo leaks no seq pin")
		require.True(t, src.snaps.beginCompactionFilter(), "iterator-time and seq pins fully released")
		src.snaps.endCompactionFilter()
		snap.Release() // idempotent
	})

	t.Run("ClosedDB", func(t *testing.T) {
		t.Parallel()
		src := openTestDB(t, nil)
		require.NoError(t, src.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
		snap, err := src.Snapshot()
		require.NoError(t, err)
		require.NoError(t, src.Close())
		_, err = snap.WriteTo(bootstrapImagePath(t), SstWriterOptions{})
		assert.ErrorIs(t, err, ErrClosed)
	})
}

func TestSnapshotWriteToDir(t *testing.T) {
	t.Parallel()

	t.Run("MultiChunkRoundTrip", func(t *testing.T) {
		t.Parallel()
		src := openTestDB(t, func(o *Options) { o.MemtableSize = 2 << 10 })
		const n = 500
		// Incompressible values (deterministic keystream) so bytesWritten, which
		// counts compressed bytes, actually crosses the small roll cap.
		mkVal := func(i int) []byte {
			v := make([]byte, 256)
			x := uint64(i)*0x9e3779b97f4a7c15 + 1
			for j := range v {
				x = x*6364136223846793005 + 1442695040888963407
				v[j] = byte(x >> 56)
			}
			return v
		}
		for i := 0; i < n; i++ {
			require.NoError(t, src.Put(PutOptions{Key: []byte(fmt.Sprintf("k%05d", i)), Value: mkVal(i)}))
		}
		src.sched.drain()

		snap, err := src.Snapshot()
		require.NoError(t, err)
		defer snap.Release()
		dir := t.TempDir()
		paths, resume, err := snap.WriteToDir(dir, SstWriterOptions{}, 16<<10)
		require.NoError(t, err)
		assert.Equal(t, snap.Seq(), resume)
		require.Greater(t, len(paths), 1, "small cap must roll multiple chunks")

		follower := openTestDB(t, nil)
		for _, p := range paths {
			require.NoError(t, follower.IngestExternalFile(p))
		}
		for i := 0; i < n; i++ {
			got, gerr := follower.Get([]byte(fmt.Sprintf("k%05d", i)))
			require.NoError(t, gerr, "i=%d", i)
			assert.Equal(t, mkVal(i), got, "i=%d", i)
		}
	})

	t.Run("EmptySnapshot", func(t *testing.T) {
		t.Parallel()
		src := openTestDB(t, nil)
		snap, err := src.Snapshot()
		require.NoError(t, err)
		defer snap.Release()
		dir := t.TempDir()
		paths, resume, err := snap.WriteToDir(dir, SstWriterOptions{}, 0)
		require.NoError(t, err)
		assert.Empty(t, paths)
		assert.Equal(t, snap.Seq(), resume)
		entries, rerr := os.ReadDir(dir)
		require.NoError(t, rerr)
		assert.Empty(t, entries, "empty snapshot writes no chunk files")
	})

	t.Run("PinReleased", func(t *testing.T) {
		t.Parallel()
		src := openTestDB(t, nil)
		require.NoError(t, src.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
		snap, err := src.Snapshot()
		require.NoError(t, err)
		_, _, err = snap.WriteToDir(t.TempDir(), SstWriterOptions{}, 0)
		require.NoError(t, err)
		snap.Release()
		assert.Equal(t, 0, src.snaps.live(), "WriteToDir leaks no seq pin")
		require.True(t, src.snaps.beginCompactionFilter(), "pins fully released")
		src.snaps.endCompactionFilter()
	})
}
