package levisdb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// firstSSTPath returns the path of the lowest-numbered .sst file in dir.
func firstSSTPath(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	best := ""
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".sst" && (best == "" || e.Name() < best) {
			best = e.Name()
		}
	}
	require.NotEmpty(t, best, "expected at least one .sst table")
	return filepath.Join(dir, best)
}

// flipByteInDataRegion flips one byte early in the file (inside the first data
// block, well before the footer), corrupting a block so its CRC check fails.
func flipByteInDataRegion(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	defer f.Close()
	var b [1]byte
	const off = 4 // past nothing meaningful; inside the first block's payload
	_, err = f.ReadAt(b[:], off)
	require.NoError(t, err)
	b[0] ^= 0xff
	_, err = f.WriteAt(b[:], off)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
}

func TestVerifyCleanDatabase(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 4 << 10 })
	for start := 0; start < 300; start += 150 {
		var batch Batch
		for i := start; i < start+150; i++ {
			batch.Put(PutOptions{Key: []byte(fmt.Sprintf("k%05d", i)), Value: []byte("value-payload")})
		}
		require.NoError(t, db.Write(&batch))
		db.sched.drain()
	}
	require.NotEmpty(t, db.eng.Tables())

	faults, err := db.Verify()
	require.NoError(t, err)
	assert.Empty(t, faults, "a healthy database has no corrupt tables")
}

func TestVerifyDetectsCorruptBlock(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	o := DefaultOptions(dir)
	o.NoSync = true
	o.MemtableSize = 4 << 10
	db, err := Open(o)
	require.NoError(t, err)

	var batch Batch
	for i := 0; i < 300; i++ {
		batch.Put(PutOptions{Key: []byte(fmt.Sprintf("k%05d", i)), Value: []byte("value-payload")})
	}
	require.NoError(t, db.Write(&batch))
	// Force everything to disk in a settled table set.
	require.NoError(t, db.CompactRange(nil, nil))

	// Corrupt a data block on disk; the fd pool re-reads the same inode, so the
	// live reader sees the damaged bytes.
	flipByteInDataRegion(t, firstSSTPath(t, dir))

	faults, err := db.Verify()
	require.NoError(t, err)
	require.NotEmpty(t, faults, "Verify must detect the corrupted block")
	assert.Error(t, faults[0].Err)
	require.NoError(t, db.Close())
}

func TestVerifyDetectsCorruptRangeDelTable(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	o := DefaultOptions(dir)
	o.NoSync = true
	o.MemtableSize = 1 << 30
	db, err := Open(o)
	require.NoError(t, err)

	for i := 0; i < 50; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%05d", i)), Value: []byte("v")}))
	}
	require.NoError(t, db.DeleteRange([]byte("k00010"), []byte("k00040")))
	require.NoError(t, db.eng.Flush()) // one table carrying points + a range-del block

	flipByteInDataRegion(t, firstSSTPath(t, dir))

	faults, err := db.Verify()
	require.NoError(t, err)
	require.NotEmpty(t, faults, "Verify must detect corruption in a range-del-carrying table")
	require.NoError(t, db.Close())
}

func TestVerifyClosedDB(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	require.NoError(t, db.Close())
	_, err := db.Verify()
	assert.ErrorIs(t, err, ErrClosed)
}

func TestRegressionVerifyCachedMetadata(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
	require.NoError(t, db.eng.Flush())
	table := db.eng.tables[0]
	f, err := os.OpenFile(table.path, os.O_RDWR, 0)
	require.NoError(t, err)
	off := int64(table.reader.filterBH.offset)
	b := make([]byte, 1)
	_, err = f.ReadAt(b, off)
	require.NoError(t, err)
	b[0] ^= 0xff
	_, err = f.WriteAt(b, off)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	faults, err := db.Verify()
	require.NoError(t, err)
	if len(faults) == 0 {
		t.Fatal("Verify reported intact table after filter block was corrupted on disk")
	}
}
