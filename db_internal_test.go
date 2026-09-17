package levisdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenMissingManifestErrors(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openAt(t, dir, 1<<20)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
	require.NoError(t, db.Close())

	// CURRENT still points at the manifest, but the manifest file is gone:
	// replayManifest's os.Open must fail and Open must return the error.
	s, err := openStorage(dir)
	require.NoError(t, err)
	num, ok, err := s.readCurrent()
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, os.Remove(s.manifestPath(num)))
	require.NoError(t, s.Close())

	_, err = Open(DefaultOptions(dir))
	require.Error(t, err)
}

func openAt(t *testing.T, dir string, memSize int64) *DB {
	t.Helper()
	o := DefaultOptions(dir)
	o.MemtableSize = memSize
	// Skip compression to keep recovery tests fast; blocks record their own codec
	// id, so reopening data written with any codec still reads correctly.
	o.FreshCodec = CodecNone
	o.BottomCodec = CodecNone
	// NoSync keeps tests fast. crash() closes handles without a real power loss,
	// so page-cache writes survive and recovery still sees the data.
	o.NoSync = true
	db, err := Open(o)
	require.NoError(t, err)
	return db
}

func TestRecoverUnflushedWrites(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Large memtable so nothing flushes; all writes stay in the WAL only.
	db := openAt(t, dir, 1<<30)
	for i := 0; i < 100; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("key%03d", i)), Value: []byte(fmt.Sprintf("v%d", i))}))
	}
	db.crash() // no clean Close: memtables not flushed, WAL left behind

	// Reopen: WAL replay must recover every committed write.
	db2 := openAt(t, dir, 1<<30)
	defer db2.Close()
	for i := 0; i < 100; i++ {
		v, err := db2.Get([]byte(fmt.Sprintf("key%03d", i)))
		require.NoError(t, err, i)
		assert.Equal(t, []byte(fmt.Sprintf("v%d", i)), v)
	}
}

func TestRecoverMixedFlushedAndWAL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	db := openAt(t, dir, 1<<30)
	for start := 0; start < 150; start += 75 {
		var batch Batch
		for i := start; i < start+75; i++ {
			batch.Put(PutOptions{Key: []byte(fmt.Sprintf("key%03d", i)), Value: []byte(fmt.Sprintf("v%d", i))})
		}
		require.NoError(t, db.Write(&batch))
		if start == 0 {
			require.NoError(t, db.eng.Flush())
		}
	}
	// Guarantee both recovery sources exist instead of relying on flush timing.
	require.NotEmpty(t, db.eng.Tables())
	require.False(t, db.eng.memEmpty())
	db.crash()

	db2 := openAt(t, dir, 512)
	defer db2.Close()
	miss := 0
	for i := 0; i < 150; i++ {
		v, err := db2.Get([]byte(fmt.Sprintf("key%03d", i)))
		if err != nil {
			miss++
			continue
		}
		assert.Equal(t, []byte(fmt.Sprintf("v%d", i)), v)
	}
	assert.Zero(t, miss, "all committed writes must survive a crash")
}

func TestRecoverDeletes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	db := openAt(t, dir, 1<<30)
	require.NoError(t, db.Put(PutOptions{Key: []byte("a"), Value: []byte("1")}))
	require.NoError(t, db.Put(PutOptions{Key: []byte("b"), Value: []byte("2")}))
	require.NoError(t, db.Delete([]byte("a")))
	db.crash()

	db2 := openAt(t, dir, 1<<30)
	defer db2.Close()

	_, err := db2.Get([]byte("a"))
	assert.ErrorIs(t, err, ErrNotFound, "delete must survive recovery")
	v, err := db2.Get([]byte("b"))
	require.NoError(t, err)
	assert.Equal(t, []byte("2"), v)
}

func TestRecoverDoubleClean(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Two clean sessions in a row must recover without error.
	db := openAt(t, dir, 1<<20)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
	require.NoError(t, db.Close())

	db2 := openAt(t, dir, 1<<20)
	v, err := db2.Get([]byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), v)
	require.NoError(t, db2.Close())
}

func TestSortTablesByNum(t *testing.T) {
	t.Parallel()
	ts := []manifestTableInfo{{Num: 3}, {Num: 1}, {Num: 2}}
	sortTablesByNum(ts)
	assert.Equal(t, []uint32{1, 2, 3}, []uint32{ts[0].Num, ts[1].Num, ts[2].Num})
}

func TestRestoreTablesMissingFileErrors(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Write and flush data so at least one .sst table exists, then close cleanly.
	db := openAt(t, dir, 512)
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("v")}))
	require.NoError(t, db.Close())

	// Delete one .sst table the manifest references; reopen must fail because
	// restoreTables cannot open the missing table file.
	removed := false
	data := dir
	files, err := os.ReadDir(data)
	require.NoError(t, err)
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".sst" {
			require.NoError(t, os.Remove(filepath.Join(data, f.Name())))
			removed = true
			break
		}
	}
	require.True(t, removed, "expected at least one flushed .sst table")

	o := DefaultOptions(dir)
	o.MemtableSize = 512
	_, err = Open(o)
	assert.Error(t, err)
}

func TestRecoverSecondCrashPreservesRecovered(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	db := openAt(t, dir, 1<<30)
	for i := 0; i < 50; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%02d", i)), Value: []byte("v")}))
	}
	db.crash()

	// First recovery flushes the recovered data to tables, then a second crash.
	db2 := openAt(t, dir, 1<<30)
	db2.crash()

	// Third open must still see everything.
	db3 := openAt(t, dir, 1<<30)
	defer db3.Close()
	for i := 0; i < 50; i++ {
		v, err := db3.Get([]byte(fmt.Sprintf("k%02d", i)))
		require.NoError(t, err, i)
		assert.Equal(t, []byte("v"), v)
	}
}

func TestOpenCorruptManifestErrors(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openAt(t, dir, 1<<20)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
	require.NoError(t, db.Close())

	// Corrupt the live manifest so replay fails on reopen.
	s, err := openStorage(dir)
	require.NoError(t, err)
	num, ok, err := s.readCurrent()
	require.NoError(t, err)
	require.True(t, ok)
	path := s.manifestPath(num)
	require.NoError(t, s.Close())

	// Overwrite with bytes that frame as a journal record but decode to an
	// unknown edit tag, so replayManifestFile returns an error.
	jw := newJournalWriter(mustCreate(t, path))
	require.NoError(t, jw.Write([]byte{0x7f})) // unknown manifest tag
	require.NoError(t, jw.Flush())

	_, err = Open(DefaultOptions(dir))
	require.Error(t, err)
}

func mustCreate(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	return f
}

// firstLog returns the first leftover WAL segment number. Recovery tests write a
// single key then need to locate the WAL segment it landed in.
func firstLog(t *testing.T, s *storageT) uint32 {
	t.Helper()
	logs, err := s.listLogs()
	require.NoError(t, err)
	require.NotEmpty(t, logs, "no leftover WAL segment found")
	return logs[0]
}

// allLogs collects the leftover WAL segment numbers.
func allLogs(t *testing.T, s *storageT) []uint32 {
	t.Helper()
	logs, err := s.listLogs()
	require.NoError(t, err)
	return logs
}

func TestRecoverWALDecodeErrorPropagates(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openAt(t, dir, 1<<30) // large memtable: writes stay in the WAL
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
	// Simulate a crash so the WAL segment is left behind for replay.
	db.crash()

	// Append a well-framed journal record whose payload is an invalid batch
	// (too short) to the leftover WAL, so recoverWAL's decode fails on reopen.
	s, err := openStorage(dir)
	require.NoError(t, err)
	num := firstLog(t, s)
	logPath, err := s.logPath(num)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_APPEND, 0o644)
	require.NoError(t, err)
	jw := newJournalWriter(f)
	require.NoError(t, jw.Write([]byte{0x01, 0x02})) // < 8 bytes -> "short record"
	require.NoError(t, jw.Flush())
	f.Close()

	// Strict recovery rejects the undecodable record.
	_, err = Open(func() Options {
		o := DefaultOptions(dir)
		o.StrictWALRecovery = true
		return o
	}())
	require.Error(t, err)
}

func TestLenientRecoveryKeepsPrefixBeforeCorruption(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openAt(t, dir, 1<<30) // large memtable: writes stay in the WAL
	require.NoError(t, db.Put(PutOptions{Key: []byte("good"), Value: []byte("v")}))
	db.crash()

	// Append an undecodable (well-framed, bad payload) record after the good one.
	s, err := openStorage(dir)
	require.NoError(t, err)
	num := firstLog(t, s)
	logPath, err := s.logPath(num)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_APPEND, 0o644)
	require.NoError(t, err)
	jw := newJournalWriter(f)
	require.NoError(t, jw.Write([]byte{0x01, 0x02}))
	require.NoError(t, jw.Flush())
	require.NoError(t, f.Close())

	// Lenient (default) recovery opens successfully and keeps the intact prefix.
	db2, err := Open(DefaultOptions(dir))
	require.NoError(t, err)
	defer db2.Close()
	v, err := db2.Get([]byte("good"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), v)
}

func TestRecoverWALRejectsSequenceRegressionAcrossSegments(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1 << 30
	opts.StrictWALRecovery = true // reject corruption instead of recovering the prefix
	db, err := Open(opts)
	require.NoError(t, err)
	db.crash()

	s, err := openStorage(dir)
	require.NoError(t, err)
	logs, err := s.listLogs()
	require.NoError(t, err)
	require.Len(t, logs, 1)
	writeSegment := func(path string, seq uint64) {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
		require.NoError(t, err)
		jw := newJournalWriter(f)
		require.NoError(t, jw.Write(encodeBatch(nil, seq, []walEntry{{
			Kind:  walKindPut,
			Key:   []byte{byte(seq)},
			Value: []byte("v"),
		}})))
		require.NoError(t, jw.Flush())
		require.NoError(t, f.Sync())
		require.NoError(t, f.Close())
	}
	seg0, err := s.logPath(logs[0])
	require.NoError(t, err)
	seg1, err := s.logPath(logs[0] + 1)
	require.NoError(t, err)
	writeSegment(seg0, 2)
	writeSegment(seg1, 1)
	require.NoError(t, s.Close())

	_, err = Open(opts)
	assert.ErrorContains(t, err, "non-increasing sequence")
}

// TestRecoverWALRejectsImpossibleKeyMetadata: a valid-CRC record whose contents
// are logically impossible (an empty key) is a semantic corruption signal and
// must fail Open even in lenient mode.
func TestRecoverWALRejectsImpossibleKeyMetadata(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	db, err := Open(opts)
	require.NoError(t, err)
	db.crash()

	s, err := openStorage(dir)
	require.NoError(t, err)
	logs, err := s.listLogs()
	require.NoError(t, err)
	require.Len(t, logs, 1)
	logPath, err := s.logPath(logs[0])
	require.NoError(t, err)
	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	jw := newJournalWriter(f)
	require.NoError(t, jw.Write(encodeBatch(nil, 1, []walEntry{{Kind: walKindPut, Value: []byte("v")}})))
	require.NoError(t, jw.Flush())
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
	require.NoError(t, s.Close())

	_, err = Open(opts)
	assert.ErrorContains(t, err, "empty key")
}

func TestOpenCorruptTableFileErrors(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openAt(t, dir, 256)
	for i := 0; i < 20; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte{byte('a' + i)}, Value: []byte("v")}))
	}
	require.NoError(t, db.Close())

	// Overwrite a live table with garbage so its footer is unreadable;
	// restoreTables -> OpenTable -> newCachedTableReader must fail and Open
	// must return the error.
	s, err := openStorage(dir)
	require.NoError(t, err)
	corrupted := false
	entries, _ := os.ReadDir(s.dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) != tableSuffix {
			continue
		}
		require.NoError(t, os.WriteFile(filepath.Join(s.dir, e.Name()), []byte("garbage"), 0o644))
		corrupted = true
		break
	}
	require.NoError(t, s.Close())
	require.True(t, corrupted, "expected at least one table file to corrupt")

	_, err = Open(DefaultOptions(dir))
	require.Error(t, err)
}

func TestOpenRejectsManifestTableSizeMismatch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value")}))
	require.NoError(t, db.Close())

	data := dir
	files, err := os.ReadDir(data)
	require.NoError(t, err)
	require.NotEmpty(t, files)
	tablePath := filepath.Join(data, files[0].Name())
	f, err := os.OpenFile(tablePath, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.Write([]byte("unmanifested-tail"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = Open(opts)
	assert.ErrorContains(t, err, "size mismatch")
}

func countManifests(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	n := 0
	for _, e := range entries {
		if len(e.Name()) > 9 && e.Name()[:9] == "MANIFEST-" {
			n++
		}
	}
	return n
}

func TestManifestNoAccumulationAcrossRestarts(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	o := DefaultOptions(dir)

	// Several clean open/close cycles plus a crash, then a final open.
	for i := 0; i < 5; i++ {
		db, err := Open(o)
		require.NoError(t, err)
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")}))
		require.NoError(t, db.Close())
	}
	// Crash leaves the session manifest behind as an orphan.
	db, err := Open(o)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("crashed"), Value: []byte("v")}))
	db.crash()

	// Final open must leave exactly one manifest (the live one).
	dbf, err := Open(o)
	require.NoError(t, err)
	defer dbf.Close()
	assert.Equal(t, 1, countManifests(t, dir), "restarts must not accumulate manifests")
}

func TestManifestRotationBoundsGrowth(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	o := DefaultOptions(dir)
	o.MemtableSize = 1 << 30 // flush each batch explicitly to produce manifest edits
	db, err := Open(o)
	require.NoError(t, err)

	for start := 0; start < 120; start += 40 {
		var batch Batch
		for i := start; i < start+40; i++ {
			batch.Put(PutOptions{Key: []byte(fmt.Sprintf("key%05d", i)), Value: []byte("v")})
		}
		require.NoError(t, db.Write(&batch))
		// Put this manifest one edit below rotation without changing a global
		// threshold or generating thousands of unrelated flushes.
		db.manMu.Lock()
		db.man.edits = manifestRotateEdits - 1
		startNum := db.manNum
		db.manMu.Unlock()
		require.NoError(t, db.eng.Flush())
		require.Greater(t, db.manNum, startNum, "manifest should have rotated")
		assert.LessOrEqual(t, countManifests(t, dir), 2, "old manifests must be removed on rotation")
	}
	db.sched.drain()
	require.NoError(t, db.Close())

	// Data survives across all the rotations on reopen.
	db2, err := Open(o)
	require.NoError(t, err)
	defer db2.Close()
	for i := 0; i < 120; i++ {
		v, err := db2.Get([]byte(fmt.Sprintf("key%05d", i)))
		require.NoError(t, err, i)
		assert.Equal(t, []byte("v"), v)
	}
}

func TestOpenManifestKeepsPostRenameStateOnSyncFailure(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := openStorage(dir)
	require.NoError(t, err)
	sentinel := errors.New("directory sync failed")
	store.currentSyncDir = func(string) error { return sentinel }
	output := &tableMeta{num: 99, size: 123}
	db := &DB{
		opts:           DefaultOptions(dir),
		store:          store,
		alloc:          newAllocator(99),
		eng:            &engineT{tables: []*tableMeta{output}},
		startupOutputs: []*tableMeta{output},
	}

	err = db.openManifest()
	assert.ErrorIs(t, err, sentinel)
	require.NotNil(t, db.man, "the manifest named by CURRENT must remain owned")
	assert.Empty(t, db.startupOutputs, "referenced recovery outputs must not be deleted by Open cleanup")
	current, ok, readErr := store.readCurrent()
	require.NoError(t, readErr)
	require.True(t, ok)
	assert.Equal(t, db.manNum, current)
	assert.FileExists(t, store.manifestPath(current))

	require.NoError(t, db.man.Close())
	require.NoError(t, store.Close())
}

func TestManifestRotationAdoptsPostRenameStateOnSyncFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := openStorage(dir)
	require.NoError(t, err)
	db := &DB{
		opts:  DefaultOptions(dir),
		store: store,
		alloc: newAllocator(0),
		eng:   &engineT{},
	}
	require.NoError(t, db.openManifest())
	oldNum := db.manNum

	sentinel := errors.New("directory sync failed")
	store.currentSyncDir = func(string) error { return sentinel }
	db.man.edits = manifestRotateEdits
	db.maybeRotateManifest()
	require.NotEqual(t, oldNum, db.manNum)
	assert.ErrorIs(t, db.backgroundError(), sentinel)
	current, ok, readErr := store.readCurrent()
	require.NoError(t, readErr)
	require.True(t, ok)
	assert.Equal(t, db.manNum, current)
	assert.FileExists(t, store.manifestPath(oldNum), "old manifest is the crash fallback")
	assert.FileExists(t, store.manifestPath(db.manNum))

	require.NoError(t, db.man.Close())
	require.NoError(t, store.Close())
}

func TestCloseRetainsWALWhenManifestRotationSyncFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1 << 30
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value")}))

	sentinel := errors.New("directory sync failed")
	db.store.currentSyncDir = func(string) error { return sentinel }
	db.man.edits = manifestRotateEdits - 1
	err = db.Close()
	assert.ErrorIs(t, err, sentinel)
	logs := allLogs(t, db.store)
	assert.NotEmpty(t, logs, "a late manifest durability failure must prevent WAL retirement")
}

func TestManifestSyncFailurePreservesReferencedFlushOutput(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1 << 30
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value")}))

	sentinel := errors.New("manifest sync failed")
	db.man.syncFile = func() error { return sentinel }
	err = db.eng.Flush()
	assert.ErrorIs(t, err, sentinel)
	db.crash()

	// The edit bytes were flushed before the injected Sync failure and are
	// therefore replayable. Its referenced table must still exist even though
	// the live process treated the commit as failed.
	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()
	value, err := db.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), value)
}

func TestCloseRetainsWALWhenManifestCloseFails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1 << 30
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value")}))

	sentinel := errors.New("manifest close failed")
	db.man.closeFile = func() error {
		_ = db.man.file.Close()
		return sentinel
	}
	err = db.Close()
	assert.ErrorIs(t, err, sentinel)
	logs := allLogs(t, db.store)
	assert.NotEmpty(t, logs, "manifest close failure must be known before WAL retirement")
}

func TestNoSyncBackgroundWALSyncPersistsBeforeCrash(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	o := DefaultOptions(dir)
	o.MemtableSize = 1 << 30 // keep everything in the WAL, none flushed
	o.NoSync = true
	o.WALSyncInterval = 10 * time.Millisecond
	db, err := Open(o)
	require.NoError(t, err)

	for i := 0; i < 50; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%03d", i)), Value: []byte("v")}))
	}
	// Wait for at least one background fsync to flush the page cache to disk.
	time.Sleep(60 * time.Millisecond)
	db.crash()

	db2, err := Open(func() Options { o2 := o; return o2 }())
	require.NoError(t, err)
	defer db2.Close()
	for i := 0; i < 50; i++ {
		v, err := db2.Get([]byte(fmt.Sprintf("k%03d", i)))
		require.NoError(t, err, "key %d", i)
		assert.Equal(t, []byte("v"), v)
	}
}

func TestWALSyncSkipsWhileCommitterActive(t *testing.T) {
	t.Parallel()
	f, err := os.CreateTemp(t.TempDir(), "wal")
	require.NoError(t, err)
	w := newWAL(f, nil, walConfig{sync: false})
	defer w.Close()

	// No committer running: Sync performs the fsync.
	skipped, err := w.syncNow()
	require.NoError(t, err)
	assert.False(t, skipped)

	// Simulate an in-flight committer; Sync must skip rather than block.
	w.mu.Lock()
	w.writing = true
	w.mu.Unlock()
	skipped, err = w.syncNow()
	require.NoError(t, err)
	assert.True(t, skipped, "Sync skips while a committer owns the journal writer")
	w.mu.Lock()
	w.writing = false
	w.mu.Unlock()
}

// TestLenientRecoveryStopsAtCorruptionAcrossSegments is a regression test:
// lenient recovery must halt ALL further replay at the first physical
// corruption, not just the corrupt segment. Records in later segments are past
// the corruption in the ordered WAL stream and must be dropped, or a later Put
// could survive while an earlier (lost) Delete of the same key does not.
func TestLenientRecoveryStopsAtCorruptionAcrossSegments(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1 << 30
	db, err := Open(opts)
	require.NoError(t, err)
	db.crash()

	s, err := openStorage(dir)
	require.NoError(t, err)
	logs, err := s.listLogs()
	require.NoError(t, err)
	require.Len(t, logs, 1)

	// Segment 1: one valid record (seq 1, key "a") then a corrupt tail record.
	seg1, err := s.logPath(logs[0])
	require.NoError(t, err)
	f, err := os.OpenFile(seg1, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	jw := newJournalWriter(f)
	require.NoError(t, jw.Write(encodeBatch(nil, 1, []walEntry{{
		Kind:  walKindPut,
		Key:   []byte("a"),
		Value: []byte("v"),
	}})))
	require.NoError(t, jw.Write([]byte{0x01, 0x02})) // undecodable record
	require.NoError(t, jw.Flush())
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	// Segment 2: a valid record (seq 5, key "b") that lives PAST the corruption.
	seg2, err := s.logPath(logs[0] + 1)
	require.NoError(t, err)
	f2, err := os.OpenFile(seg2, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	jw2 := newJournalWriter(f2)
	require.NoError(t, jw2.Write(encodeBatch(nil, 5, []walEntry{{
		Kind:  walKindPut,
		Key:   []byte("b"),
		Value: []byte("v"),
	}})))
	require.NoError(t, jw2.Flush())
	require.NoError(t, f2.Sync())
	require.NoError(t, f2.Close())
	require.NoError(t, s.Close())

	db2, err := Open(opts) // lenient default
	require.NoError(t, err)
	defer db2.Close()

	// "a" (before the corruption) is recovered.
	v, err := db2.Get([]byte("a"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), v)
	// "b" (in a later segment, past the corruption) must NOT be recovered.
	_, err = db2.Get([]byte("b"))
	assert.ErrorIs(t, err, ErrNotFound, "records past a corruption must not be recovered")
}

// TestRecoveryFlushesMidReplay verifies recovery flushes the memtable when
// it reaches the threshold during replay, rather than accumulating the entire WAL
// into one memtable (which could overflow the skiplist arena). Many recovered
// entries with a tiny MemtableSize must yield multiple tables after reopen.
func TestRecoveryFlushesMidReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	o := DefaultOptions(dir)
	o.MemtableSize = 512 // tiny, so mid-replay flush triggers
	o.NoSync = true
	o.FreshCodec = CodecNone
	o.BottomCodec = CodecNone
	db, err := Open(o)
	require.NoError(t, err)
	const n = 60 // enough for several 512-byte-memtable flushes on replay
	for i := 0; i < n; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("key%04d", i)), Value: []byte("value-payload")}))
	}
	db.crash() // leave the WAL for replay; nothing flushed on crash

	db2, err := Open(o)
	require.NoError(t, err)
	defer db2.Close()

	// All data recovered.
	for i := 0; i < n; i++ {
		v, err := db2.Get([]byte(fmt.Sprintf("key%04d", i)))
		require.NoError(t, err, i)
		assert.Equal(t, []byte("value-payload"), v)
	}
	// Mid-replay flushing produced multiple on-disk tables (arena stayed bounded).
	st, err := db2.Stats()
	require.NoError(t, err)
	assert.Greater(t, st.Tables, 1, "recovery should flush mid-replay into several tables")
}

func TestCheckpointRecoveryBoundarySurvivesManifestRotation(t *testing.T) {
	t.Parallel()
	for _, readOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "writable", true: "read-only"}[readOnly], func(t *testing.T) {
			opts := DefaultOptions(t.TempDir())
			opts.WALRetention = time.Hour
			opts.CompactionFilter = func(CompactionFilterEntry) bool { return false }
			db, err := Open(opts)
			require.NoError(t, err)
			require.NoError(t, db.Put(PutOptions{Key: []byte("drop"), Value: []byte("v")}))
			require.NoError(t, db.CompactRange(nil, nil))
			// Rotate without changing the package-wide threshold used by parallel tests.
			db.manMu.Lock()
			db.man.edits = manifestRotateEdits
			db.maybeRotateManifest()
			db.manMu.Unlock()
			require.NoError(t, db.backgroundError())
			// This write is not covered by the checkpoint and still needs replay.
			require.NoError(t, db.Put(PutOptions{Key: []byte("live"), Value: []byte("new")}))
			db.crash()
			opts.ReadOnly = readOnly
			db, err = Open(opts)
			require.NoError(t, err)
			defer db.Close()
			_, err = db.Get([]byte("drop"))
			require.ErrorIs(t, err, ErrNotFound)
			v, err := db.Get([]byte("live"))
			require.NoError(t, err)
			require.Equal(t, "new", string(v))
		})
	}
}

func TestCheckpointManifestFailurePreservesRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.CompactionFilter = func(CompactionFilterEntry) bool { return false }
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
	// Flush first so the next manifest append is the recovery boundary itself.
	require.NoError(t, db.eng.Flush())
	db.man.syncFile = func() error { return errors.New("boundary sync failure") }
	_, err = db.checkpointWALMode(true)
	require.ErrorContains(t, err, "boundary sync failure")
	require.Zero(t, db.filterSafeSeq.Load())
	require.Error(t, db.CompactRange(nil, nil))
	db.crash()
	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()
	v, err := db.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, "v", string(v))
}

func TestLegacyManifestStillReplaysUnflushedWAL(t *testing.T) {
	t.Parallel()
	opts := DefaultOptions(t.TempDir())
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
	// Older baselines contained LastSeq but no recovery boundary. LastSeq alone
	// must not cause replay to skip a write whose only durable copy is the WAL.
	old := db.man
	db.replayLogNum = 0
	require.NoError(t, db.openManifest())
	require.NoError(t, old.Close())
	db.crash()
	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()
	v, err := db.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, "v", string(v))
}

func TestRecoveryPreservesAllocatedSequence(t *testing.T) {
	t.Parallel()
	for _, rotate := range []bool{false, true} {
		t.Run(fmt.Sprint("rotate=", rotate), func(t *testing.T) {
			t.Parallel()
			opts := DefaultOptions(t.TempDir())
			opts.MemtableSize = 1 << 30
			db, err := Open(opts)
			require.NoError(t, err)
			apply := db.wal.wal.apply
			db.wal.wal.apply = func(es []walEntry) {
				// Model a background flush after the first entry of a durable batch has
				// applied, but before applyCommitted publishes the batch's final sequence.
				db.eng.Put(es[0].Seq, es[0].Key, es[0].Value)
				require.NoError(t, db.eng.Flush())
				if rotate {
					db.manMu.Lock()
					db.man.edits = manifestRotateEdits
					db.maybeRotateManifest()
					db.manMu.Unlock()
				}
				apply(es[1:])
			}
			var b Batch
			b.Put(PutOptions{Key: []byte("a"), Value: []byte("old")})
			b.Put(PutOptions{Key: []byte("b"), Value: []byte("other")})
			require.NoError(t, db.Write(&b))
			st, err := db.replayManifest(db.manNum)
			require.NoError(t, err)
			require.Equal(t, uint64(2), st.LastSeq)
			pth := db.walPath(db.wal.num)
			db.crash()
			raw, err := os.ReadFile(pth)
			require.NoError(t, err)
			raw[len(raw)-1] ^= 1
			require.NoError(t, os.WriteFile(pth, raw, 0o600))
			db, err = Open(opts)
			require.NoError(t, err)
			defer db.Close()
			require.NoError(t, db.Put(PutOptions{Key: []byte("a"), Value: []byte("new")}))
			v, err := db.Get([]byte("a"))
			require.NoError(t, err, "a successful new write must not reuse an existing SST sequence")
			require.Equal(t, "new", string(v))
			require.Equal(t, uint64(3), db.LatestSeq())
		})
	}
}
