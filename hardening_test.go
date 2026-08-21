package levisdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type blockingSecondShardCall struct {
	mu      sync.Mutex
	calls   int
	blocked chan struct{}
	release chan struct{}
}

func (p *blockingSecondShardCall) Shard([]byte, int) int {
	p.mu.Lock()
	p.calls++
	call := p.calls
	p.mu.Unlock()
	if call == 2 {
		close(p.blocked)
		<-p.release
	}
	return 0
}

func (*blockingSecondShardCall) Name() string { return "blocking-second-shard-call" }

func TestPointReadPinsCapturedSequenceAgainstCompaction(t *testing.T) {
	for _, tc := range []struct {
		name string
		read func(*DB) error
	}{
		{
			name: "Get",
			read: func(db *DB) error {
				value, err := db.Get([]byte("key"))
				if err == nil && !bytes.Equal(value, []byte("old")) {
					return fmt.Errorf("Get returned %q", value)
				}
				return err
			},
		},
		{
			name: "Has",
			read: func(db *DB) error {
				has, err := db.Has([]byte("key"))
				if err == nil && !has {
					return fmt.Errorf("Has returned false")
				}
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			part := &blockingSecondShardCall{
				blocked: make(chan struct{}),
				release: make(chan struct{}),
			}
			db := openTestDB(t, func(o *Options) {
				o.ShardCount = 1
				o.MemtableSize = 1 << 30
				o.CustomPartitioner = part
			})
			released := false
			defer func() {
				if !released {
					close(part.release)
				}
			}()
			require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("old")}))
			require.NoError(t, db.shards[0].Flush())

			result := make(chan error, 1)
			go func() { result <- tc.read(db) }()
			<-part.blocked // the read captured and pinned sequence one

			require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("new")}))
			require.NoError(t, db.shards[0].Flush())
			require.NoError(t, db.CompactShard(0))
			close(part.release)
			released = true
			require.NoError(t, <-result)
			assert.Zero(t, db.snaps.live(), "point-read sequence pin must be released")
		})
	}
}

func TestRecoverWALAcrossJournalFlushBoundary(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openAt(t, dir, 1<<30)
	wantA := make([]byte, 10<<10)
	wantB := make([]byte, 30<<10)
	for i := range wantA {
		wantA[i] = byte(i)
	}
	for i := range wantB {
		wantB[i] = byte(i * 3)
	}
	require.NoError(t, db.Put(PutOptions{Key: []byte("a"), Value: wantA}))
	require.NoError(t, db.Put(PutOptions{Key: []byte("b"), Value: wantB}))
	db.crash()

	db, err := Open(func() Options {
		o := DefaultOptions(dir)
		o.ShardCount = 4
		o.MemtableSize = 1 << 30
		return o
	}())
	require.NoError(t, err)
	defer db.Close()
	gotA, err := db.Get([]byte("a"))
	require.NoError(t, err)
	gotB, err := db.Get([]byte("b"))
	require.NoError(t, err)
	assert.Equal(t, wantA, gotA)
	assert.Equal(t, wantB, gotB)
}

func TestWALAppliesConcurrentBatchesInCommitOrder(t *testing.T) {
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })
	originalApply := db.wal.apply
	entered := make(chan struct{})
	release := make(chan struct{})
	db.wal.apply = func(entries []walEntry) {
		if len(entries) > 1 {
			close(entered)
			<-release
		}
		originalApply(entries)
	}

	var large Batch
	for i := 0; i < 100; i++ {
		large.Put(PutOptions{Key: []byte(fmt.Sprintf("bulk-%03d", i)), Value: []byte("v")})
	}
	largeDone := make(chan error, 1)
	go func() { largeDone <- db.Write(&large) }()
	<-entered

	laterDone := make(chan error, 1)
	go func() { laterDone <- db.Put(PutOptions{Key: []byte("later"), Value: []byte("v")}) }()
	select {
	case err := <-laterDone:
		require.NoError(t, err)
		t.Fatal("later batch returned before the earlier batch was fully applied")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-largeDone)
	require.NoError(t, <-laterDone)
	for i := 0; i < 100; i++ {
		_, err := db.Get([]byte(fmt.Sprintf("bulk-%03d", i)))
		require.NoError(t, err)
	}
}

func TestConcurrentIteratorAndWrites(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.ShardCount = 1
		o.MemtableSize = 1 << 30
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k-%04d", i)), Value: []byte("value")}))
		}
	}()
	for pass := 0; pass < 40; pass++ {
		it, err := db.NewIterator()
		require.NoError(t, err)
		var prev []byte
		for it.Next() {
			key := append([]byte(nil), it.Key()...)
			if prev != nil {
				assert.Less(t, string(prev), string(key))
			}
			prev = key
		}
		require.NoError(t, it.Error())
		require.NoError(t, it.Close())
	}
	wg.Wait()
}

func TestCleanCloseRetiresWALWithoutDuplicateTables(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	o := DefaultOptions(dir)
	o.ShardCount = 1
	o.MemtableSize = 1 << 30
	db, err := Open(o)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
	require.NoError(t, db.Close())

	logs, err := os.ReadDir(filepath.Join(dir, walDir))
	require.NoError(t, err)
	assert.Empty(t, logs)

	db, err = Open(o)
	require.NoError(t, err)
	st, err := db.Stats()
	require.NoError(t, err)
	assert.Equal(t, 1, st.Tables)
	require.NoError(t, db.Close())
}

type fileFingerprint struct {
	size int64
	mode os.FileMode
	mod  time.Time
}

func fingerprintTree(t *testing.T, root string) map[string]fileFingerprint {
	t.Helper()
	out := map[string]fileFingerprint{}
	require.NoError(t, filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		out[rel] = fileFingerprint{size: info.Size(), mode: info.Mode(), mod: info.ModTime()}
		return nil
	}))
	return out
}

func TestReadOnlyOpenDoesNotMutateRecoveryState(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := openAt(t, dir, 1<<30)
	require.NoError(t, db.Put(PutOptions{Key: []byte("wal-only"), Value: []byte("value")}))
	db.crash()
	before := fingerprintTree(t, dir)

	o := DefaultOptions(dir)
	o.ShardCount = 4
	o.MemtableSize = 1 << 30
	o.ReadOnly = true
	ro, err := Open(o)
	require.NoError(t, err)
	v, err := ro.Get([]byte("wal-only"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), v)
	require.NoError(t, ro.Close())

	assert.Equal(t, before, fingerprintTree(t, dir))
}

func TestReadOnlyLargeWALReplayStaysInMemory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeOpts := DefaultOptions(dir)
	writeOpts.ShardCount = 1
	writeOpts.MemtableSize = 1 << 30
	// The crash helper syncs the WAL before closing it. Avoid an unrelated fsync
	// for every setup write so this recovery regression stays within the package's
	// test-duration budget.
	writeOpts.NoSync = true
	db, err := Open(writeOpts)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("changing"), Value: []byte("old")}))
	for i := 0; i < 200; i++ {
		require.NoError(t, db.Put(PutOptions{
			Key:   []byte(fmt.Sprintf("key-%04d", i)),
			Value: []byte("value-payload"),
		}))
		if i == 99 {
			require.NoError(t, db.Delete([]byte("changing")))
		}
	}
	require.NoError(t, db.Put(PutOptions{Key: []byte("changing"), Value: []byte("new")}))
	db.crash()
	before := fingerprintTree(t, dir)

	readOpts := writeOpts
	readOpts.MemtableSize = 128
	readOpts.ReadOnly = true
	ro, err := Open(readOpts)
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		value, err := ro.Get([]byte(fmt.Sprintf("key-%04d", i)))
		require.NoError(t, err, i)
		assert.Equal(t, []byte("value-payload"), value)
	}
	value, err := ro.Get([]byte("changing"))
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), value)
	has, err := ro.Has([]byte("changing"))
	require.NoError(t, err)
	assert.True(t, has)
	it, err := ro.NewIterator()
	require.NoError(t, err)
	count := 0
	for it.Next() {
		count++
	}
	require.NoError(t, it.Error())
	require.NoError(t, it.Close())
	assert.Equal(t, 201, count)
	require.NoError(t, ro.Close())

	assert.Equal(t, before, fingerprintTree(t, dir))
}

func TestWALCheckpointBoundsSegmentsAndSurvivesCrash(t *testing.T) {
	oldLimit := walSegmentBytes
	walSegmentBytes = 1024
	defer func() { walSegmentBytes = oldLimit }()

	dir := t.TempDir()
	o := DefaultOptions(dir)
	o.ShardCount = 2
	o.MemtableSize = 1 << 30
	db, err := Open(o)
	require.NoError(t, err)
	value := make([]byte, 700)
	for i := 0; i < 20; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("key-%02d", i)), Value: value}))
	}
	logs, err := os.ReadDir(filepath.Join(dir, walDir))
	require.NoError(t, err)
	require.Len(t, logs, 1, "checkpoint must retire every old segment")
	db.crash()

	db, err = Open(o)
	require.NoError(t, err)
	defer db.Close()
	for i := 0; i < 20; i++ {
		got, err := db.Get([]byte(fmt.Sprintf("key-%02d", i)))
		require.NoError(t, err)
		assert.Equal(t, value, got)
	}
}

func TestBackgroundErrorIsStickyAndObservable(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	want := errors.New("disk stopped")
	db.setBackgroundError(want)
	err := db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")})
	assert.ErrorIs(t, err, want)
	st, err := db.Stats()
	require.NoError(t, err)
	assert.ErrorIs(t, st.BackgroundError, want)
	prop, err := db.GetProperty("levisdb.background-error")
	require.NoError(t, err)
	assert.Contains(t, prop, want.Error())
}

func TestDisableBlockCache(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.DisableBlockCache = true })
	assert.Zero(t, db.cache.capacity)
}

func TestCompactionFileSizeTargetsAndSplitting(t *testing.T) {
	t.Parallel()
	cc := testCompactionConfig()
	cc.FileSizeBase = 256
	cc.FileSizeMultiplier = 2
	cc.FileSizeMax = 700
	assert.Equal(t, int64(256), cc.targetFileSize(0))
	assert.Equal(t, int64(512), cc.targetFileSize(1))
	assert.Equal(t, int64(700), cc.targetFileSize(2))
	assert.Equal(t, int64(700), cc.targetFileSize(20))

	s := newTestShard(t, 1<<30)
	value := make([]byte, 128)
	const entriesPerTable = 10
	for table := 0; table < 2; table++ {
		for i := 0; i < entriesPerTable; i++ {
			key := []byte(fmt.Sprintf("t%d-key-%03d", table, i))
			s.Put(uint64(table*entriesPerTable+i+1), key, value)
		}
		require.NoError(t, s.Flush())
	}
	require.NoError(t, s.Compact(0, maxIKeySeq, cc))
	tables := s.Tables()
	assert.Greater(t, len(tables), 1)
	for _, table := range tables {
		assert.Equal(t, 1, table.Depth)
	}
}

func TestCompactionReadFailureKeepsInputs(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	flushSingle(t, s, 1, "a", "1")
	flushSingle(t, s, 2, "b", "2")
	before := s.Tables()
	require.Len(t, before, 2)
	s.mu.Lock()
	s.tables[0].reader.r = errReaderAt{}
	s.mu.Unlock()

	err := s.Compact(0, maxIKeySeq, testCompactionConfig())
	assert.ErrorIs(t, err, errRead)
	after := s.Tables()
	require.Len(t, after, 2)
	nums := []uint32{after[0].Num, after[1].Num}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	wantNums := []uint32{before[0].Num, before[1].Num}
	sort.Slice(wantNums, func(i, j int) bool { return wantNums[i] < wantNums[j] })
	assert.Equal(t, wantNums, nums)
	s.mu.RLock()
	paths := make([]string, len(s.tables))
	for i, table := range s.tables {
		paths[i] = table.path
	}
	s.mu.RUnlock()
	for _, path := range paths {
		_, statErr := os.Stat(path)
		assert.NoError(t, statErr)
	}
}

func TestFlushManifestFailureKeepsImmutableForRetry(t *testing.T) {
	t.Parallel()
	want := errors.New("manifest unavailable")
	s := newTestShard(t, 1<<20)
	s.cfg.Commit = func([]*tableMeta, []*tableMeta, func()) error { return want }
	s.Put(1, []byte("old"), []byte("value"))
	err := s.Flush()
	assert.ErrorIs(t, err, want)
	assert.Empty(t, s.Tables())
	ambiguousOutput, pathErr := s.tablePath(1)
	require.NoError(t, pathErr)
	assert.FileExists(t, ambiguousOutput, "an ambiguously committed manifest edit may reference this output")

	// Writes continue in the fresh active memtable while the failed immutable
	// remains available for a later retry.
	s.Put(2, []byte("new"), []byte("value"))
	s.cfg.Commit = nil
	require.NoError(t, s.Flush())
	require.NoError(t, s.Flush())
	assert.Equal(t, []byte("value"), mustGet(t, s, 10, "old"))
	assert.Equal(t, []byte("value"), mustGet(t, s, 10, "new"))
}

func TestCompactionManifestFailureKeepsSourceTables(t *testing.T) {
	t.Parallel()
	want := errors.New("manifest unavailable")
	s := newTestShard(t, 1<<20)
	flushSingle(t, s, 1, "a", "1")
	flushSingle(t, s, 2, "b", "2")
	before := s.Tables()
	s.cfg.Commit = func([]*tableMeta, []*tableMeta, func()) error { return want }

	err := s.Compact(0, maxIKeySeq, testCompactionConfig())
	assert.ErrorIs(t, err, want)
	assert.Equal(t, before, s.Tables())
	ambiguousOutput, pathErr := s.tablePath(3)
	require.NoError(t, pathErr)
	assert.FileExists(t, ambiguousOutput, "an ambiguously committed replacement may reference this output")
	assert.Equal(t, []byte("1"), mustGet(t, s, 10, "a"))
	assert.Equal(t, []byte("2"), mustGet(t, s, 10, "b"))
}

func TestIteratorPinsSequenceUntilClosed(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))

	it, err := db.NewIterator()
	require.NoError(t, err)
	assert.Equal(t, 1, db.snaps.live(), "ordinary iterator must pin its read sequence")
	require.NoError(t, it.Close())
	assert.Zero(t, db.snaps.live())

	snap, err := db.Snapshot()
	require.NoError(t, err)
	it, err = snap.NewIterator()
	require.NoError(t, err)
	assert.Equal(t, 2, db.snaps.live(), "snapshot and its iterator own independent pins")
	snap.Release()
	assert.Equal(t, 1, db.snaps.live())
	require.NoError(t, it.Close())
	assert.Zero(t, db.snaps.live())
}

func TestExhaustedIteratorReleasesSequencePin(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	it, err := db.NewIterator()
	require.NoError(t, err)
	assert.Equal(t, 1, db.snaps.live())
	assert.False(t, it.Next())
	assert.Zero(t, db.snaps.live())
	require.NoError(t, it.Close())
}

func TestCompactionDropsVersionAtRetentionBoundary(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	flushSingle(t, s, 1, "k", "v1")
	flushSingle(t, s, 3, "k", "v3")
	require.NoError(t, s.Compact(0, 3, testCompactionConfig()))

	count := 0
	for _, table := range s.tables {
		it := table.reader.newIterator()
		for it.Next() {
			count++
		}
		require.NoError(t, it.Error())
	}
	assert.Equal(t, 1, count, "a version exactly at retainSeq already satisfies the oldest snapshot")
	assert.Equal(t, []byte("v3"), mustGet(t, s, 3, "k"))
}

func TestCompactionDeduplicatesReplayedInternalKeys(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	flushSingle(t, s, 7, "k", "value")
	flushSingle(t, s, 7, "k", "value")
	require.NoError(t, s.Compact(0, maxIKeySeq, testCompactionConfig()))

	count := 0
	for _, table := range s.tables {
		it := table.reader.newIterator()
		for it.Next() {
			count++
		}
		require.NoError(t, it.Error())
	}
	assert.Equal(t, 1, count)
	assert.Equal(t, []byte("value"), mustGet(t, s, 7, "k"))
}

func TestDeepCompactionOutputCannotShadowNewerShallowTable(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	cc := testCompactionConfig()

	// Build two old depth-1 tables.
	flushSingle(t, s, 1, "value-key", "old")
	flushSingle(t, s, 2, "old-a", "value")
	require.NoError(t, s.Compact(0, maxIKeySeq, cc))
	flushSingle(t, s, 3, "deleted-key", "old")
	flushSingle(t, s, 4, "old-b", "value")
	require.NoError(t, s.Compact(0, maxIKeySeq, cc))

	// Leave genuinely newer mutations in a shallow table.
	s.Put(5, []byte("value-key"), []byte("new"))
	s.del(6, []byte("deleted-key"))
	require.NoError(t, s.Flush())

	// Compacting the two old depth-1 tables creates a file after the shallow
	// table. File creation order must not make its old versions win point reads.
	require.NoError(t, s.Compact(1, maxIKeySeq, cc))
	assert.Equal(t, []byte("new"), mustGet(t, s, maxIKeySeq, "value-key"))
	has, deleted, err := s.has(maxIKeySeq, []byte("deleted-key"))
	require.NoError(t, err)
	assert.True(t, has)
	assert.True(t, deleted)
	_, found, deleted, err := s.get(maxIKeySeq, []byte("deleted-key"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, deleted)
}

func TestReplayedOlderMemtableEntryCannotShadowNewerTable(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	s.Put(10, []byte("value-key"), []byte("new"))
	s.del(11, []byte("deleted-key"))
	require.NoError(t, s.Flush())

	// Recovery deliberately replays intact WAL entries even when an identical
	// or newer version is already manifest-backed. Model an older surviving WAL
	// prefix in the active memtable.
	s.Put(5, []byte("value-key"), []byte("old"))
	s.Put(6, []byte("deleted-key"), []byte("old"))

	assert.Equal(t, []byte("new"), mustGet(t, s, maxIKeySeq, "value-key"))
	_, found, deleted, err := s.get(maxIKeySeq, []byte("deleted-key"))
	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, deleted)
	has, deleted, err := s.has(maxIKeySeq, []byte("deleted-key"))
	require.NoError(t, err)
	assert.True(t, has)
	assert.True(t, deleted)
}

func TestCompactAllRewritesSingleTable(t *testing.T) {
	t.Parallel()
	s := newTestShard(t, 1<<20)
	s.Put(1, []byte("gone"), []byte("value"))
	s.del(2, []byte("gone"))
	require.NoError(t, s.Flush())
	require.Len(t, s.Tables(), 1)

	require.NoError(t, s.CompactAll(maxIKeySeq, testCompactionConfig()))
	assert.Empty(t, s.Tables(), "full compaction must reclaim a lone bottom-table tombstone")
	_, found, _, err := s.get(maxIKeySeq, []byte("gone"))
	require.NoError(t, err)
	assert.False(t, found)
}

func TestCloseWaitsForManualCompaction(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.ShardCount = 1
		o.MemtableSize = 1 << 30
		o.TierRatio = 100
	})
	require.NoError(t, db.Put(PutOptions{Key: []byte("a"), Value: []byte("1")}))
	require.NoError(t, db.shards[0].Flush())
	require.NoError(t, db.Put(PutOptions{Key: []byte("b"), Value: []byte("2")}))
	require.NoError(t, db.shards[0].Flush())

	originalCommit := db.shards[0].cfg.Commit
	entered := make(chan struct{})
	release := make(chan struct{})
	db.shards[0].cfg.Commit = func(inputs, outputs []*tableMeta, install func()) error {
		close(entered)
		<-release
		return originalCommit(inputs, outputs, install)
	}
	compactDone := make(chan error, 1)
	go func() { compactDone <- db.CompactShard(0) }()
	<-entered

	closeDone := make(chan error, 1)
	go func() { closeDone <- db.Close() }()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
		t.Fatal("Close returned while manual compaction still owned table and manifest resources")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-compactDone)
	require.NoError(t, <-closeDone)
}

func TestFailedRecoveryRemovesUnmanifestedTables(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.ShardCount = 2
	opts.Partitioner = PartitionerRange
	opts.MemtableSize = 1 << 30
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte{0x01}, Value: []byte("left")}))
	require.NoError(t, db.Put(PutOptions{Key: []byte{0xff}, Value: []byte("right")}))
	db.crash()

	blockedShard := filepath.Join(dir, shardsDir, "01")
	require.NoError(t, os.WriteFile(blockedShard, []byte("not a directory"), 0o644))
	_, err = Open(opts)
	require.Error(t, err)

	var tables []string
	require.NoError(t, filepath.WalkDir(filepath.Join(dir, shardsDir), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if filepath.Ext(entry.Name()) == ".sst" {
			tables = append(tables, path)
		}
		return nil
	}))
	assert.Empty(t, tables, "recovery outputs without a manifest must be rolled back")

	require.NoError(t, os.Remove(blockedShard))
	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()
	left, err := db.Get([]byte{0x01})
	require.NoError(t, err)
	right, err := db.Get([]byte{0xff})
	require.NoError(t, err)
	assert.Equal(t, []byte("left"), left)
	assert.Equal(t, []byte("right"), right)
}

func TestGetResultCannotMutateMemtable(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 1 << 30 })
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value")}))
	got, err := db.Get([]byte("key"))
	require.NoError(t, err)
	got[0] = 'X'

	again, err := db.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), again)
}

func TestReadOnlyLocksAreSharedAndExcludeWriter(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.ShardCount = 1
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	readOnly := opts
	readOnly.ReadOnly = true
	first, err := Open(readOnly)
	require.NoError(t, err)
	second, err := Open(readOnly)
	require.NoError(t, err)
	_, err = Open(opts)
	assert.Error(t, err, "writer must not open while shared readers hold the lock")
	require.NoError(t, first.Close())
	_, err = Open(opts)
	assert.Error(t, err, "remaining reader must still exclude the writer")
	require.NoError(t, second.Close())

	db, err = Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

type invalidShardPartitioner struct{ shard int }

func (p invalidShardPartitioner) Shard([]byte, int) int { return p.shard }
func (invalidShardPartitioner) Name() string            { return "invalid-shard-test-v1" }

func TestInvalidCustomPartitionerReturnsErrorsOnEveryPointAPI(t *testing.T) {
	t.Parallel()
	for _, shard := range []int{-1, 2} {
		t.Run(fmt.Sprintf("shard-%d", shard), func(t *testing.T) {
			opts := DefaultOptions(t.TempDir())
			opts.ShardCount = 2
			opts.CustomPartitioner = invalidShardPartitioner{shard: shard}
			db, err := Open(opts)
			require.NoError(t, err)
			defer db.Close()

			assert.ErrorContains(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}), "partitioner returned shard")
			_, err = db.Get([]byte("k"))
			assert.ErrorContains(t, err, "partitioner returned shard")
			_, err = db.Has([]byte("k"))
			assert.ErrorContains(t, err, "partitioner returned shard")
			snap, err := db.Snapshot()
			require.NoError(t, err)
			defer snap.Release()
			_, err = snap.Get([]byte("k"))
			assert.ErrorContains(t, err, "partitioner returned shard")
			_, err = snap.Has([]byte("k"))
			assert.ErrorContains(t, err, "partitioner returned shard")
		})
	}
}

func TestWritableOpenRemovesOnlyOrphanCanonicalTables(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.ShardCount = 1
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("live"), Value: []byte("value")}))
	require.NoError(t, db.Close())

	shardDir := filepath.Join(dir, shardsDir, "00")
	orphan := filepath.Join(shardDir, "00fffffe.sst")
	nonCanonical := filepath.Join(shardDir, "00FFFFFD.sst")
	unrelated := filepath.Join(shardDir, "keep.tmp")
	require.NoError(t, os.WriteFile(orphan, []byte("orphan"), 0o644))
	require.NoError(t, os.WriteFile(nonCanonical, []byte("not-engine-owned"), 0o644))
	require.NoError(t, os.WriteFile(unrelated, []byte("not-engine-owned"), 0o644))

	db, err = Open(opts)
	require.NoError(t, err)
	value, err := db.Get([]byte("live"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), value)
	assert.NoFileExists(t, orphan)
	assert.FileExists(t, nonCanonical)
	assert.FileExists(t, unrelated)
	require.NoError(t, db.Close())
}

func TestReadOnlyOpenPreservesOrphanTables(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.ShardCount = 1
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	shardDir, err := db.store.shardDir(0)
	require.NoError(t, err)
	orphan := filepath.Join(shardDir, "00fffffe.sst")
	require.NoError(t, os.WriteFile(orphan, []byte("orphan"), 0o644))

	opts.ReadOnly = true
	ro, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, ro.Close())
	assert.FileExists(t, orphan, "read-only open must never run orphan cleanup")
}

func TestFlushWorkerDrainsMemtableFilledDuringActiveFlush(t *testing.T) {
	db := openTestDB(t, func(o *Options) {
		o.ShardCount = 1
		o.MemtableSize = 1
		o.TierRatio = 100
	})

	originalCommit := db.shards[0].cfg.Commit
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	db.shards[0].cfg.Commit = func(inputs, outputs []*tableMeta, install func()) error {
		once.Do(func() {
			close(entered)
			<-release
		})
		return originalCommit(inputs, outputs, install)
	}

	require.NoError(t, db.Put(PutOptions{Key: []byte("first"), Value: []byte("value")}))
	<-entered
	require.NoError(t, db.Put(PutOptions{Key: []byte("second"), Value: []byte("value")}))
	close(release)
	db.sched.drain()

	assert.True(t, db.shards[0].memEmpty(), "worker must drain the over-limit replacement memtable without a third write")
	assert.Len(t, db.shards[0].Tables(), 2)
}

func TestLegacyUnversionedPartitionerIdentityIsRejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := openStorage(dir)
	require.NoError(t, err)
	manifest, err := createManifest(store.manifestPath(1), "hash", 1)
	require.NoError(t, err)
	require.NoError(t, manifest.Close())
	require.NoError(t, store.setCurrent(1))
	require.NoError(t, store.Close())

	opts := DefaultOptions(dir)
	opts.ShardCount = 1
	_, err = Open(opts)
	assert.ErrorIs(t, err, ErrPartitionerMismatch)
}
