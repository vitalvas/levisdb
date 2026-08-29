package levisdb

import (
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestDB(t *testing.T, mutate func(*Options)) *DB {
	t.Helper()
	o := DefaultOptions(t.TempDir())
	o.MemtableSize = 1024 // small, to exercise flush
	o.NoSync = true       // logical tests do not need durable fsyncs; keeps them fast
	// Skip compression by default: logic tests do not need it and zstd/s2 CPU
	// dominates test time. Codec behavior has its own dedicated tests, and a
	// caller can re-enable a codec via mutate.
	o.FreshCodec = CodecNone
	o.BottomCodec = CodecNone
	if mutate != nil {
		mutate(&o)
	}
	db, err := Open(o)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestPutGetDelete(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)

	require.NoError(t, db.Put(PutOptions{Key: []byte("hello"), Value: []byte("world")}))
	v, err := db.Get([]byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, []byte("world"), v)

	require.NoError(t, db.Delete([]byte("hello")))
	_, err = db.Get([]byte("hello"))
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestGetMissing(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	_, err := db.Get([]byte("nope"))
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestOverwrite(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v1")}))
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v2")}))
	v, err := db.Get([]byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), v)
}

func TestManyKeysAndFlush(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 4096 })

	const n = 120
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%05d", i))
		require.NoError(t, db.Put(PutOptions{Key: k, Value: []byte(fmt.Sprintf("v%d", i))}))
	}
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%05d", i))
		v, err := db.Get(k)
		require.NoError(t, err, i)
		assert.Equal(t, []byte(fmt.Sprintf("v%d", i)), v)
	}
}

func TestBatchAtomicApply(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	var b Batch
	b.Put(PutOptions{Key: []byte("a"), Value: []byte("1")})
	b.Put(PutOptions{Key: []byte("b"), Value: []byte("2")})
	b.Delete([]byte("a"))
	require.NoError(t, db.Write(&b))

	_, err := db.Get([]byte("a"))
	assert.ErrorIs(t, err, ErrNotFound)
	v, err := db.Get([]byte("b"))
	require.NoError(t, err)
	assert.Equal(t, []byte("2"), v)
}

func TestIteratorSorted(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 2048 })

	want := make([]string, 0, 500)
	for i := 0; i < 150; i++ {
		k := fmt.Sprintf("k%04d", i)
		want = append(want, k)
		require.NoError(t, db.Put(PutOptions{Key: []byte(k), Value: []byte("v")}))
	}
	sort.Strings(want)

	it, err := db.NewIterator()
	require.NoError(t, err)
	defer it.Close()

	var got []string
	for it.Next() {
		got = append(got, string(it.Key()))
	}
	require.NoError(t, it.Error())
	assert.Equal(t, want, got)
}

func TestIteratorSkipsDeleted(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	require.NoError(t, db.Put(PutOptions{Key: []byte("a"), Value: []byte("1")}))
	require.NoError(t, db.Put(PutOptions{Key: []byte("b"), Value: []byte("2")}))
	require.NoError(t, db.Delete([]byte("a")))

	it, err := db.NewIterator()
	require.NoError(t, err)
	defer it.Close()

	var keys []string
	for it.Next() {
		keys = append(keys, string(it.Key()))
	}
	assert.Equal(t, []string{"b"}, keys)
}

func TestClosedRejectsOps(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	require.NoError(t, db.Close())
	require.NoError(t, db.Close()) // idempotent

	assert.ErrorIs(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}), ErrClosed)
	_, err := db.Get([]byte("k"))
	assert.ErrorIs(t, err, ErrClosed)
}

func TestEmptyKeyRejected(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	assert.ErrorIs(t, db.Put(PutOptions{Key: nil, Value: []byte("v")}), ErrEmptyKey)
	_, err := db.Get(nil)
	assert.ErrorIs(t, err, ErrEmptyKey)
}

func TestReadOnlyRejectsWrites(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db, err := Open(DefaultOptions(dir))
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
	require.NoError(t, db.Close())

	ro := DefaultOptions(dir)
	ro.ReadOnly = true
	rodb, err := Open(ro)
	require.NoError(t, err)
	defer rodb.Close()

	assert.ErrorIs(t, rodb.Put(PutOptions{Key: []byte("x"), Value: []byte("y")}), ErrReadOnly)
	// Reads still work.
	v, err := rodb.Get([]byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), v)
}

func TestReopenRestoresData(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	open := func() *DB {
		o := DefaultOptions(dir)
		o.MemtableSize = 1024
		o.NoSync = true // reopen/restore test; durability fsync not under test
		o.FreshCodec = CodecNone
		o.BottomCodec = CodecNone
		db, err := Open(o)
		require.NoError(t, err)
		return db
	}

	db := open()
	for i := 0; i < 100; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("key%03d", i)), Value: []byte(fmt.Sprintf("v%d", i))}))
	}
	require.NoError(t, db.Close())

	// Reopen: flush-on-close persisted every key; the manifest restores them.
	db2 := open()
	defer db2.Close()
	for i := 0; i < 100; i++ {
		v, err := db2.Get([]byte(fmt.Sprintf("key%03d", i)))
		require.NoError(t, err, i)
		assert.Equal(t, []byte(fmt.Sprintf("v%d", i)), v)
	}
}

func TestWriteEmptyBatchIsNoOp(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	var b Batch
	assert.NoError(t, db.Write(&b))
}

func TestNoSyncStillReadsBack(t *testing.T) {
	t.Parallel()
	// With NoSync the WAL skips per-group fsync, but writes remain logically
	// visible within the running process (durability is what changes, not
	// visibility).
	db := openTestDB(t, func(o *Options) { o.NoSync = true })
	for i := 0; i < 40; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%02d", i)), Value: []byte("v")}))
	}
	for i := 0; i < 40; i++ {
		v, err := db.Get([]byte(fmt.Sprintf("k%02d", i)))
		require.NoError(t, err, i)
		assert.Equal(t, []byte("v"), v)
	}
}

func TestWALObserverReceivesWrites(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var seen []WALEntry
	obs := observerFunc(func(batch []WALEntry) {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range batch {
			cp := e
			cp.Key = append([]byte(nil), e.Key...)
			seen = append(seen, cp)
		}
	})

	db := openTestDB(t, func(o *Options) { o.WALObserver = obs })
	require.NoError(t, db.Put(PutOptions{Key: []byte("a"), Value: []byte("1")}))
	require.NoError(t, db.Delete([]byte("a")))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, 2)
	assert.Equal(t, EntryPut, seen[0].Kind)
	assert.Equal(t, []byte("a"), seen[0].Key)
	assert.Equal(t, EntryDelete, seen[1].Kind)
	assert.Equal(t, uint64(1), seen[0].Seq)
	assert.Equal(t, uint64(2), seen[1].Seq)
}

type observerFunc func([]WALEntry)

func (f observerFunc) Observe(b []WALEntry) { f(b) }

func TestConcurrentWrites(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 512 })
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				k := []byte(fmt.Sprintf("g%d-k%d", g, i))
				require.NoError(t, db.Put(PutOptions{Key: k, Value: []byte("v")}))
			}
		}(g)
	}
	wg.Wait()

	for g := 0; g < 8; g++ {
		for i := 0; i < 30; i++ {
			v, err := db.Get([]byte(fmt.Sprintf("g%d-k%d", g, i)))
			require.NoError(t, err)
			assert.Equal(t, []byte("v"), v)
		}
	}
}

func benchDB(b *testing.B) *DB {
	b.Helper()
	o := DefaultOptions(b.TempDir())
	o.MemtableSize = 4 << 20
	db, err := Open(o)
	require.NoError(b, err)
	b.Cleanup(func() { db.Close() })
	return db
}

// bulkLoad seeds n sequential integer keys via batches to avoid an fsync per
// key during benchmark setup.
func bulkLoad(b *testing.B, db *DB, n int, val []byte) {
	b.Helper()
	const batchSize = 500
	for i := 0; i < n; i += batchSize {
		var batch Batch
		for j := 0; j < batchSize && i+j < n; j++ {
			batch.Put(PutOptions{Key: seqKey(i + j), Value: val})
		}
		require.NoError(b, db.Write(&batch))
	}
	db.sched.drain()
}

func seqKey(i int) []byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], uint64(i))
	return k[:]
}

func BenchmarkPutSequential(b *testing.B) {
	db := benchDB(b)
	val := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(PutOptions{Key: seqKey(i), Value: val}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutBatched(b *testing.B) {
	db := benchDB(b)
	val := make([]byte, 100)
	const batchSize = 100
	b.ResetTimer()
	for i := 0; i < b.N; i += batchSize {
		var batch Batch
		for j := 0; j < batchSize && i+j < b.N; j++ {
			batch.Put(PutOptions{Key: seqKey(i + j), Value: val})
		}
		if err := db.Write(&batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetRandom(b *testing.B) {
	db := benchDB(b)
	const n = 5000
	val := make([]byte, 100)
	bulkLoad(b, db, n, val)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Pseudo-random probe without Math.random: mix the index.
		k := seqKey(int((uint64(i) * 2654435761) % uint64(n)))
		if _, err := db.Get(k); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHasRandom(b *testing.B) {
	db := benchDB(b)
	const n = 5000
	val := make([]byte, 100)
	bulkLoad(b, db, n, val)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Has(seqKey(int((uint64(i) * 2654435761) % uint64(n)))); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetSequential(b *testing.B) {
	db := benchDB(b)
	const n = 5000
	val := make([]byte, 100)
	bulkLoad(b, db, n, val)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(seqKey(i % n)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMixed(b *testing.B) {
	db := benchDB(b)
	const n = 12000
	val := make([]byte, 100)
	bulkLoad(b, db, n, val)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%4 == 0 {
			_ = db.Put(PutOptions{Key: seqKey(i % n), Value: val})
		} else {
			_, _ = db.Get(seqKey(int((uint64(i) * 2654435761) % uint64(n))))
		}
	}
}

func BenchmarkIteratorScan(b *testing.B) {
	db := benchDB(b)
	const n = 12000
	val := make([]byte, 100)
	for i := 0; i < n; i++ {
		require.NoError(b, db.Put(PutOptions{Key: []byte(fmt.Sprintf("key%08d", i)), Value: val}))
	}
	db.sched.drain()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := db.NewIterator()
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for it.Next() {
			count++
		}
		it.Close()
		if count != n {
			b.Fatalf("scanned %d, want %d", count, n)
		}
	}
}

// benchDefaultsDB opens a database with the exact DefaultOptions (2 MiB
// memtable, durable per-batch fsync, s2/zstd codecs, 256 MiB cache) so the
// benchmarks below report real out-of-the-box performance.
func benchDefaultsDB(b *testing.B) *DB {
	b.Helper()
	db, err := Open(DefaultOptions(b.TempDir()))
	require.NoError(b, err)
	b.Cleanup(func() { db.Close() })
	return db
}

const benchValueSize = 256 // representative value payload

// BenchmarkDefaults_* measure the full public API with stock DefaultOptions.
// Writes are durable (one fsync per batch), so single-Put throughput is
// fsync-bound; batched writes amortize the fsync across the batch.

func BenchmarkDefaults_PutSync(b *testing.B) {
	db := benchDefaultsDB(b)
	val := make([]byte, benchValueSize)
	b.SetBytes(benchValueSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(PutOptions{Key: seqKey(i), Value: val}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDefaults_PutBatch100(b *testing.B) {
	db := benchDefaultsDB(b)
	val := make([]byte, benchValueSize)
	const batchSize = 100
	b.SetBytes(benchValueSize)
	b.ResetTimer()
	for i := 0; i < b.N; i += batchSize {
		var batch Batch
		for j := 0; j < batchSize && i+j < b.N; j++ {
			batch.Put(PutOptions{Key: seqKey(i + j), Value: val})
		}
		if err := db.Write(&batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDefaults_GetRandom(b *testing.B) {
	db := benchDefaultsDB(b)
	const n = 100_000
	bulkLoad(b, db, n, make([]byte, benchValueSize))
	b.SetBytes(benchValueSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := seqKey(int((uint64(i) * 2654435761) % uint64(n)))
		if _, err := db.Get(k); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDefaults_GetRandomParallel(b *testing.B) {
	db := benchDefaultsDB(b)
	const n = 100_000
	bulkLoad(b, db, n, make([]byte, benchValueSize))
	b.SetBytes(benchValueSize)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			i++
			k := seqKey(int((i * 2654435761) % uint64(n)))
			if _, err := db.Get(k); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkDefaults_HasRandom(b *testing.B) {
	db := benchDefaultsDB(b)
	const n = 100_000
	bulkLoad(b, db, n, make([]byte, benchValueSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Has(seqKey(int((uint64(i) * 2654435761) % uint64(n)))); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDefaults_Mixed(b *testing.B) {
	db := benchDefaultsDB(b)
	const n = 100_000
	val := make([]byte, benchValueSize)
	bulkLoad(b, db, n, val)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%4 == 0 { // 25% durable writes, 75% reads
			_ = db.Put(PutOptions{Key: seqKey(i % n), Value: val})
		} else {
			_, _ = db.Get(seqKey(int((uint64(i) * 2654435761) % uint64(n))))
		}
	}
}

func BenchmarkDefaults_IteratorScan(b *testing.B) {
	db := benchDefaultsDB(b)
	const n = 100_000
	bulkLoad(b, db, n, make([]byte, benchValueSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := db.NewIterator()
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for it.Next() {
			count++
		}
		it.Close()
		if count != n {
			b.Fatalf("scanned %d, want %d", count, n)
		}
	}
}

func TestIteratorsAfterCloseFail(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	require.NoError(t, db.Close())

	_, err := db.NewIterator()
	assert.ErrorIs(t, err, ErrClosed)
	_, err = db.NewRangeIterator([]byte("a"), []byte("z"))
	assert.ErrorIs(t, err, ErrClosed)
}

func TestCompactRange(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) {
		o.MemtableSize = 256
	})

	// Write, overwrite, and delete keys across several flushes so tables hold
	// dead versions and tombstones. Keys 0-14 are deleted; 15-29 survive.
	for gen := 0; gen < 2; gen++ {
		for i := 0; i < 30; i++ {
			require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%03d", i)), Value: []byte(fmt.Sprintf("v%d", gen))}))
		}
	}
	for i := 0; i < 15; i++ {
		require.NoError(t, db.Delete([]byte(fmt.Sprintf("k%03d", i))))
	}
	db.sched.drain()

	before, err := db.Stats()
	require.NoError(t, err)

	require.NoError(t, db.CompactRange(nil, nil))

	after, err := db.Stats()
	require.NoError(t, err)
	// Full compaction collapses tables; the total count should not grow and the
	// on-disk size should shrink now that dead versions/tombstones are gone.
	assert.LessOrEqual(t, after.Tables, before.Tables)
	assert.LessOrEqual(t, after.TablesSize, before.TablesSize)

	// Data integrity: surviving keys readable, deleted keys gone.
	for i := 15; i < 30; i++ {
		v, err := db.Get([]byte(fmt.Sprintf("k%03d", i)))
		require.NoError(t, err, i)
		assert.Equal(t, []byte("v1"), v)
	}
	for i := 0; i < 15; i++ {
		_, err := db.Get([]byte(fmt.Sprintf("k%03d", i)))
		assert.ErrorIs(t, err, ErrNotFound, i)
	}
}

func TestCompactRangeErrors(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	require.NoError(t, db.Close())
	assert.ErrorIs(t, db.CompactRange(nil, nil), ErrClosed)
}

func TestHas(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 32 })
	require.NoError(t, db.Put(PutOptions{Key: []byte("present"), Value: []byte("v")}))
	require.NoError(t, db.Put(PutOptions{Key: []byte("flushed"), Value: []byte("v")}))
	db.sched.drain() // push at least one key into an on-disk table

	// Present in a table (Has exercises the on-disk table path).
	has, err := db.Has([]byte("flushed"))
	require.NoError(t, err)
	assert.True(t, has)

	has, err = db.Has([]byte("present"))
	require.NoError(t, err)
	assert.True(t, has)

	has, err = db.Has([]byte("absent"))
	require.NoError(t, err)
	assert.False(t, has)

	// A deleted key reports absent.
	require.NoError(t, db.Delete([]byte("present")))
	has, err = db.Has([]byte("present"))
	require.NoError(t, err)
	assert.False(t, has)
}

func TestHasEdgeCases(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	_, err := db.Has(nil)
	assert.ErrorIs(t, err, ErrEmptyKey)
	require.NoError(t, db.Close())
	_, err = db.Has([]byte("k"))
	assert.ErrorIs(t, err, ErrClosed)
}

func TestDBWithSmallFDLimit(t *testing.T) {
	t.Parallel()
	// Far more tables than the descriptor limit: reads must still succeed by
	// reopening evicted descriptors on demand.
	db := openTestDB(t, func(o *Options) {
		o.MemtableSize = 256 // small -> many flushed tables
		o.MaxOpenFiles = 3
	})
	const n = 80
	for i := 0; i < n; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("key%05d", i)), Value: []byte(fmt.Sprintf("v%d", i))}))
	}
	db.sched.drain()

	for i := 0; i < n; i++ {
		v, err := db.Get([]byte(fmt.Sprintf("key%05d", i)))
		require.NoError(t, err, i)
		assert.Equal(t, []byte(fmt.Sprintf("v%d", i)), v)
	}

	db.fds.mu.Lock()
	open := db.fds.open
	db.fds.mu.Unlock()
	assert.LessOrEqual(t, open, 3, "open descriptors bounded by MaxOpenFiles")
}

func TestWriteBackpressureSlowdownRecordsStall(t *testing.T) {
	t.Parallel()
	// Tiny slowdown threshold, high stop and TierRatio so compaction does not fire
	// and depth-0 tables accumulate deterministically.
	db := openTestDB(t, func(o *Options) {
		o.MemtableSize = 256
		o.TierRatio = 1000
		o.L0SlowdownTables = 3
		o.L0StopTables = 1000
	})

	// Create several depth-0 tables (write straight to the engine so the setup
	// itself is not throttled).
	for i := 0; i < 6; i++ {
		db.eng.Put(uint64(i+1), []byte(fmt.Sprintf("k%04d", i)), []byte("v"))
		require.NoError(t, db.eng.Flush())
	}
	require.GreaterOrEqual(t, db.eng.depth0Count(), db.opts.L0SlowdownTables,
		"enough fresh-tier tables to trigger slowdown")

	before := db.metrics.writeStalls.Load()
	require.NoError(t, db.Put(PutOptions{Key: []byte("trigger"), Value: []byte("v")}))
	assert.Greater(t, db.metrics.writeStalls.Load(), before, "slowdown recorded a stall")

	st, err := db.Stats()
	require.NoError(t, err)
	assert.Positive(t, st.WriteStalls)
}

func TestWriteBackpressureHardStopReleasesAfterDrain(t *testing.T) {
	t.Parallel()
	// A pre-filled engine sits above the hard-stop mark; a writer must block until
	// a background drain pulls the fresh tier below stop, then proceed. Proves the
	// stop path both engages and releases (no deadlock).
	db := openTestDB(t, func(o *Options) {
		o.MemtableSize = 256
		o.TierRatio = 1000 // do not auto-compact; we drain manually
		o.L0SlowdownTables = 3
		o.L0StopTables = 4
	})
	// Build 5 depth-0 tables (>= stop) by writing straight to the engine, which
	// bypasses the throttle so the precondition itself does not block.
	for i := 0; i < 5; i++ {
		db.eng.Put(uint64(i+1), []byte(fmt.Sprintf("k%04d", i)), []byte("v"))
		require.NoError(t, db.eng.Flush())
	}
	require.GreaterOrEqual(t, db.eng.depth0Count(), db.opts.L0StopTables)

	// Release the writer shortly after it blocks by compacting the fresh tier
	// down to one deeper table (depth-0 count -> 0).
	go func() {
		time.Sleep(20 * time.Millisecond)
		retain, cc, release, err := db.compactionRunConfig(false)
		if err == nil {
			_ = db.eng.CompactAll(retain, cc)
			release()
		}
	}()

	done := make(chan error, 1)
	go func() { done <- db.Put(PutOptions{Key: []byte("late"), Value: []byte("v")}) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("write blocked by hard stop never released after drain")
	}
	assert.Positive(t, db.metrics.writeStalls.Load(), "hard stop recorded a stall")
	v, err := db.Get([]byte("late"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), v)
}

// TestWriteBackpressureFailsFastOnBackgroundError is a regression test: when a
// the engine is past the hard-stop mark AND background compaction is permanently
// poisoned, a write must fail fast with the background error rather than
// livelock forever in the throttle loop (the engine will never drain).
func TestWriteBackpressureFailsFastOnBackgroundError(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) {
		o.MemtableSize = 256
		o.TierRatio = 1000 // no auto-compaction to drain the tier
		o.L0SlowdownTables = 3
		o.L0StopTables = 4
	})
	// Fill the engine past the hard-stop mark (bypassing the throttle).
	for i := 0; i < 5; i++ {
		db.eng.Put(uint64(i+1), []byte(fmt.Sprintf("k%04d", i)), []byte("v"))
		require.NoError(t, db.eng.Flush())
	}
	require.GreaterOrEqual(t, db.eng.depth0Count(), db.opts.L0StopTables)

	// Poison background work so the tier can never drain below stop.
	db.setBackgroundError(errFailWrite)

	done := make(chan error, 1)
	go func() { done <- db.Put(PutOptions{Key: []byte("late"), Value: []byte("v")}) }()
	select {
	case err := <-done:
		require.Error(t, err, "write must fail fast, not livelock, when compaction is poisoned")
	case <-time.After(10 * time.Second):
		t.Fatal("write livelocked in throttle loop on a poisoned DB")
	}
}

// TestTombstoneTriggerNoLivelockWithHeldSnapshot is a regression test: with a
// long-lived snapshot pinning the retained sequence below the tombstones, a
// tombstone-triggered compaction cannot drop tombstones, so it must NOT keep
// re-firing and walking tier depth without bound. The drain must terminate.
func TestTombstoneTriggerNoLivelockWithHeldSnapshot(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) {
		o.MemtableSize = 256
		o.TierRatio = 1000               // count trigger off
		o.TombstoneCompactionRatio = 0.5 // tombstone trigger on
		o.FileSizeBase = 512             // small files so delete-heavy data
		o.FileSizeMax = 512              // rolls into >= 2 output tables
	})

	// Populate, then hold a snapshot that pins retain below the deletes.
	for i := 0; i < 40; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%03d", i)), Value: []byte("value-payload")}))
	}
	db.sched.drain()
	snap, err := db.Snapshot()
	require.NoError(t, err)
	defer snap.Release()

	// Delete most keys: tombstones now have seq > the snapshot's pinned seq.
	for i := 0; i < 40; i++ {
		require.NoError(t, db.Delete([]byte(fmt.Sprintf("k%03d", i))))
	}

	// The drain must finish promptly; without the gate it walks depth forever.
	done := make(chan struct{})
	go func() { db.sched.drain(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("compaction drain livelocked with a held snapshot")
	}

	// Sanity: the snapshot still reads the pre-delete values (retention worked).
	v, err := snap.Get([]byte("k000"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value-payload"), v)
	// And the live view sees the deletes.
	_, err = db.Get([]byte("k000"))
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestEntryTooLargeRejected(t *testing.T) {
	// Not parallel: it lowers the global maxEntrySize (so the test needn't
	// allocate a gigabyte), which would otherwise race concurrent tests.
	orig := maxEntrySize
	maxEntrySize = 4096
	t.Cleanup(func() { maxEntrySize = orig })

	db := openTestDB(t, nil)

	big := make([]byte, maxEntrySize) // key(3) + value(4096) > limit
	assert.ErrorIs(t, db.Put(PutOptions{Key: []byte("big"), Value: big}), ErrEntryTooLarge)

	// TTL path counts the expiry prefix toward the limit.
	justUnder := make([]byte, maxEntrySize-len("k")-expiryPrefixLen)
	assert.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: justUnder, TTL: time.Hour}))
	overWithTTL := make([]byte, maxEntrySize-len("k")-expiryPrefixLen+1)
	assert.ErrorIs(t, db.Put(PutOptions{Key: []byte("k"), Value: overWithTTL, TTL: time.Hour}), ErrEntryTooLarge)

	// A batch is rejected atomically if any op is too large (no partial apply).
	var b Batch
	b.Put(PutOptions{Key: []byte("ok"), Value: []byte("v")})
	b.Put(PutOptions{Key: []byte("toobig"), Value: make([]byte, maxEntrySize)})
	assert.ErrorIs(t, db.Write(&b), ErrEntryTooLarge)
	_, err := db.Get([]byte("ok"))
	assert.ErrorIs(t, err, ErrNotFound, "rejected batch must not partially apply")

	// The DB is still usable and uncorrupted after rejections.
	require.NoError(t, db.Put(PutOptions{Key: []byte("small"), Value: []byte("v")}))
	v, err := db.Get([]byte("small"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), v)
}

func TestBatchTooLargeRejected(t *testing.T) {
	// Not parallel: lowers global maxEntrySize.
	orig := maxEntrySize
	maxEntrySize = 4096
	t.Cleanup(func() { maxEntrySize = orig })

	db := openTestDB(t, nil)

	// Several ops each under the per-entry limit but together over the batch limit.
	each := maxEntrySize / 2 // 2 ops already exceed the limit
	var b Batch
	for i := 0; i < 4; i++ {
		b.Put(PutOptions{Key: []byte(fmt.Sprintf("k%d", i)), Value: make([]byte, each)})
	}
	assert.ErrorIs(t, db.Write(&b), ErrBatchTooLarge)

	// No op applied (rejected before WAL append).
	for i := 0; i < 4; i++ {
		_, err := db.Get([]byte(fmt.Sprintf("k%d", i)))
		assert.ErrorIs(t, err, ErrNotFound, "op %d must not apply from a rejected batch", i)
	}

	// A batch that stays within the limit still applies.
	var ok Batch
	ok.Put(PutOptions{Key: []byte("a"), Value: []byte("v")})
	require.NoError(t, db.Write(&ok))
	v, err := db.Get([]byte("a"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), v)
}
