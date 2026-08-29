// Package levisdb is an embedded key-value storage engine for Go, modeled on
// LevelDB. Writes land in an in-memory memtable backed by a write-ahead log,
// flush to sorted on-disk tables, and merge through size-tiered compaction. A
// scheduler serializes flush and compaction off the write path, so a single
// spinning disk sees mostly sequential I/O.
//
// # Basic use
//
// Open a database, write and read, then close. DefaultOptions fills every field
// but Dir, which is required.
//
//	opts := levisdb.DefaultOptions("./data")
//	db, err := levisdb.Open(opts)
//	if err != nil {
//		return err
//	}
//	defer db.Close()
//
//	if err := db.Put(levisdb.PutOptions{Key: []byte("k"), Value: []byte("v")}); err != nil {
//		return err
//	}
//	v, err := db.Get([]byte("k")) // ErrNotFound if absent
//
// Always check the error from Close: it flushes committed writes and retires the
// live WAL. If a background flush or compaction fails, subsequent writes fail and
// the first error is reported through Stats.BackgroundError and the
// "levisdb.background-error" property.
//
// # Platform support
//
// Only Linux and macOS are supported. The engine relies on advisory flock
// locking and directory fsync behind explicit "linux || darwin" build
// constraints; other operating systems are deliberately excluded.
//
// # Compaction and backpressure
//
// Compaction is size-tiered: a tier merges into the next once it holds TierRatio
// tables. Target file size grows with tier depth,
//
//	target(d) = min(FileSizeBase * FileSizeMultiplier^d, FileSizeMax)
//
// so hot shallow tiers hold small, cheap files and cold deep tiers hold fewer,
// larger ones. MaxCompactionBytes caps the input merged in one non-bottom pass;
// the bottom tier always merges whole so tombstone GC stays correct.
// TombstoneCompactionRatio compacts a delete-heavy tier early to reclaim space.
//
// Writes are throttled by fresh-tier table count so a burst cannot outrun the
// serialized compactor: at L0SlowdownTables writes are delayed, and at
// L0StopTables they block until the tier drains. CompactRange forces compaction
// on demand.
//
// # Compression
//
// FreshCodec (default CodecS2) compresses upper tiers and flushed L0 tables;
// BottomCodec (default CodecS2) compresses the deepest tier. Each accepts
// CodecNone, CodecS2, CodecFlate, or CodecZstd, from fastest to highest ratio.
// LevelCodecs overrides the codec per depth, falling back to that split for empty
// entries and depths past its length. Compression is per block: a block that
// would not shrink is stored raw, so incompressible data is never stored larger
// than raw.
// EntropyCompression (default off) additionally skips the codec entirely on
// blocks whose sampled entropy looks incompressible. Each block records its own
// codec id, so changing these options affects only tables written afterward.
//
// # Durability and recovery
//
// Every mutation is appended to the WAL before it is visible. Concurrent writes
// are coalesced into one grouped journal write and one fsync (group commit). By
// default a returned write is crash-durable. NoSync trades that for speed,
// leaving durability to the OS page cache; a background goroutine then fsyncs
// every WALSyncInterval to bound the loss window. On open the WAL replays into
// the memtable. A torn crash tail is always tolerated, and by default recovery
// is lenient about deeper corruption, keeping the intact prefix; set
// StrictWALRecovery to fail the open instead. A WALObserver taps the committed
// stream in order for replication or change-data capture.
//
// # TTL
//
// PutOptions.TTL sets a per-value lifetime. Zero means no expiration; a negative
// or unrepresentable duration returns ErrInvalidTTL. Expiration is stored as an
// absolute deadline, so recovery does not restart it. An expired newest value
// behaves as a tombstone and never reveals an older one. Point reads evaluate
// expiry when called; each iterator captures one time at creation.
//
// # Snapshots and iterators
//
// Snapshot pins a consistent read view at the current sequence; writes made after
// it are invisible to its reads and iterators until Release. NewIterator and
// NewRangeIterator return ordered scans over the whole keyspace or a [start, end)
// range. Snapshots and iterators pin the table versions they read, so release and
// close them promptly to let compaction reclaim obsolete files. Iterator key and
// value slices are valid only until the next Next; copy to retain.
//
// # Size limits
//
// A single key plus value must be at most 1 GiB, and one batch may add at most
// 1 GiB to the memtable. The block format and memtable arena address data with
// 32-bit offsets, so an oversized entry would wrap and corrupt; it is rejected at
// the write boundary with ErrEntryTooLarge or ErrBatchTooLarge. These are hard
// format limits, not tunables.
//
// # Monitoring
//
// Stats returns a point-in-time snapshot (table counts and sizes, cache
// hits/misses, cumulative compaction/flush/WAL bytes, write stalls).
// GetProperty returns individual values by LevelDB-style name.
// Opening a database also publishes a process-wide expvar named "levisdb" mapping
// each data directory to its stats.
//
// # Concurrency
//
// A *DB and a *Snapshot are safe for concurrent use by multiple goroutines,
// including concurrent reads. A single *Batch or Iterator holds mutable state and
// must not be shared; use one per goroutine.
package levisdb
