// Package levisdb is an embedded, sharded key-value storage engine for Go,
// modeled on LevelDB and tuned for large, slow spinning disks. Writes share one
// sequential write-ahead log, the keyspace is split into independent shards that
// flush and compact on their own, and a global scheduler keeps compaction
// concurrency bounded so a single spindle sees mostly sequential I/O.
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
// # Sharding and partitioning
//
// A Partitioner maps each key to one of ShardCount shards. Its Name and the
// shard count are baked into the on-disk data and verified on open, so reopening
// with a different mapping fails with ErrPartitionerMismatch. Built-ins:
//
//   - PartitionerHash (default): FNV-1a, even spread, no locality.
//   - PartitionerMurmur3: MurmurHash3, even spread, better on structured keys.
//   - PartitionerRange: routes by key prefix for range-scan locality.
//
// Set Options.CustomPartitioner to supply your own; it must be deterministic,
// concurrency-safe, and return a stable non-empty Name.
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
// Writes are throttled per shard by fresh-tier table count so a burst cannot
// outrun the single serialized compactor: at L0SlowdownTables writes are delayed,
// and at L0StopTables they block until the shard drains. CompactRange,
// CompactShard, and CompactShardRange force compaction on demand.
//
// # Compression
//
// FreshCodec (default CodecS2) compresses upper tiers and flushed L0 tables;
// BottomCodec (default CodecZstd) compresses the deepest tier. LevelCodecs
// overrides the codec per depth, falling back to that split for empty entries and
// depths past its length. Compression is per block and conditional: a
// near-random or non-shrinking block is stored raw, so incompressible data pays
// no compression tax. Each block records its own codec id, so changing these
// options affects only tables written afterward.
//
// # Durability and recovery
//
// Every mutation is appended to the shared WAL before it is visible. Concurrent
// writes are coalesced into one grouped journal write and one fsync (group
// commit). By default a returned write is crash-durable. NoSync trades that for
// speed, leaving durability to the OS page cache; a background goroutine then
// fsyncs every WALSyncInterval to bound the loss window. On open the WAL replays
// into memtables. A torn crash tail is always tolerated, and by default recovery
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
// 1 GiB to any one shard. The block format and memtable arena address data with
// 32-bit offsets, so an oversized entry would wrap and corrupt; it is rejected at
// the write boundary with ErrEntryTooLarge or ErrBatchTooLarge. These are hard
// format limits, not tunables.
//
// # Monitoring
//
// Stats returns a point-in-time snapshot (table counts and sizes, cache
// hits/misses, cumulative compaction/flush/WAL bytes, write stalls, per-shard
// breakdown). GetProperty returns individual values by LevelDB-style name.
// Opening a database also publishes a process-wide expvar named "levisdb" mapping
// each data directory to its stats.
//
// # Concurrency
//
// A *DB and a *Snapshot are safe for concurrent use by multiple goroutines,
// including concurrent reads. A single *Batch or Iterator holds mutable state and
// must not be shared; use one per goroutine.
package levisdb
