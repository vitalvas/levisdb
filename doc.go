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
//	r, err := db.Get([]byte("k")) // ErrNotFound if absent; r.Value, r.ExpiresAt
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
// Compaction is size-tiered. A tier merges into the next when ANY of three
// triggers fires: it holds TierRatio tables (count), its aggregate on-disk size
// reaches TierByteTrigger (density, default 8*FileSizeMax), or (when
// TombstoneCompactionRatio > 0, default 0.5) that fraction of its entries are
// deletes. The density and tombstone triggers need at least two tables, so a lone
// table is never merged alone. A negative TierByteTrigger or
// TombstoneCompactionRatio disables that trigger, leaving the others.
//
// Below L0 the count and density triggers additionally require real key overlap in
// the tier (unless DisableOverlapSelection is set): merging disjoint tables, each
// already the sole table for its key range, reclaims nothing and only rewrites the
// data one tier deeper. Append-only monotonic keys produce exactly such disjoint
// tables, so this stops a flush of unrelated ranges from cascading through every
// tier for no benefit. L0 always drains regardless of overlap (to bound read
// amplification), and the tombstone trigger is unaffected (it reclaims deletes).
//
// Target file size grows with tier depth,
//
//	target(d) = min(FileSizeBase * FileSizeMultiplier^d, FileSizeMax)
//
// so hot shallow tiers hold small, cheap files and cold deep tiers hold fewer,
// larger ones. Depth 0 (a memtable flush) is unsized -- one file per flush -- so
// the first target this governs is depth 1 at FileSizeBase*FileSizeMultiplier
// (with the defaults, 2 MiB base gives a 4 MiB depth-1 target). FileSizeMax
// defaults to 128 MiB (derived from FileSizeBase so the cap scales with the base)
// and holds the deepest tiers at that ceiling; raise FileSizeMax or
// FileSizeMultiplier for larger bottom-tier files and fewer of them. The roll
// projects the finished file size (data bytes plus bloom/index/footer) and rolls
// before it exceeds the target, so a compacted file lands at or under the target;
// a single key larger than the target still produces a one-key file above it.
//
// MaxLevels (1..8, default 8) bounds the ladder: the deepest tier merges in place
// so it never grows deeper, trading larger bottom merges for tighter read and
// recovery fan-out. A compaction merges only the largest group of key-overlapping
// tables (overlap-scoped selection) so unrelated key ranges are not rewritten; set
// DisableOverlapSelection to always merge the whole tier. MaxCompactionBytes caps
// the input merged in one pass (negative disables the cap). Both apply even to a
// growth hop that relocates into a new, deeper, still-empty tier, so a fast-growing
// store never merges its whole accumulated tier at once; only the in-place merge at
// the deepest populated tier merges the complete tier and overlapping shallower
// tables, which bottom tombstone GC requires to stay correct after ingestion.
//
// Writes are throttled by fresh-tier table count so a burst cannot outrun the
// serialized compactor: at L0SlowdownTables (default 16) writes are delayed, and
// at L0StopTables (default 24) they block until the tier drains. A negative value
// disables a threshold.
//
// CompactRange forces a full compaction on demand. It proceeds one tier at a time
// from the shallowest down, committing and freeing each tier's inputs before the
// next, rather than merging every tier in a single pass. This bounds peak scratch
// space, merge memory, and the crash-recovery window to a single tier instead of
// the whole dataset, so a full compaction's resource use stays bounded regardless
// of total data size. The compaction filter and tombstone drop run only on the
// final collapse into the deepest tier, so each surviving key is filtered once.
//
// # Open table descriptors
//
// Every live table reads through an on-demand file descriptor. Two reclaimers
// bound how many stay open:
//
//   - MaxOpenFiles (default 1000) caps the count: once exceeded, a clock
//     (second-chance) sweep closes the least-recently-used descriptor, which
//     reopens transparently on its next read. A negative value keeps every table
//     open (no bound). This alone keeps a database of millions of tables within the
//     OS descriptor limit.
//   - FDIdleTimeout (default 0, off) adds time-based release: a background reaper
//     closes any descriptor untouched for at least the timeout, returning the
//     descriptor to the OS during quiet periods even while under the count cap. It
//     reopens on the next read. Enable it when an idle database should not hold
//     descriptors open indefinitely.
//
// Both are no-ops on a ReadOnly open past what reads require.
//
// # Compression
//
// FreshCodec (default CodecS2) compresses upper tiers and flushed L0 tables;
// BottomCodec (default CodecS2) compresses the deepest tier. Each accepts
// CodecNone, CodecS2, CodecFlate, or CodecZstd, from fastest to highest ratio.
// LevelCodecs overrides the codec per depth, falling back to that split for empty
// entries and depths past its length. Compression is per block: a block that
// would not shrink is stored raw, so incompressible data is never stored larger
// than raw. Each block records its own codec id, so changing these options
// affects only tables written afterward.
//
// # Durability and recovery
//
// Every mutation is appended to the WAL before it is visible. Concurrent writes
// are coalesced into one grouped journal write and one fsync (group commit). There
// are two durability modes:
//
//   - Durable (NoSync false, the default): group commit fsyncs once per batch, so
//     a returned Put/Write is crash-durable the instant it returns. The loss
//     window on a power loss is only the in-flight group. This is the safe default.
//   - NoSync true: the per-commit fsync is skipped for throughput. Durability is
//     NOT handed to the OS page cache to flush whenever it likes; instead a
//     levisdb-owned background goroutine blocking-fsyncs the WAL every
//     WALSyncInterval (default one second). So the power-loss window is bounded to
//     at most one interval of writes, deterministically, not by non-deterministic
//     page-cache eviction. WALSyncInterval must not be negative (zero uses the
//     default); a negative value is rejected because it would remove that bound.
//     Use NoSync only for rebuildable or cache-like data.
//
// Close and a clean shutdown flush committed writes and retire the live WAL
// regardless of mode. A crash (process kill or power loss) relies on WAL replay on
// the next open. On open the WAL replays into the memtable. A torn crash tail is
// always tolerated, and by default recovery is lenient about deeper corruption,
// keeping the intact prefix; set StrictWALRecovery to fail the open instead. A
// WALObserver taps the committed stream in order for replication or change-data
// capture.
//
// # TTL
//
// PutOptions.ExpiresAt sets a per-value absolute expiry, a Unix-second instant
// (for example time.Now().Add(ttl).Unix()). Expiry is second-granular; sub-second
// TTLs are not supported, so the smallest usable deadline is one second ahead.
// Zero means no expiration; a negative or already-past value returns ErrInvalidTTL
// (so a write never stores a value no read would return). The deadline is stored
// as given, so recovery does not restart it and a replica reproduces it exactly
// via WALEntry.ExpiresAt (apply it back as PutOptions.ExpiresAt, no clock drift).
//
// An expired newest value behaves as a tombstone and never reveals an older one.
// Expiry is evaluated against a read time, and that time differs by read kind: a
// point read (Get/Has) uses the wall clock at the moment of the call, while an
// iterator or snapshot captures one time at creation and holds it for the whole
// scan, so a scan cannot have a key expire out from under it midway. Compaction
// reclaims an expired value only once no live iterator's captured time still sees
// it live. CompactionFilterEntry.ExpiresAt carries the value's absolute deadline
// (the same Unix-second value a put set), 0 for a non-TTL value.
//
// # Deduplication
//
// Options.Deduplication, off by default, skips a put whose key already holds an
// identical value AND the same ExpiresAt, so an unchanged row re-written any number
// of times produces no new WAL record, memtable version, or compaction work.
// Because ExpiresAt is absolute, re-writing the same key/value/ExpiresAt is a
// byte-identical record; this is what makes a replayed or idempotent write stream
// collapse to one stored version. A put differing in value OR in ExpiresAt is not
// a duplicate and is written. A put whose key is currently absent, tombstoned, or
// expired is never a duplicate, so it always applies. Deletes and range deletes are
// never deduplicated. The check costs one point lookup per put while enabled, so it
// is off by default; enable it for idempotent or replayed write streams.
//
// # Range deletes
//
// DeleteRange(start, end) removes every key in the half-open range [start, end)
// as one record, so deleting a large or prefix span is O(1) rather than one
// tombstone per key. It applies at the sequence it commits, so an earlier
// snapshot still sees the keys and a later write in the range is unaffected. Range
// deletes persist in a per-table meta-block, shadow covered keys on reads and
// scans, and are reclaimed at the bottom compaction tier once no snapshot needs
// them. WALObserver and GetUpdatesSince deliver a range delete as an
// EntryDeleteRange WALEntry with Key = start and Value = end.
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
// # Replication
//
// The committed write stream is exposed for followers and change-data capture.
// LatestSeq returns the current highest committed sequence (the resume point). A
// WALObserver receives the live stream synchronously per batch. GetUpdatesSince
// replays committed mutations after a given sequence for a consumer that fell
// behind; enable WALRetention / WALRetentionBytes to keep flushed WAL segments
// long enough to serve them, or GetUpdatesSince covers only the live segment. A
// request below the retained horizon returns ErrRetentionExpired, signalling the
// consumer to re-bootstrap from a Snapshot. Snapshot.WriteTo writes the pinned
// data as an ingestible base image and returns the sequence to resume the tail
// from, preserving exact TTL deadlines, so a bootstrap closes the gap the
// tail-only helpers leave; WriteToDir rolls that image into bounded files for a
// large database. Delivery is at-least-once, so consumers must be idempotent. For
// bulk load, SstFileWriter builds a table offline and IngestExternalFile installs
// it at one fresh sequence.
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
// each data directory to its stats. Verify is a scrub: it reads every block of
// every table and reports on-disk corruption (bad CRC, truncated/garbled file)
// proactively, before a query hits the damaged block, changing nothing on disk.
//
// # Concurrency
//
// A *DB and a *Snapshot are safe for concurrent use by multiple goroutines,
// including concurrent reads. A single *Batch or Iterator holds mutable state and
// must not be shared; use one per goroutine.
package levisdb
