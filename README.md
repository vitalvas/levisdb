# levisdb

levisdb is an embedded, sharded LSM key-value store for Go. It is designed for
large, seek-sensitive disks: writes share one sequential WAL, shards flush and
compact independently, and a global scheduler keeps compaction concurrency
bounded.

## Platform support

levisdb supports Linux and macOS only. It uses advisory `flock` locking and
directory `fsync` semantics behind explicit `linux || darwin` build
constraints. FreeBSD, Windows, and other operating systems are deliberately
not supported.

## Features

- atomic batches and crash recovery through a checksummed, segmented WAL
- lenient WAL recovery that salvages the intact prefix past a bit-rotted record
- durable manifest transactions for flush and compaction
- FNV-1a, Murmur3, and range partitioners, plus validated custom partitioners
- snapshot reads and ordered range iterators
- size-tiered compaction with per-depth output file targets and a per-pass byte cap
- tombstone-density compaction that drains delete-heavy tiers early
- write backpressure that slows and then stops writers before the tier ladder runs away
- S2 for fresh data and Zstandard for bottom-tier data, skipped for high-entropy blocks
- Bloom filters, a bounded block cache, and a bounded table descriptor pool
- non-mutating read-only opens, including in-memory replay of a crash WAL
- optional ordered WAL observation for replication or change-data capture
- per-value TTL persisted across WAL recovery and compaction
- optional compaction filtering with key, value, and remaining TTL context
- monitoring through `Stats`, LevelDB-style `GetProperty`, and a process `expvar`

## Basic use

```go
package main

import (
	"log"
	"time"

	"github.com/vitalvas/levisdb"
)

func main() {
	opts := levisdb.DefaultOptions("./data")
	db, err := levisdb.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	if err := db.Put(levisdb.PutOptions{
		Key:   []byte("key"),
		Value: []byte("value"),
		TTL:   24 * time.Hour,
	}); err != nil {
		log.Fatal(err)
	}
	value, err := db.Get([]byte("key"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s", value)
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}
```

Use `DefaultOptions` and then adjust the workload-specific values. The zero
values accepted by `Open` receive defaults, but `Dir` is required. Notable
safety and performance controls include:

- `NoSync`: skips per-group WAL `fsync`; writes are not crash-durable when
  `Put`/`Write` returns. `WALSyncInterval` defaults to one second to bound the
  usual loss window; a negative interval disables that periodic sync.
- `ReadOnly`: rejects mutations and makes no filesystem changes.
- `DisableBlockCache`: explicitly disables decoded-block caching.
- `CompactionConcurrency`: defaults to one for a single spinning disk.
- `TombstoneCompactionRatio`: defaults to `0.5`; set a negative value to disable
  delete-density compaction.
- `FileSizeBase`, `FileSizeMultiplier`, and `FileSizeMax`: define the
  per-depth compaction output size curve.
- `CompactionFilter`: returns true to keep a live value or false to discard it.
  It can run concurrently across shards, must not call back into the database,
  and is deferred while a snapshot, iterator, or point read is live. Filtering
  may force a WAL checkpoint first so a crash cannot replay a discarded value.

`Close` flushes committed writes and retires the live WAL. Always check its
error. If an asynchronous flush or compaction fails, subsequent writes fail and
the first error is exposed through `Stats.BackgroundError` and the
`levisdb.background-error` property.

Snapshots and iterators pin the table versions they read. Release snapshots and
close iterators promptly so compaction can reclaim obsolete versions and files.

## Options reference

Every field is optional except `Dir`; a zero value takes the default. Defaults
target a single slow spinning disk.

| Option | Default | Purpose |
| --- | --- | --- |
| `Dir` | (required) | Data directory. |
| `ShardCount` | `32` | Number of shards. Baked into data; verified on open. |
| `Partitioner` / `CustomPartitioner` | `PartitionerHash` | Key-to-shard mapping. See [Partitioning](#partitioning). |
| `MemtableSize` | `2 MiB` | Per-shard memtable flush threshold in bytes. |
| `TierRatio` | `4` | Size-tiered compaction fan-out (tables per tier before merge). |
| `TombstoneCompactionRatio` | `0.5` | Delete fraction that triggers early compaction; negative disables. |
| `L0SlowdownTables` / `L0StopTables` | `16` / `24` | Fresh-tier table counts that slow, then stop, writers. |
| `MaxCompactionBytes` | `10 x FileSizeMax` | Input byte cap per non-bottom compaction; negative disables. |
| `FileSizeBase` / `FileSizeMultiplier` / `FileSizeMax` | `2 MiB` / `2` / `16 MiB` | Per-depth output size curve. |
| `FreshCodec` / `BottomCodec` / `LevelCodecs` | `CodecS2` / `CodecZstd` / nil | Compression. See [Compression](#compression). |
| `BloomBits` | `10` | Bloom filter bits per key. |
| `BlockSize` | `4 KiB` | SSTable data block size in bytes. |
| `BlockCacheSize` / `DisableBlockCache` | `256 MiB` / `false` | Decoded-block cache capacity; or turn it off. |
| `MaxOpenFiles` | `1024` | Open table descriptors kept at once; negative keeps all open. |
| `CompactionConcurrency` | `1` | Shards compacting at once; `CompactionConcurrencyAuto` for one per CPU. |
| `NoSync` / `WALSyncInterval` | `false` / `1s` | WAL durability. See [Write-ahead log](#write-ahead-log). |
| `StrictWALRecovery` | `false` | Fail open on any WAL corruption instead of salvaging the prefix. |
| `ReadOnly` | `false` | Reject writes; make no filesystem changes. |
| `WALObserver` | nil | Tap the committed stream for replication/CDC. |
| `CompactionFilter` | nil | Drop values during compaction. See [Compaction filtering](#compaction-filtering). |

## Examples

Atomic batch. Every op in a batch is one WAL append: all become durable
together or none do.

```go
var b levisdb.Batch
b.Put(levisdb.PutOptions{Key: []byte("a"), Value: []byte("1")})
b.Put(levisdb.PutOptions{Key: []byte("b"), Value: []byte("2")})
b.Delete([]byte("stale"))
if err := db.Write(&b); err != nil {
	log.Fatal(err)
}
```

Ordered scan. A range iterator walks `[start, end)`; a `nil` end scans to the
last key. Always `Close` it.

```go
it, err := db.NewRangeIterator([]byte("k010"), []byte("k020"))
if err != nil {
	log.Fatal(err)
}
defer it.Close()
for it.Next() {
	fmt.Printf("%s = %s\n", it.Key(), it.Value())
}
if err := it.Error(); err != nil {
	log.Fatal(err)
}
```

Consistent read view. A snapshot pins one sequence; writes made after it are
invisible to its reads and iterators until it is released.

```go
snap, err := db.Snapshot()
if err != nil {
	log.Fatal(err)
}
defer snap.Release()

v, err := snap.Get([]byte("a")) // the value as of the snapshot
```

Monitoring. `Stats` for a whole snapshot, `GetProperty` for one value.

```go
st, _ := db.Stats()
fmt.Printf("tables=%d size=%d cache hit/miss=%d/%d stalls=%d\n",
	st.Tables, st.TablesSize, st.CacheHits, st.CacheMisses, st.WriteStalls)

n, _ := db.GetProperty("levisdb.num-tables")
```

Change-data capture. A `WALObserver` sees every committed batch in order, after
fsync. It must return quickly and must not call back into the database.

```go
type printer struct{}

func (printer) Observe(batch []levisdb.WALEntry) {
	for _, e := range batch {
		fmt.Printf("seq=%d shard=%d kind=%d key=%s\n", e.Seq, e.Shard, e.Kind, e.Key)
	}
}

opts := levisdb.DefaultOptions("./data")
opts.WALObserver = printer{}
```

Custom partitioner. Supply your own key-to-shard mapping; it is baked into the
data and its `Name` is verified on reopen.

```go
type tenantPartitioner struct{}

func (tenantPartitioner) Name() string { return "tenant-v1" }
func (tenantPartitioner) Shard(key []byte, numShards int) int {
	// Route by a leading tenant byte so one tenant's keys stay on one shard.
	return int(key[0]) % numShards
}

opts := levisdb.DefaultOptions("./data")
opts.CustomPartitioner = tenantPartitioner{}
```

## API reference

Package-level:

- `Open(Options) (*DB, error)` - open or create a database.
- `DefaultOptions(dir string) Options` - defaults for a data directory.

`*DB`:

| Method | Purpose |
| --- | --- |
| `Put(PutOptions) error` | Set a key, optionally with a TTL. |
| `Delete(key []byte) error` | Remove a key. |
| `Get(key []byte) ([]byte, error)` | Read the latest value or `ErrNotFound`. |
| `Has(key []byte) (bool, error)` | Existence check without copying the value. |
| `Write(*Batch) error` | Commit a batch atomically. |
| `NewIterator() (Iterator, error)` | Ordered scan over all keys. |
| `NewRangeIterator(start, end []byte) (Iterator, error)` | Ordered scan over `[start, end)`; `nil` end is unbounded. |
| `Snapshot() (*Snapshot, error)` | Pin a consistent read view. |
| `CompactRange(start, end []byte) error` | Force compaction over a key range. |
| `CompactShard(shard int) error` | Force compaction of one shard. |
| `CompactShardRange(shard int, start, end []byte) error` | Force compaction of a range within one shard. |
| `Stats() (Stats, error)` | Point-in-time monitoring snapshot. |
| `GetProperty(name string) (string, error)` | One named property as a string. |
| `Close() error` | Flush, retire the WAL, release resources. |

`*Batch`: `Put(PutOptions)`, `Delete(key []byte)`, `Reset()`, `Len() int`.

`*Snapshot`: `Get`, `Has`, `NewIterator`, `NewRangeIterator` (same signatures as
the `*DB` reads, fixed to the snapshot's sequence), `Seq() uint64`, `Release()`.

`Iterator` (interface): `Next() bool`, `Key() []byte`, `Value() []byte`,
`Error() error`, `Close() error`. Key and value slices are valid only until the
next `Next`; copy to retain.

Built-in partitioners: `HashPartitioner`, `Murmur3Partitioner`,
`RangePartitioner` (each implements the `Partitioner` interface). The
`Partitioner` string constants `PartitionerHash`, `PartitionerMurmur3`,
`PartitionerRange` and codec constants `CodecNone`, `CodecS2`, `CodecZstd`
name the built-ins in `Options`.

## Size limits

A single key plus value must be at most 1 GiB, and one batch may add at most
1 GiB to any one shard. The on-disk block format and the memtable arena address
data with 32-bit offsets, so an oversized entry would wrap an offset and
silently corrupt data. levisdb rejects it at the write boundary instead:
`Put`/`Write` return `ErrEntryTooLarge` for an oversized entry and
`ErrBatchTooLarge` for an oversized per-shard batch. These are hard format
limits, not tunables. If you chunk large values upstream (for example with
content-defined chunking), keep the maximum chunk well under the cap.

## Partitioning

The partitioner maps each key to a shard. It is baked into the data and its
name is verified on open, so you cannot reopen a database with a different
partitioner or shard count without an `ErrPartitionerMismatch`.

- `PartitionerHash` (default): FNV-1a over the key, even spread, no locality.
  The safe choice for point-lookup workloads.
- `PartitionerMurmur3`: MurmurHash3 token mapping. Also an even spread, with
  better distribution than FNV-1a on structured or low-entropy keys.
- `PartitionerRange`: routes by key prefix so adjacent keys tend to share a
  shard. Choose it when range scans dominate and you want scan locality; it can
  skew shards if the key space is uneven.

Set `Options.CustomPartitioner` to supply your own. It must be deterministic,
concurrency-safe, and return a stable non-empty `Name`.

## Compaction and backpressure

Compaction is size-tiered: a tier compacts into the next once it accumulates
`TierRatio` tables. `MaxCompactionBytes` caps the input merged in one non-bottom
pass so a large tier drains in bounded steps rather than one merge that pins the
disk; the bottom tier always merges whole so tombstone GC stays correct.

Target `.sst` size grows with tier depth, so deep (cold) tiers hold fewer, larger
files and shallow (hot) tiers hold small ones that flush and merge cheaply. As
compaction rolls output at depth `d`, the target is:

```
target(d) = min(FileSizeBase * FileSizeMultiplier^d, FileSizeMax)
```

With the defaults (`FileSizeBase` 2 MiB, `FileSizeMultiplier` 2, `FileSizeMax`
16 MiB):

| Depth | Target size |
| --- | --- |
| 0 (fresh, from a memtable flush) | 2 MiB |
| 1 | 4 MiB |
| 2 | 8 MiB |
| 3 | 16 MiB |
| 4 and deeper | 16 MiB (capped) |

`MemtableSize` defaults to `FileSizeBase` so a flushed L0 table lands at the
depth-0 target. Raise `FileSizeMax` (or the multiplier) for larger bottom-tier
files and fewer of them; the target is a per-file roll point, not a hard limit,
so a single oversized key can still produce a larger file.

`TombstoneCompactionRatio` (default `0.5`) compacts a tier early once that
fraction of its entries are deletes, reclaiming delete-heavy data toward the
bottom tier where tombstones and the values they shadow are dropped. A negative
value disables it. The trigger is suppressed while a snapshot pins an older
sequence, since those deletes must stay visible.

Writes are throttled per shard by fresh-tier (depth 0) table count so a burst
cannot outrun the single serialized compactor: at `L0SlowdownTables` (default
16) writes are briefly delayed, and at `L0StopTables` (default 24) they block
until the scheduler drains the shard. A negative value disables a threshold.
Each throttled write is counted in `Stats.WriteStalls`.

`CompactRange`, `CompactShard`, and `CompactShardRange` force full compaction on
demand, which also runs any configured compaction filter without waiting for
automatic tier selection.

## Compression

By default compression is a two-way split by depth, with two knobs:

| Option | Default | Applies to |
| --- | --- | --- |
| `FreshCodec` | `CodecS2` (fast) | flushed L0 tables and every compaction output except the deepest tier |
| `BottomCodec` | `CodecZstd` (high ratio) | only the deepest tier, where data is coldest and rewritten least |

Each accepts `CodecS2`, `CodecZstd`, or `CodecNone`. The split exists because
fresh data is rewritten often (favor CPU-cheap S2) while bottom data is written
once and read for a long time (favor Zstd's ratio).

For finer control, `LevelCodecs []string` overrides the codec per tier depth:
`LevelCodecs[d]` names the codec for tables written at depth `d`. Depths past
the end of the slice, and empty entries, fall back to the Fresh/Bottom split, so
you only specify the depths you care about.

```go
opts := levisdb.DefaultOptions("./data")

// Two-way split (the default shape): fast fresh, high-ratio bottom.
opts.FreshCodec = levisdb.CodecS2
opts.BottomCodec = levisdb.CodecZstd

// Per level: no compression at L0 (hot, churny), s2 at L1-L2, zstd for L3 and
// anything deeper (via BottomCodec).
opts.LevelCodecs = []string{levisdb.CodecNone, levisdb.CodecS2, levisdb.CodecS2}

// Leave a gap: override only L0; every other depth keeps the Fresh/Bottom split.
opts.LevelCodecs = []string{levisdb.CodecNone}

// Disable compression entirely.
opts.FreshCodec = levisdb.CodecNone
opts.BottomCodec = levisdb.CodecNone
```

Whichever codec a block is assigned, compression is per block and conditional: a
block whose sampled entropy is near random (already-compressed or encrypted
data) is stored uncompressed to avoid wasting CPU, and any block that would not
shrink is stored raw as well. So the codec choice is a ceiling, not a guarantee,
and incompressible data pays no compression tax. Because each block records its
own codec id, changing these options only affects tables written afterward;
existing tables keep decoding with the codec they were written with, and a later
compaction re-encodes them under the new setting.

## Monitoring

`Stats` returns a point-in-time snapshot: live table count and size, tables per
depth, block-cache blocks/bytes/hits/misses, open file descriptors, cumulative
compaction/flush/WAL byte counters, write stalls, the background error, and a
per-shard breakdown for spotting partition skew.

`GetProperty` returns individual values by LevelDB-style name (for example
`levisdb.num-tables`, `levisdb.cache-hits`, `levisdb.wal-bytes-written`,
`levisdb.write-stalls`, `levisdb.background-error`).

Opening a database also publishes a process-wide `expvar` named `levisdb`, a
map of data directory to its stats, so a `net/http/pprof` or `expvar` HTTP
handler exposes every open database with no extra wiring. Closing a database
removes it from that view.

## Errors

Sentinel errors returned by the API, all matchable with `errors.Is`:
`ErrNotFound`, `ErrClosed`, `ErrReadOnly`, `ErrEmptyKey`, `ErrInvalidTTL`,
`ErrEntryTooLarge`, `ErrBatchTooLarge`, `ErrPartitionerMismatch`, and
`ErrFileNumberExhausted`.

## Write-ahead log

Every mutation is first appended to one shared WAL, so a committed write survives
a crash even before its shard's memtable is flushed to a table.

**Group commit.** Concurrent `Put`/`Delete`/`Write` calls are coalesced by a
single committer into one grouped journal write and one fsync, so the disk sees
one sequential stream no matter how many goroutines write. Each call returns only
after its batch is durable and any `WALObserver` has run. A whole `Batch` is one
record: all of its ops become durable together or none do.

**Durability modes.** By default (`NoSync` false) the committer fsyncs once per
group, so a returned write is crash-durable and the loss window is just the
in-flight group. `NoSync` skips that fsync and leaves the data in the OS page
cache: faster, but a crash can lose everything written since the last sync. Under
`NoSync` a background goroutine still fsyncs every `WALSyncInterval` (default one
second; negative disables it) to bound that window.

**Rotation and checkpoint.** When the live segment grows past its size bound the
WAL rotates to a new segment and checkpoints: it flushes the shard memtables the
old segment covered, then retires that segment. So the on-disk WAL only ever
holds records not yet captured in a table, and recovery work stays bounded.
Enabling a `CompactionFilter` can force an extra checkpoint before filtering so a
crash cannot replay a value the filter discarded.

**Recovery.** On open, levisdb replays the WAL back into memtables, then resumes.
A torn tail from a crash (a half-written final record) is always tolerated. By
default recovery is also lenient about deeper damage: it stops at the first
corrupt record and keeps the intact prefix, so one bit-rotted record does not
make the database unopenable. Set `StrictWALRecovery` to fail the open on any
corruption instead. `ReadOnly` opens replay a crash WAL in memory only, changing
nothing on disk.

**Observation.** A `WALObserver` taps the committed stream in order, after fsync,
once per batch, for replication or change-data capture. See the CDC example
above. Delivery is at-least-once: a consumer resumes from its own stored `Seq`
and must be idempotent; the engine does not retain WAL history for lagging
consumers. `Stats.WALBytesWritten` / `levisdb.wal-bytes-written` report the
cumulative record volume.

## On-disk layout

Under `Dir`: a shared write-ahead log in segmented, checksummed journal blocks
(see [Write-ahead log](#write-ahead-log)); an append-only manifest recording
every flush and compaction, with a `CURRENT` file naming the live manifest; and
per-shard `.sst` tables. Tables use LevelDB-style prefix-compressed data blocks
with restart points, a flat index, an embedded bloom filter, and per-block
CRC32C.

## TTL

`PutOptions.TTL` is per value. Zero means no expiration; negative or
unrepresentable durations return `ErrInvalidTTL`. Expiration is persisted as an
absolute deadline, so recovery does not restart the TTL. Once the newest value
expires it behaves as a tombstone and never reveals an older value. Point reads
evaluate wall-clock expiry when called; each iterator captures one time at
creation so a scan cannot change halfway through.

## Compaction filtering

Set `Options.CompactionFilter` to decide whether a non-deleted, unexpired value
survives when compaction rewrites it:

```go
opts := levisdb.DefaultOptions("./data")
opts.CompactionFilter = func(entry levisdb.CompactionFilterEntry) bool {
	// Remove short-lived values instead of rewriting them again.
	if entry.TTL > 0 && entry.TTL < 5*time.Minute {
		return false
	}
	return true
}

db, err := levisdb.Open(opts)
```

Returning `true` keeps the value. Returning `false` discards it as a logical
tombstone, so an older version of the same key cannot become visible. The input
fields have these meanings:

- `Key`: the user key.
- `Value`: the user value, without internal TTL metadata.
- `TTL`: remaining lifetime at the compaction cutoff, or zero for a value
  without expiration.

Expired values and existing tombstones are not passed to the callback. `Key`
and `Value` are copies owned by the callback, so it may retain or modify them.
A kept value can be presented again during a later compaction.

Different shard compactors may invoke the callback concurrently. The callback
must therefore be concurrency-safe, should return promptly, and must not call
back into the same database. A callback panic becomes a compaction error: a
manual compaction returns it, while a background compaction records it as the
database's persistent background error.

Filtering is deferred when a snapshot, iterator, or point read is already
pinned. New reads wait while an active filtering compaction commits, preserving
stable read views. Before filtering, levisdb may rotate the WAL and flush shard
memtables so a crash cannot replay a discarded value. Writes committed after
that crash-safe cutoff remain untouched until a later compaction. This means
enabling the filter can add checkpoint I/O to compaction.

`CompactRange` and `CompactShard` force full compaction and therefore provide a
way to run the configured filter without waiting for automatic tier selection.

