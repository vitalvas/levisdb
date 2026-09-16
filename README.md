# levisdb

levisdb is an embedded LSM key-value store for Go. Writes land in an in-memory
memtable backed by a write-ahead log, flush to sorted on-disk tables, and merge
through size-tiered compaction. A scheduler serializes flush and compaction off
the write path, so a single spinning disk sees mostly sequential I/O.

## Platform support

levisdb supports Linux and macOS only. It uses advisory `flock` locking and
directory `fsync` semantics behind explicit `linux || darwin` build
constraints. FreeBSD, Windows, and other operating systems are deliberately
not supported.

## Features

- atomic batches and crash recovery through a checksummed, segmented WAL
- lenient WAL recovery that salvages the intact prefix past a bit-rotted record
- durable manifest transactions for flush and compaction
- snapshot reads and ordered range iterators
- size-tiered compaction with count and byte-density triggers and per-depth output targets
- overlap-scoped compaction that merges only key-overlapping tables, bounding read amplification
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
- `TombstoneCompactionRatio`: defaults to `0.5`; set a negative value to disable
  delete-density compaction.
- `FileSizeBase`, `FileSizeMultiplier`, and `FileSizeMax`: define the
  per-depth compaction output size curve.
- `CompactionFilter`: returns true to keep a live value or false to discard it.
  It must not call back into the database, and is deferred while a snapshot,
  iterator, or point read is live. Filtering may force a WAL checkpoint first so
  a crash cannot replay a discarded value.

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
| `MemtableSize` | `2 MiB` | Memtable flush threshold in bytes. |
| `TierRatio` | `4` | Size-tiered compaction fan-out (tables per tier before merge). |
| `TierByteTrigger` | `8 x FileSizeMax` | Tier bytes that trigger compaction below the count ratio; negative disables. |
| `TombstoneCompactionRatio` | `0.5` | Delete fraction that triggers early compaction; negative disables. |
| `L0SlowdownTables` / `L0StopTables` | `16` / `24` | Fresh-tier table counts that slow, then stop, writers. |
| `MaxCompactionBytes` | `10 x FileSizeMax` | Input byte cap per non-bottom compaction; negative disables. |
| `DisableOverlapSelection` | `false` | Merge the whole tier instead of only the largest key-overlapping group. |
| `FileSizeBase` / `FileSizeMultiplier` / `FileSizeMax` | `2 MiB` / `2` / `16 MiB` | Per-depth output size curve. |
| `FreshCodec` / `BottomCodec` / `LevelCodecs` | `CodecS2` / `CodecS2` / nil | Compression (`none`/`s2`/`flate`/`zstd`). See [Compression](#compression). |
| `EntropyCompression` | `false` | Skip the codec on incompressible blocks via an entropy pre-check. |
| `BloomBits` | `10` | Bloom filter bits per key. |
| `BlockSize` | `4 KiB` | SSTable data block size in bytes. |
| `BlockCacheSize` / `DisableBlockCache` | `256 MiB` / `false` | Decoded-block cache capacity; or turn it off. |
| `MaxOpenFiles` | `1024` | Open table descriptors kept at once; negative keeps all open. |
| `NoSync` / `WALSyncInterval` | `false` / `1s` | WAL durability. See [Write-ahead log](#write-ahead-log). |
| `StrictWALRecovery` | `false` | Fail open on any WAL corruption instead of salvaging the prefix. |
| `ReadOnly` | `false` | Reject writes; make no filesystem changes. |
| `WALObserver` | nil | Tap the committed stream for replication/CDC. |
| `WALRetention` / `WALRetentionBytes` | `0` / `0` | Keep flushed WAL for `GetUpdatesSince` catch-up. `0` disables retention. See [Replication](#replication). |
| `CompactionFilter` | nil | Drop values during compaction. See [Compaction filtering](#compaction-filtering). |

## Examples

Atomic batch. A batch commits atomically: it becomes visible to reads and
snapshots all-or-nothing, and its ops are one WAL record that becomes durable
together or not at all. See [Write-ahead log](#write-ahead-log).

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
		fmt.Printf("seq=%d kind=%d key=%s\n", e.Seq, e.Kind, e.Key)
	}
}

opts := levisdb.DefaultOptions("./data")
opts.WALObserver = printer{}
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
| `DeleteRange(start, end []byte) error` | Remove every key in `[start, end)` in one record. |
| `Get(key []byte) ([]byte, error)` | Read the latest value or `ErrNotFound`. |
| `Has(key []byte) (bool, error)` | Existence check without copying the value. |
| `Write(*Batch) error` | Commit a batch atomically. |
| `NewIterator() (Iterator, error)` | Ordered scan over all keys. |
| `NewRangeIterator(start, end []byte) (Iterator, error)` | Ordered scan over `[start, end)`; `nil` end is unbounded. |
| `Snapshot() (*Snapshot, error)` | Pin a consistent read view. |
| `CompactRange(start, end []byte) error` | Force compaction over a key range. |
| `LatestSeq() uint64` | Highest committed sequence; the replication resume point. |
| `GetUpdatesSince(seq uint64) (*WALUpdates, error)` | Replay committed mutations after `seq` for catch-up. |
| `IngestExternalFile(path string) error` | Bulk-load a table built by `SstFileWriter`. |
| `Stats() (Stats, error)` | Point-in-time monitoring snapshot. |
| `GetProperty(name string) (string, error)` | One named property as a string. |
| `Verify() ([]CorruptTable, error)` | Scrub: read every table's blocks and report on-disk corruption. |
| `Close() error` | Flush, retire the WAL, release resources. |

`*Batch`: `Put(PutOptions)`, `Delete(key []byte)`, `DeleteRange(start, end []byte)`,
`Reset()`, `Len() int`.

`*WALUpdates`: `Next() bool`, `Batch() []WALEntry`, `Error() error`, `Close() error`.

`*SstFileWriter`: `NewSstFileWriter(path, SstWriterOptions)`, then `Put`,
`PutTTL`, `Delete` (keys ascending), `Finish()`.

`*Snapshot`: `Get`, `Has`, `NewIterator`, `NewRangeIterator` (same signatures as
the `*DB` reads, fixed to the snapshot's sequence), `Seq() uint64`, `Release()`.

`Iterator` (interface): `Next() bool`, `Key() []byte`, `Value() []byte`,
`Error() error`, `Close() error`. Key and value slices are valid only until the
next `Next`; copy to retain.

Codec constants `CodecNone`, `CodecS2`, `CodecZstd`, and `CodecFlate` name the
built-in compression codecs in `Options`.

## Size limits

A single key plus value must be at most 1 GiB, and one batch may add at most
1 GiB to the memtable. The on-disk block format and the memtable arena address
data with 32-bit offsets, so an oversized entry would wrap an offset and
silently corrupt data. levisdb rejects it at the write boundary instead:
`Put`/`Write` return `ErrEntryTooLarge` for an oversized entry and
`ErrBatchTooLarge` for an oversized batch. These are hard format limits, not
tunables. If you chunk large values upstream (for example with content-defined
chunking), keep the maximum chunk well under the cap.

## Compaction and backpressure

Compaction is size-tiered: a tier compacts into the next once it accumulates
`TierRatio` tables (the count trigger), or once its aggregate on-disk size reaches
`TierByteTrigger` (the density trigger, defaults to `8 x FileSizeMax`). The density
trigger bounds read amplification when a few large tables would otherwise sit below
the count threshold uncompacted; it needs at least two tables in the tier, so a
lone table is never merged alone. A negative `TierByteTrigger` disables it.

A non-bottom compaction merges only the largest group of key-overlapping tables in
the tier (overlap-scoped selection), not the whole tier, so unrelated key ranges
are not rewritten and a lookup touches at most one output table per non-overlapping
group. The bottom tier still merges wholly because tombstone GC needs the complete
tier. Set `DisableOverlapSelection` to always merge the whole tier instead.
`MaxCompactionBytes` then caps the input merged in one non-bottom pass so a large
group drains in bounded steps rather than one merge that pins the disk.

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

Writes are throttled by fresh-tier (depth 0) table count so a burst cannot
outrun the serialized compactor: at `L0SlowdownTables` (default 16) writes are
briefly delayed, and at `L0StopTables` (default 24) they block until the
scheduler drains the tier. A negative value disables a threshold. Each throttled
write is counted in `Stats.WriteStalls`.

`CompactRange` forces full compaction on demand, which also runs any configured
compaction filter without waiting for automatic tier selection.

## Compression

Compression is a split by depth, with two knobs:

| Option | Default | Applies to |
| --- | --- | --- |
| `FreshCodec` | `CodecS2` (fast) | flushed L0 tables and every compaction output except the deepest tier |
| `BottomCodec` | `CodecS2` | only the deepest tier, where data is coldest and rewritten least |

Each accepts `CodecNone`, `CodecS2`, `CodecFlate`, or `CodecZstd`. Both default to
S2 (fast, moderate ratio) because Zstd's encode cost dominated compaction on large
compressible values; set `BottomCodec = CodecZstd` for a higher-ratio cold tier
when the extra encode cost is acceptable. `CodecFlate` (DEFLATE) is a middle
point: a better ratio than S2 at a higher CPU cost, for a mid tier that wants
more compression than S2 without Zstd's encode cost.

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

Whichever codec a block is assigned, any block that would not shrink is stored
raw, so the codec choice is a ceiling and incompressible data is never stored
larger than raw. Because each block records its own codec id, changing these
options only affects tables written afterward; existing tables keep decoding with
the codec they were written with, and a later compaction re-encodes them under
the new setting.

`EntropyCompression` (default off) adds a per-block entropy pre-check: a block
whose sampled Shannon entropy looks incompressible (already-compressed,
encrypted, or random data) is stored raw *without* attempting the codec, saving
that CPU. With it off, the codec is always attempted and the size fallback above
decides. Enable it when much of your data is already compressed and the wasted
compression attempts cost more than the entropy sampling.

## Monitoring

`Stats` returns a point-in-time snapshot: live table count and size, tables per
depth, block-cache blocks/bytes/hits/misses, open file descriptors, cumulative
compaction/flush/WAL byte counters, write stalls, and the background error.

`GetProperty` returns individual values by LevelDB-style name (for example
`levisdb.num-tables`, `levisdb.cache-hits`, `levisdb.wal-bytes-written`,
`levisdb.write-stalls`, `levisdb.background-error`).

Opening a database also publishes a process-wide `expvar` named `levisdb`, a
map of data directory to its stats, so a `net/http/pprof` or `expvar` HTTP
handler exposes every open database with no extra wiring. Closing a database
removes it from that view.

`Verify` is a scrub for proactive integrity checking: it reads every block of
every on-disk table and forces the per-block CRC32C check that reads already
perform, so bit rot or a truncated file is caught before a query happens to hit
the damaged block. It returns the corrupt tables (`[]CorruptTable`) and changes
nothing on disk; a caller with a replica or backup can then restore or
re-replicate them. It is I/O-heavy on a large database and does not block writes,
so run it during quiet periods. Reads already fail safe on corruption (a bad CRC
errors rather than returning wrong data); Verify only makes the detection
proactive rather than lazy.

## Errors

Sentinel errors returned by the API, all matchable with `errors.Is`:
`ErrNotFound`, `ErrClosed`, `ErrReadOnly`, `ErrEmptyKey`, `ErrInvalidTTL`,
`ErrInvalidRange`, `ErrEntryTooLarge`, `ErrBatchTooLarge`, `ErrRetentionExpired`,
and `ErrFileNumberExhausted`.

## Replication

levisdb exposes the committed write stream so a follower or change-data-capture
consumer can stay in sync. Every mutation carries a monotonic sequence, and a
consumer records the highest sequence it has durably applied.

- `LatestSeq()` returns the current highest committed sequence without pinning
  anything (cheaper than `Snapshot().Seq()`).
- `WALObserver` (above) delivers the live stream synchronously as batches commit.
- `GetUpdatesSince(seq)` replays every committed mutation after `seq` for a
  consumer that fell behind. Each `WALEntry` carries `Seq`, `Kind`, `Key`,
  `Value`, and for TTL puts both `TTL` (remaining at delivery) and `ExpiresAt`
  (the absolute deadline, so a replica reproduces the exact expiry).

`GetUpdatesSince` serves mutations still in the live WAL for free. To serve
mutations already flushed and retired, enable retention with `WALRetention` (a
duration) and/or `WALRetentionBytes`; flushed segments are then kept until they
are past *both* horizons. A request below the retained horizon returns
`ErrRetentionExpired`, signalling the consumer to re-bootstrap from a `Snapshot`
(iterate it, remember `Seq()`) and resume `GetUpdatesSince` from there. Delivery
is at-least-once: a consumer may re-see entries after a crash, so it must be
idempotent.

```go
u, err := db.GetUpdatesSince(lastApplied)
if err != nil {
	// errors.Is(err, levisdb.ErrRetentionExpired) -> re-bootstrap from a snapshot
	return err
}
defer u.Close()
for u.Next() {
	for _, e := range u.Batch() {
		apply(e) // e.Seq, e.Kind, e.Key, e.Value, e.ExpiresAt
	}
}
```

For bulk load, build a table offline with `SstFileWriter` (keys ascending) and
load it with `IngestExternalFile`. The whole file is assigned one fresh sequence
above every committed write and installed at the bottom tier, so its keys become
the newest version of whatever they carry. The file's key range must not overlap
keys still in the memtable; overlapping already-flushed tables is fine.

## Write-ahead log

Every mutation is first appended to the WAL, so a committed write survives a
crash even before the memtable is flushed to a table.

**Group commit.** Concurrent `Put`/`Delete`/`Write` calls are coalesced by a
single committer into one grouped journal write and one fsync, so the WAL sees
one sequential stream no matter how many goroutines write to it. Each call
returns only after its batch is durable and any `WALObserver` has run.

**Batch atomicity.** A `Batch` is one record: all of its ops become durable
together or none do, and it becomes visible to reads and snapshots atomically.
A global sequence number is assigned to the batch as a contiguous range, and the
read sequence advances past the batch only once it has applied, so a concurrent
reader never sees a batch half-applied.

**Durability modes.** By default (`NoSync` false) the committer fsyncs once per
group, so a returned write is crash-durable and the loss window is just the
in-flight group. `NoSync` skips that fsync and leaves the data in the OS page
cache: faster, but a crash can lose everything written since the last sync. Under
`NoSync` a background goroutine still fsyncs every `WALSyncInterval` (default one
second; negative disables it) to bound that window.

**Rotation and checkpoint.** When the live segment grows past its size bound the
WAL rotates to a fresh segment and checkpoints: the memtable the old segment
covered is flushed, then that segment is retired. So the on-disk WAL only ever
holds records not yet captured in a table, and recovery work stays bounded. The
checkpoint runs on a background goroutine, so a write never blocks on the flush
barrier (backpressure already bounds how far writes get ahead); `Close` waits for
an in-flight checkpoint before it returns. Enabling a `CompactionFilter` can force
an extra checkpoint before filtering so a crash cannot replay a value the filter
discarded.

**Recovery.** On open, levisdb replays the WAL segments back into the memtable,
then resumes. A torn tail from a crash (a half-written final record) is always
tolerated. By default recovery is also lenient about deeper damage: it stops at
the first corrupt record and keeps the intact prefix, so one bit-rotted record
does not make the database unopenable. Set `StrictWALRecovery` to fail the open
on any corruption instead. `ReadOnly` opens replay a crash WAL in memory only,
changing nothing on disk.

**Observation.** A `WALObserver` taps the committed stream in order, after fsync,
once per batch, for replication or change-data capture. See the CDC example
above. Delivery is at-least-once: a consumer resumes from its own stored `Seq`
and must be idempotent; the engine does not retain WAL history for lagging
consumers. `Stats.WALBytesWritten` / `levisdb.wal-bytes-written` report the
cumulative record volume.

## On-disk layout

Under `Dir`, all files sit directly in the directory: a write-ahead log in
segmented, checksummed journal blocks named `<num>.log` (see
[Write-ahead log](#write-ahead-log)); an append-only manifest recording every
flush and compaction, with a `CURRENT` file naming the live manifest; a `LOCK`
file for the single-process lock; and `<num>.sst` tables. Tables use
LevelDB-style prefix-compressed data blocks with restart points, a flat index,
an embedded bloom filter, and per-block CRC32C.

## TTL

`PutOptions.TTL` is per value. Zero means no expiration; negative or
unrepresentable durations return `ErrInvalidTTL`. Expiration is persisted as an
absolute deadline, so recovery does not restart the TTL. Once the newest value
expires it behaves as a tombstone and never reveals an older value. Point reads
evaluate wall-clock expiry when called; each iterator captures one time at
creation so a scan cannot change halfway through.

## Range deletes

`DeleteRange(start, end)` removes every key in the half-open range `[start, end)`
as one record, so deleting a large or prefix span is O(1) rather than one
tombstone per key. `end` must be non-empty and strictly greater than `start`,
else `ErrInvalidRange`. The delete applies at the sequence it commits: a snapshot
taken earlier still sees the keys, and a write with a key in the range made after
the call is unaffected. Range deletes persist in a per-table meta-block, shadow
covered keys on reads and scans, and are reclaimed at the bottom compaction tier
once no snapshot needs them. A `WALObserver` and `GetUpdatesSince` deliver a range
delete as a `WALEntry` of kind `EntryDeleteRange` with `Key` = start, `Value` =
end.

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

The callback should return promptly and must not call back into the same
database. A callback panic becomes a compaction error: a manual compaction
returns it, while a background compaction records it as the database's persistent
background error.

Filtering is deferred when a snapshot, iterator, or point read is already
pinned. New reads wait while an active filtering compaction commits, preserving
stable read views. Before filtering, levisdb may rotate the WAL and flush the
memtable so a crash cannot replay a discarded value. Writes committed after that
crash-safe cutoff remain untouched until a later compaction. This means enabling
the filter can add checkpoint I/O to compaction.

`CompactRange` forces full compaction and therefore provides a way to run the
configured filter without waiting for automatic tier selection.

