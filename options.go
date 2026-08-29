package levisdb

import (
	"fmt"
	"log/slog"
	"math"
	"runtime"
	"time"
)

// EntryKind identifies the kind of mutation carried by a WALEntry.
type EntryKind uint8

const (
	// EntryPut is a key set to a value.
	EntryPut EntryKind = iota
	// EntryDelete is a key removal (tombstone).
	EntryDelete
)

// WALEntry is one committed mutation delivered to a WALObserver in commit
// order. Key and Value are valid only for the duration of the Observe call; a
// consumer that retains them must copy.
type WALEntry struct {
	Seq   uint64        // monotonic log sequence number; a stable resume point
	Shard int           // owning shard index
	Kind  EntryKind     // EntryPut or EntryDelete
	Key   []byte        // mutation key
	Value []byte        // value for EntryPut; nil for EntryDelete
	TTL   time.Duration // remaining lifetime; zero means none, negative means already expired
}

// WALObserver taps the durable write stream so callers can build replication
// or change-data-capture without forking the engine.
//
// Observe is called synchronously after each configured WAL commit (after
// fsync unless NoSync is enabled), once per batch, in commit order. It MUST
// return quickly and MUST NOT block on I/O, panic, or call back into the
// database; a slow observer throttles all writes, while a panic terminates the
// live WAL with an error. Consumers that need async delivery do their own
// buffering.
//
// Delivery is at-least-once: after a crash a consumer resumes from its own
// stored Seq and may re-see entries, so consumers must be idempotent. Only the
// live stream is observed; the engine does not retain WAL history for lagging
// consumers.
type WALObserver interface {
	Observe(batch []WALEntry)
}

// Partitioner maps a key to one of numShards shards.
//
// The partitioner and shard count are baked into on-disk data: they decide
// which shard owns each key. Reopening a database with a different partitioner
// would misroute reads, so Name is persisted and verified on open.
type Partitioner interface {
	// Shard returns the owning shard index in [0, numShards). It must be
	// deterministic and safe for concurrent use.
	Shard(key []byte, numShards int) int

	// Name is a stable identity persisted in the manifest and checked on open.
	Name() string
}

// ShardRanger is an optional interface a Partitioner may implement when it is
// order-preserving (shards own contiguous key ranges). A range iterator then
// scans only the shards a query range can touch instead of all of them. A hash
// partitioner scatters adjacent keys across every shard, so it must NOT implement
// this; the iterator falls back to scanning all shards for those.
type ShardRanger interface {
	// ShardRange returns the half-open shard index range [lo, hi) that a key range
	// [start, end) can touch. A nil start means from the smallest key; a nil end
	// means unbounded above. The result must cover every shard that Shard would
	// return for any key in [start, end); returning the full [0, numShards) is
	// always correct but forgoes pruning.
	ShardRange(start, end []byte, numShards int) (lo, hi int)
}

// Default option values. All are tunable so the engine can be calibrated
// against a real spindle.
const (
	DefaultShardCount   = 8
	DefaultMemtableSize = 2 << 20 // 2 MiB, matched to the fresh-tier file size
	DefaultTierRatio    = 4
	// DefaultTombstoneCompactionRatio compacts a tier once half its entries are
	// tombstones, reclaiming delete-heavy data without waiting for the table-count
	// trigger. A negative value in Options disables it.
	DefaultTombstoneCompactionRatio = 0.5
	// DefaultL0SlowdownTables and DefaultL0StopTables set write backpressure a
	// few multiples above TierRatio: writes slow once a shard's fresh tier is
	// several compactions behind, and stop before the backlog grows unbounded.
	DefaultL0SlowdownTables      = 16
	DefaultL0StopTables          = 24
	DefaultFileSizeBase          = 2 << 20 // 2 MiB
	DefaultFileSizeMultiplier    = 2
	DefaultFileSizeMax           = 16 << 20 // 16 MiB
	DefaultBloomBits             = 10
	DefaultBlockSize             = 4 << 10   // 4 KiB
	DefaultBlockCacheSize        = 256 << 20 // 256 MiB
	DefaultCompactionConcurrency = 1
	DefaultMaxOpenFiles          = 1024 // bounded open table descriptors

	// CompactionConcurrencyAuto, when set as CompactionConcurrency, resolves to
	// one compactor per CPU (capped at ShardCount). Use only on fast or
	// multi-disk storage; a single HDD wants the default of 1 to keep the head
	// on one sequential stream.
	CompactionConcurrencyAuto = -1
	DefaultFreshCodec         = CodecS2
	// DefaultBottomCodec is S2 too: zstd's slow encode dominated compaction on
	// large, compressible values (bottom-tier merges stalled). S2 keeps compaction
	// fast; zstd stays selectable and decodable for existing data.
	DefaultBottomCodec = CodecS2

	// DefaultWALSyncInterval bounds the NoSync crash-loss window when the caller
	// does not set one.
	DefaultWALSyncInterval = time.Second

	// defaultMaxCompactionFiles sets MaxCompactionBytes to this many FileSizeMax
	// files by default: large enough that ordinary tiers merge in one pass, small
	// enough to bound a runaway tier's single merge.
	defaultMaxCompactionFiles = 10

	// defaultTierByteTriggerFiles sets TierByteTrigger to this many FileSizeMax
	// files by default: a tier holding this many bytes is compacted even below the
	// count threshold, catching the "few large tables never reach the ratio" stall
	// without pre-empting the count trigger on ordinary fresh tiers. It is above
	// DefaultTierRatio * FileSizeMax (4 * 16 = 64 MiB), so the count trigger fires
	// first for a normal full tier; the density trigger only engages for 2-3 large
	// tables that together exceed 128 MiB yet stay below the count of 4.
	defaultTierByteTriggerFiles = 8

	// maxMemtableSize caps the per-shard flush threshold. The skiplist arena
	// addresses nodes by uint32 offset and grows slightly faster than the tracked
	// entry size (per-node header + level links), so the arena must never reach
	// 2^32. 2 GiB leaves ample headroom below that boundary.
	maxMemtableSize = 2 << 30 // 2 GiB
)

// maxEntrySize caps a single key+value AND the cumulative bytes one batch may
// add to a single shard (writeBatch), because a whole batch is applied before
// any flush check. The skiplist arena (uint32 offsets) and the data-block
// restart array (uint32 in-block offsets) both address data with 32-bit offsets,
// so data approaching those limits would wrap and silently corrupt. Together
// with the per-batch bound, mid-replay flushing on recovery, and MemtableSize <=
// maxMemtableSize, a memtable arena stays well below 2^32. 1 GiB leaves generous
// headroom while accommodating any realistic value. It is a var only so tests
// can lower it without allocating a gigabyte; production never changes it.
var maxEntrySize = 1 << 30 // 1 GiB

// Options configures a levisdb database. The zero value is not valid; use
// DefaultOptions and adjust from there, or rely on fillDefaults which is
// applied by Open.
type Options struct {
	// Dir is the data directory. Required.
	Dir string

	// Partitioner selects a built-in key-to-shard mapping: PartitionerHash
	// (default, even spread via FNV-1a), PartitionerMurmur3 (even spread via
	// MurmurHash3, better distribution for structured keys), or PartitionerRange
	// (scan-local). Ignored when CustomPartitioner is set.
	Partitioner string
	// CustomPartitioner overrides Partitioner when non-nil.
	CustomPartitioner Partitioner
	// ShardCount is the number of shards. Baked into data; verified on open.
	ShardCount int

	// MemtableSize is the per-shard memtable flush threshold in bytes.
	MemtableSize int64
	// TierRatio is the size-tiered compaction fan-out.
	TierRatio int

	// TierByteTrigger compacts a tier once its aggregate on-disk size reaches this
	// many bytes, even if it holds fewer than TierRatio tables. The count trigger
	// alone lets a few large tables accumulate uncompacted (unbounded read
	// amplification); this density trigger bounds that. It fires only with at least
	// two tables in the tier, so a single table is never "compacted" alone. Zero
	// uses the default; a negative value disables the trigger, leaving only the
	// count and tombstone triggers.
	TierByteTrigger int64

	// TombstoneCompactionRatio triggers a tier's compaction once the fraction of
	// its entries that are tombstones (deletes) reaches this value, even if the
	// tier has fewer than TierRatio tables. This reclaims delete-heavy tiers early
	// by compacting them toward the bottom where tombstones and the values they
	// shadow are dropped. Must be in [0,1]. Zero uses the default; a negative
	// value disables the trigger, leaving only the table-count trigger.
	TombstoneCompactionRatio float64

	// L0SlowdownTables and L0StopTables apply write backpressure per shard based
	// on the number of fresh-tier (depth 0) tables, so a write burst cannot
	// outrun the single serialized compactor and grow the tier ladder without
	// bound. When a shard's depth-0 table count reaches L0SlowdownTables, writes
	// to it are briefly delayed; at L0StopTables they block until the scheduler
	// drains the shard below the slowdown mark. L0StopTables must be >=
	// L0SlowdownTables. Zero uses the defaults; a negative value disables that
	// threshold.
	L0SlowdownTables int
	L0StopTables     int

	// FileSizeBase is the fresh-tier target .sst size in bytes.
	FileSizeBase int64
	// FileSizeMultiplier grows the target size per tier depth.
	FileSizeMultiplier int
	// FileSizeMax caps the per-tier target size in bytes.
	FileSizeMax int64

	// MaxCompactionBytes caps the total input bytes merged in one non-bottom
	// compaction, so a large tier is drained in bounded steps rather than one
	// merge that ties up the single spindle. Bottom-tier compactions (which drop
	// tombstones) always merge the whole tier for GC correctness. Zero uses the
	// default; a negative value disables the cap.
	MaxCompactionBytes int64

	// DisableOverlapSelection turns off overlap-scoped compaction. By default a
	// non-bottom compaction merges only the largest group of key-overlapping tables
	// in the tier, so unrelated key ranges are not rewritten and a lookup touches at
	// most one output table per non-overlapping group. Set true to merge the whole
	// tier every time (the pre-overlap behavior).
	DisableOverlapSelection bool

	// BloomBits is the bloom filter bits per key.
	BloomBits int
	// BlockSize is the SSTable block size in bytes.
	BlockSize int
	// BlockCacheSize is the block cache capacity in bytes.
	BlockCacheSize int64
	// DisableBlockCache disables decoded-block caching. It takes precedence over
	// BlockCacheSize and provides an explicit opt-out while zero-valued options
	// continue to receive the default capacity.
	DisableBlockCache bool

	// MaxOpenFiles bounds how many table descriptors are kept open at once;
	// tables reopen on demand when a descriptor is evicted. Zero uses the
	// default; a negative value keeps every table open (unbounded).
	MaxOpenFiles int

	// CompactionConcurrency bounds how many shards compact at once. Default 1
	// keeps a single spindle to one sequential stream. Set it to
	// CompactionConcurrencyAuto for one compactor per CPU (capped at
	// ShardCount) on fast or multi-disk storage.
	CompactionConcurrency int

	// FreshCodec compresses fresh (upper) tiers: CodecS2, CodecZstd, or CodecNone.
	FreshCodec string
	// BottomCodec compresses the bottom (oldest) tier: CodecZstd, CodecS2, or
	// CodecNone.
	BottomCodec string
	// LevelCodecs overrides the codec per tier depth: LevelCodecs[d] names the
	// codec (CodecS2, CodecZstd, or CodecNone) for tables written at depth d.
	// Depths past the end of the slice, and empty entries, fall back to the
	// FreshCodec/BottomCodec split (BottomCodec for the deepest tier, FreshCodec
	// otherwise). Nil leaves that split unchanged. A block records its own codec
	// id, so changing this only affects tables written afterward.
	LevelCodecs []string

	// EntropyCompression enables a per-block entropy pre-check: a block whose
	// sampled Shannon entropy looks incompressible (already-compressed, encrypted,
	// or random data) is stored raw without attempting the codec, saving that CPU.
	// The default (false) disables the pre-check and always attempts the codec,
	// relying on the size-check fallback that stores a block raw when compression
	// did not shrink it. Enable it when much of the data is already compressed and
	// the wasted compression attempts cost more than the entropy sampling.
	EntropyCompression bool

	// WALObserver, if non-nil, receives committed entries for replication/CDC.
	WALObserver WALObserver

	// Logger, if non-nil, receives structured storage-engine events (open, flush,
	// compaction, WAL rotation/checkpoint, close) via log/slog, mirroring what
	// LevelDB writes to its LOG file. The engine never logs to a package-global or
	// to stderr; all events go through this logger so the caller controls level,
	// format, and destination with their own slog.Handler. Nil disables logging
	// with no overhead beyond a cheap level check. Events are emitted at Debug for
	// routine per-operation detail and Info for lifecycle milestones; attach a
	// leveled handler to filter. The engine adds a "component"="levisdb" attribute
	// and, per event, an "op" plus context like the shard index and table numbers.
	Logger *slog.Logger

	// CompactionFilter, if non-nil, is called for each non-deleted, unexpired
	// value rewritten by compaction. It may be called concurrently by different
	// shard compactors and must not call back into this DB. A panic fails compaction.
	// Filtering is deferred while snapshots, iterators, or point reads are live
	// so their stable view cannot change underneath them. The database may force
	// a WAL checkpoint before filtering to make discarded values crash-safe.
	CompactionFilter CompactionFilter

	// ReadOnly opens the database without allowing writes.
	ReadOnly bool
	// NoSync disables the per-group WAL fsync, leaving durability to the OS page
	// cache. The default (false) is durable: group commit fsyncs once per batch.
	// Enabling it is faster but widens the crash-loss window; use only for
	// rebuildable or cache-like data.
	NoSync bool

	// WALSyncInterval bounds the NoSync crash-loss window: a background goroutine
	// fsyncs the WAL this often so unsynced writes are not lost indefinitely. It
	// applies only when NoSync is set (durable mode already fsyncs per group).
	// Zero uses the default; a negative value disables the periodic sync (writes
	// then persist only when the OS flushes the page cache).
	WALSyncInterval time.Duration

	// StrictWALRecovery makes Open fail on any WAL corruption encountered during
	// replay. The default (false, lenient) instead stops replay at the first
	// corrupt record and recovers the intact prefix, so a single bit-rotted
	// record on a large disk does not make the whole database unopenable. A torn
	// tail from a crash is always tolerated regardless of this setting.
	StrictWALRecovery bool
}

// DefaultOptions returns Options populated with defaults for the given data
// directory.
func DefaultOptions(dir string) Options {
	o := Options{Dir: dir}
	o.fillDefaults()
	return o
}

// fillDefaults sets any unset field to its default. It does not validate.
func (o *Options) fillDefaults() {
	if o.Partitioner == "" {
		o.Partitioner = PartitionerHash
	}
	if o.ShardCount == 0 {
		o.ShardCount = DefaultShardCount
	}
	if o.TierRatio == 0 {
		o.TierRatio = DefaultTierRatio
	}
	switch {
	case o.TombstoneCompactionRatio == 0:
		o.TombstoneCompactionRatio = DefaultTombstoneCompactionRatio
	case o.TombstoneCompactionRatio < 0:
		o.TombstoneCompactionRatio = 0 // caller disabled the trigger
	}
	if o.L0SlowdownTables == 0 {
		o.L0SlowdownTables = DefaultL0SlowdownTables
	}
	if o.L0StopTables == 0 {
		o.L0StopTables = DefaultL0StopTables
	}
	if o.WALSyncInterval == 0 {
		o.WALSyncInterval = DefaultWALSyncInterval
	}
	if o.FileSizeBase == 0 {
		o.FileSizeBase = DefaultFileSizeBase
	}
	if o.MemtableSize == 0 {
		// A memtable flushes to one fresh-tier (L0) table, so default its size
		// to the fresh-tier target so L0 tables come out uniformly sized.
		o.MemtableSize = o.FileSizeBase
	}
	if o.FileSizeMultiplier == 0 {
		o.FileSizeMultiplier = DefaultFileSizeMultiplier
	}
	if o.FileSizeMax == 0 {
		o.FileSizeMax = DefaultFileSizeMax
	}
	if o.MaxCompactionBytes == 0 {
		// Default to a few max-size files so normal tiers merge whole while a
		// runaway tier is drained in bounded steps.
		o.MaxCompactionBytes = o.FileSizeMax * defaultMaxCompactionFiles
	}
	switch {
	case o.TierByteTrigger == 0:
		o.TierByteTrigger = o.FileSizeMax * defaultTierByteTriggerFiles
	case o.TierByteTrigger < 0:
		o.TierByteTrigger = 0 // caller disabled the density trigger
	}
	if o.BloomBits == 0 {
		o.BloomBits = DefaultBloomBits
	}
	if o.BlockSize == 0 {
		o.BlockSize = DefaultBlockSize
	}
	if o.BlockCacheSize == 0 {
		o.BlockCacheSize = DefaultBlockCacheSize
	}
	if o.MaxOpenFiles == 0 {
		o.MaxOpenFiles = DefaultMaxOpenFiles
	}
	switch o.CompactionConcurrency {
	case 0:
		o.CompactionConcurrency = DefaultCompactionConcurrency
	case CompactionConcurrencyAuto:
		// Auto: one compactor per CPU, but never more than there are shards to
		// compact. Only sensible on fast/parallel storage; on a single spindle
		// keep the spindle-safe default of 1 (see roadmap).
		o.CompactionConcurrency = runtime.NumCPU()
		if o.CompactionConcurrency > o.ShardCount {
			o.CompactionConcurrency = o.ShardCount
		}
	}
	if o.FreshCodec == "" {
		o.FreshCodec = DefaultFreshCodec
	}
	if o.BottomCodec == "" {
		o.BottomCodec = DefaultBottomCodec
	}
}

// validCodecs are the codec names accepted for FreshCodec and BottomCodec.
var validCodecs = map[string]bool{CodecNone: true, CodecS2: true, CodecZstd: true}

// validPartitioners are the built-in partitioner names accepted for Partitioner
// when no CustomPartitioner is set.
var validPartitioners = map[string]bool{PartitionerHash: true, PartitionerRange: true, PartitionerMurmur3: true}

// validate checks that the options are internally consistent. It assumes
// fillDefaults has already run.
func (o *Options) validate() error {
	if o.Dir == "" {
		return fmt.Errorf("levisdb: Dir is required")
	}
	if o.CustomPartitioner == nil && !validPartitioners[o.Partitioner] {
		return fmt.Errorf("levisdb: unknown partitioner %q", o.Partitioner)
	}
	if o.CustomPartitioner != nil && o.CustomPartitioner.Name() == "" {
		return fmt.Errorf("levisdb: custom partitioner Name must not be empty")
	}
	if o.ShardCount < 1 {
		return fmt.Errorf("levisdb: ShardCount must be >= 1, got %d", o.ShardCount)
	}
	if o.MemtableSize < 1 {
		return fmt.Errorf("levisdb: MemtableSize must be positive, got %d", o.MemtableSize)
	}
	// The skiplist arena addresses nodes by uint32 offset, and the arena grows
	// slightly faster than the tracked size (per-node header + links). Cap well
	// under 4 GiB so one shard's memtable can never push the arena past 2^32 and
	// wrap an offset. See memtable_skiplist.go.
	if o.MemtableSize > maxMemtableSize {
		return fmt.Errorf("levisdb: MemtableSize must be <= %d (skiplist arena uses uint32 offsets), got %d", maxMemtableSize, o.MemtableSize)
	}
	if o.TierRatio < 2 {
		return fmt.Errorf("levisdb: TierRatio must be >= 2, got %d", o.TierRatio)
	}
	if math.IsNaN(o.TombstoneCompactionRatio) || o.TombstoneCompactionRatio < 0 || o.TombstoneCompactionRatio > 1 {
		return fmt.Errorf("levisdb: TombstoneCompactionRatio must be in [0,1], got %v", o.TombstoneCompactionRatio)
	}
	if o.L0SlowdownTables > 0 && o.L0StopTables > 0 && o.L0StopTables < o.L0SlowdownTables {
		return fmt.Errorf("levisdb: L0StopTables (%d) must be >= L0SlowdownTables (%d)", o.L0StopTables, o.L0SlowdownTables)
	}
	if o.FileSizeBase < 1 {
		return fmt.Errorf("levisdb: FileSizeBase must be positive, got %d", o.FileSizeBase)
	}
	if o.FileSizeMultiplier < 1 {
		return fmt.Errorf("levisdb: FileSizeMultiplier must be >= 1, got %d", o.FileSizeMultiplier)
	}
	if o.FileSizeMax < o.FileSizeBase {
		return fmt.Errorf("levisdb: FileSizeMax (%d) must be >= FileSizeBase (%d)", o.FileSizeMax, o.FileSizeBase)
	}
	if o.BloomBits < 1 {
		return fmt.Errorf("levisdb: BloomBits must be positive, got %d", o.BloomBits)
	}
	if o.BlockSize < 1 {
		return fmt.Errorf("levisdb: BlockSize must be positive, got %d", o.BlockSize)
	}
	if o.BlockCacheSize < 0 {
		return fmt.Errorf("levisdb: BlockCacheSize must be non-negative, got %d", o.BlockCacheSize)
	}
	if o.CompactionConcurrency < 1 {
		return fmt.Errorf("levisdb: CompactionConcurrency must be >= 1, got %d", o.CompactionConcurrency)
	}
	if !validCodecs[o.FreshCodec] {
		return fmt.Errorf("levisdb: unknown FreshCodec %q", o.FreshCodec)
	}
	if !validCodecs[o.BottomCodec] {
		return fmt.Errorf("levisdb: unknown BottomCodec %q", o.BottomCodec)
	}
	for d, name := range o.LevelCodecs {
		if name != "" && !validCodecs[name] {
			return fmt.Errorf("levisdb: unknown LevelCodecs[%d] %q", d, name)
		}
	}
	return nil
}

// partitionerName returns the identity that is persisted and verified on open.
func (o *Options) partitionerName() string {
	if o.CustomPartitioner != nil {
		return o.CustomPartitioner.Name()
	}
	return builtinPartitioner(o.Partitioner).Name()
}
