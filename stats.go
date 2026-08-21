package levisdb

import (
	"fmt"
	"sort"
	"strconv"
)

// Stats is a point-in-time snapshot of database state for monitoring.
type Stats struct {
	Shards        int   // configured shard count
	Tables        int   // total live SSTables across all shards
	TablesSize    int64 // total on-disk size of live tables in bytes
	LiveSnapshots int   // read sequences pinned by snapshots, iterators, and active point reads
	CacheBlocks   int   // blocks resident in the block cache
	CacheBytes    int64 // bytes resident in the block cache
	CacheHits     int64 // cumulative block-cache hits since open
	CacheMisses   int64 // cumulative block-cache misses (each is a disk read) since open
	OpenFiles     int   // table descriptors currently open
	// BackgroundError is the first terminal WAL, flush, or compaction failure.
	// It remains set because subsequent writes are rejected until reopen.
	BackgroundError error
	// TablesPerDepth maps tier depth to the number of live tables at that depth,
	// so callers can see the shape of the LSM (fresh vs. bottom tiers).
	TablesPerDepth map[int]int

	// Cumulative write-path counters since open, for write-amplification and
	// backlog health on a single spindle.
	CompactionCount        int64
	CompactionBytesRead    int64
	CompactionBytesWritten int64
	FlushCount             int64
	FlushBytesWritten      int64
	WALBytesWritten        int64
	WriteStalls            int64 // times a write was throttled by backpressure

	// PerShard breaks the table counts and sizes down by shard so an operator can
	// spot partition skew and target CompactShard at the bloated one.
	PerShard []ShardStats
}

// ShardStats is the per-shard slice of Stats.
type ShardStats struct {
	Index          int
	Tables         int
	TablesSize     int64
	TablesPerDepth map[int]int
}

// Stats returns a snapshot of database state. It is safe to call concurrently.
func (db *DB) Stats() (Stats, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return Stats{}, ErrClosed
	}

	hits, misses := db.cache.stats()
	var walBytes int64
	if db.wal != nil { // nil in read-only mode
		walBytes = db.wal.bytesWritten()
	}
	st := Stats{
		Shards:                 db.opts.ShardCount,
		LiveSnapshots:          db.snaps.live(),
		CacheBlocks:            db.cache.Len(),
		CacheBytes:             db.cache.Size(),
		CacheHits:              hits,
		CacheMisses:            misses,
		TablesPerDepth:         map[int]int{},
		CompactionCount:        db.metrics.compactionCount.Load(),
		CompactionBytesRead:    db.metrics.compactionBytesRead.Load(),
		CompactionBytesWritten: db.metrics.compactionBytesWritten.Load(),
		FlushCount:             db.metrics.flushCount.Load(),
		FlushBytesWritten:      db.metrics.flushBytesWritten.Load(),
		WALBytesWritten:        walBytes,
		WriteStalls:            db.metrics.writeStalls.Load(),
		PerShard:               make([]ShardStats, len(db.shards)),
	}
	st.BackgroundError = db.backgroundError()
	db.fds.mu.Lock()
	st.OpenFiles = db.fds.open
	db.fds.mu.Unlock()

	for i, s := range db.shards {
		ss := ShardStats{Index: i, TablesPerDepth: map[int]int{}}
		for _, t := range s.Tables() {
			st.Tables++
			st.TablesSize += t.Size
			st.TablesPerDepth[t.Depth]++
			ss.Tables++
			ss.TablesSize += t.Size
			ss.TablesPerDepth[t.Depth]++
		}
		st.PerShard[i] = ss
	}
	return st, nil
}

// GetProperty returns a named database property as a string, in the style of
// LevelDB. Supported names: "levisdb.num-tables", "levisdb.tables-size",
// "levisdb.live-snapshots", "levisdb.cache-blocks", "levisdb.cache-bytes",
// "levisdb.cache-hits", "levisdb.cache-misses", "levisdb.open-files",
// "levisdb.tables-per-depth", "levisdb.compaction-count",
// "levisdb.compaction-bytes-read", "levisdb.compaction-bytes-written",
// "levisdb.flush-count", "levisdb.flush-bytes-written",
// "levisdb.wal-bytes-written", "levisdb.write-stalls",
// "levisdb.background-error".
func (db *DB) GetProperty(name string) (string, error) {
	st, err := db.Stats()
	if err != nil {
		return "", err
	}
	switch name {
	case "levisdb.num-tables":
		return strconv.Itoa(st.Tables), nil
	case "levisdb.tables-size":
		return strconv.FormatInt(st.TablesSize, 10), nil
	case "levisdb.live-snapshots":
		return strconv.Itoa(st.LiveSnapshots), nil
	case "levisdb.cache-blocks":
		return strconv.Itoa(st.CacheBlocks), nil
	case "levisdb.cache-bytes":
		return strconv.FormatInt(st.CacheBytes, 10), nil
	case "levisdb.cache-hits":
		return strconv.FormatInt(st.CacheHits, 10), nil
	case "levisdb.cache-misses":
		return strconv.FormatInt(st.CacheMisses, 10), nil
	case "levisdb.open-files":
		return strconv.Itoa(st.OpenFiles), nil
	case "levisdb.tables-per-depth":
		return formatTablesPerDepth(st.TablesPerDepth), nil
	case "levisdb.compaction-count":
		return strconv.FormatInt(st.CompactionCount, 10), nil
	case "levisdb.compaction-bytes-read":
		return strconv.FormatInt(st.CompactionBytesRead, 10), nil
	case "levisdb.compaction-bytes-written":
		return strconv.FormatInt(st.CompactionBytesWritten, 10), nil
	case "levisdb.flush-count":
		return strconv.FormatInt(st.FlushCount, 10), nil
	case "levisdb.flush-bytes-written":
		return strconv.FormatInt(st.FlushBytesWritten, 10), nil
	case "levisdb.wal-bytes-written":
		return strconv.FormatInt(st.WALBytesWritten, 10), nil
	case "levisdb.write-stalls":
		return strconv.FormatInt(st.WriteStalls, 10), nil
	case "levisdb.background-error":
		if st.BackgroundError == nil {
			return "", nil
		}
		return st.BackgroundError.Error(), nil
	default:
		return "", fmt.Errorf("levisdb: unknown property %q", name)
	}
}

// formatTablesPerDepth renders the depth histogram deterministically as
// "depth:count" pairs ordered by depth, e.g. "0:3 1:1".
func formatTablesPerDepth(m map[int]int) string {
	depths := make([]int, 0, len(m))
	for d := range m {
		depths = append(depths, d)
	}
	sort.Ints(depths)
	out := ""
	for i, d := range depths {
		if i > 0 {
			out += " "
		}
		out += fmt.Sprintf("%d:%d", d, m[d])
	}
	return out
}
