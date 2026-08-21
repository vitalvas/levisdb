package levisdb

import (
	"expvar"
	"strconv"
	"sync"
	"sync/atomic"
)

// dbMetrics holds cumulative counters since open. Write amplification (bytes a
// compaction rewrites per user byte) is the key health metric for one slow
// spindle, so these are captured at the choke points where the byte counts are
// already known, adding no I/O.
type dbMetrics struct {
	compactionCount        atomic.Int64
	compactionBytesRead    atomic.Int64
	compactionBytesWritten atomic.Int64
	flushCount             atomic.Int64
	flushBytesWritten      atomic.Int64
	writeStalls            atomic.Int64 // times a write was throttled by backpressure
	// WAL bytes are counted by the WAL itself (walT.bytesWritten), which survives
	// segment rotation, so there is no counter for them here.
}

// metricsRegistry tracks the open databases whose stats the process-level
// "levisdb" expvar reports. Registration is by pointer, so several databases
// (or several opens of the same dir) coexist without colliding on an expvar
// name. Closed databases deregister, so nothing leaks for the process lifetime
// the way a per-open expvar.Publish would.
var metricsRegistry = struct {
	mu  sync.Mutex
	dbs map[*DB]struct{}
}{dbs: map[*DB]struct{}{}}

// expvarOnce guards the single process-wide Publish so opening many databases
// never panics on a duplicate expvar name.
var expvarOnce sync.Once

// registerMetrics adds db to the expvar view and publishes the "levisdb" var on
// first use. The var is an expvar.Func, so it snapshots live stats only when
// scraped rather than mirroring every counter update.
func registerMetrics(db *DB) {
	expvarOnce.Do(func() {
		expvar.Publish("levisdb", expvar.Func(collectMetrics))
	})
	metricsRegistry.mu.Lock()
	metricsRegistry.dbs[db] = struct{}{}
	metricsRegistry.mu.Unlock()
}

// deregisterMetrics removes db from the expvar view on Close.
func deregisterMetrics(db *DB) {
	metricsRegistry.mu.Lock()
	delete(metricsRegistry.dbs, db)
	metricsRegistry.mu.Unlock()
}

// collectMetrics is the expvar.Func backing the "levisdb" var. It returns a
// JSON-serializable map of dir -> Stats for every open database. A database that
// closed mid-scrape simply reports an error entry rather than failing the scrape.
func collectMetrics() any {
	metricsRegistry.mu.Lock()
	dbs := make([]*DB, 0, len(metricsRegistry.dbs))
	for db := range metricsRegistry.dbs {
		dbs = append(dbs, db)
	}
	metricsRegistry.mu.Unlock()

	out := make(map[string]any, len(dbs))
	for _, db := range dbs {
		st, err := db.Stats()
		if err != nil {
			out[db.opts.Dir] = map[string]any{"error": err.Error()}
			continue
		}
		out[db.opts.Dir] = statsToMap(st)
	}
	return out
}

// statsToMap renders a Stats snapshot as a JSON-serializable map with the same
// names GetProperty uses, so expvar and GetProperty agree.
func statsToMap(st Stats) map[string]any {
	m := map[string]any{
		"num-tables":               st.Tables,
		"tables-size":              st.TablesSize,
		"live-snapshots":           st.LiveSnapshots,
		"cache-blocks":             st.CacheBlocks,
		"cache-bytes":              st.CacheBytes,
		"cache-hits":               st.CacheHits,
		"cache-misses":             st.CacheMisses,
		"open-files":               st.OpenFiles,
		"tables-per-depth":         depthMap(st.TablesPerDepth),
		"compaction-count":         st.CompactionCount,
		"compaction-bytes-read":    st.CompactionBytesRead,
		"compaction-bytes-written": st.CompactionBytesWritten,
		"flush-count":              st.FlushCount,
		"flush-bytes-written":      st.FlushBytesWritten,
		"wal-bytes-written":        st.WALBytesWritten,
		"write-stalls":             st.WriteStalls,
	}
	if st.BackgroundError != nil {
		m["background-error"] = st.BackgroundError.Error()
	}
	return m
}

// depthMap converts the int-keyed depth histogram to a string-keyed map so it
// serializes as a JSON object.
func depthMap(m map[int]int) map[string]int {
	out := make(map[string]int, len(m))
	for d, c := range m {
		out[strconv.Itoa(d)] = c
	}
	return out
}
