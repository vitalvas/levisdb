package levisdb

import (
	"encoding/json"
	"expvar"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scrapeLevisdbExpvar decodes the process-level "levisdb" expvar into a
// dir -> stats map, the way an expvar HTTP scrape would.
func scrapeLevisdbExpvar(t *testing.T) map[string]map[string]any {
	t.Helper()
	v := expvar.Get("levisdb")
	require.NotNil(t, v, "levisdb expvar not published")
	var out map[string]map[string]any
	require.NoError(t, json.Unmarshal([]byte(v.String()), &out))
	return out
}

func TestExpvarReportsOpenDBStats(t *testing.T) {
	// Not parallel: asserts on the process-global expvar registry.
	// One shard + tiny memtable so a small burst deterministically flushes
	// (rather than spreading thinly across many shards).
	db := openTestDB(t, func(o *Options) {
		o.ShardCount = 1
		o.MemtableSize = 256
	})
	dir := db.opts.Dir

	for i := 0; i < 60; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%04d", i)), Value: []byte("v")}))
	}
	db.sched.drain()

	got := scrapeLevisdbExpvar(t)
	entry, ok := got[dir]
	require.True(t, ok, "expvar missing entry for %q; have %v keys", dir, len(got))

	// JSON numbers decode as float64; compare numerically.
	assert.Positive(t, entry["num-tables"], "flushed data should show tables")
	assert.Positive(t, entry["flush-count"], "flushes counted")
	assert.Positive(t, entry["wal-bytes-written"], "WAL bytes counted")
	// Cache accesses appear after reads.
	for i := 0; i < 40; i++ {
		_, _ = db.Get([]byte(fmt.Sprintf("k%04d", i)))
	}
	got = scrapeLevisdbExpvar(t)
	entry = got[dir]
	hits, _ := entry["cache-hits"].(float64)
	misses, _ := entry["cache-misses"].(float64)
	assert.Positive(t, hits+misses, "reads recorded cache accesses")
}

func TestExpvarDeregistersOnClose(t *testing.T) {
	// Not parallel: asserts on the process-global expvar registry.
	dir := t.TempDir()
	db, err := Open(func() Options {
		o := DefaultOptions(dir)
		o.ShardCount = 2
		return o
	}())
	require.NoError(t, err)

	assert.Contains(t, scrapeLevisdbExpvar(t), dir, "open DB should appear in expvar")

	require.NoError(t, db.Close())
	assert.NotContains(t, scrapeLevisdbExpvar(t), dir, "closed DB should be deregistered from expvar")
}

func TestExpvarHandlesMultipleDBs(t *testing.T) {
	// Two independent DBs must both appear without a duplicate-name panic.
	a := openTestDB(t, nil)
	b := openTestDB(t, nil)
	require.NoError(t, a.Put(PutOptions{Key: []byte("x"), Value: []byte("1")}))
	require.NoError(t, b.Put(PutOptions{Key: []byte("y"), Value: []byte("2")}))

	got := scrapeLevisdbExpvar(t)
	assert.Contains(t, got, a.opts.Dir)
	assert.Contains(t, got, b.opts.Dir)
}
