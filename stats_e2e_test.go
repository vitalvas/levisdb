package levisdb

import (
	"os"
	"testing"
)

// This file is an opt-in I/O-shape harness, run manually with -bench, never by the
// gated test suite (benchmarks need -bench to execute). It measures amplification
// metrics that are independent of the underlying device, then projects a per-lookup
// read cost by multiplying the measured block-read count by a configurable
// per-seek service time, so the same run characterizes storage with any seek cost:
//
//	go test -run '^$' -bench 'BenchmarkIOShape' -benchtime 1x .
//
// Reported custom metrics (per b.ReportMetric, so they show in -bench output):
//   - read-amp      : tables probed per point lookup (structural read fan-out)
//   - blk-rd/read   : block reads (cache misses) per point lookup -- seeks per read
//   - write-amp     : bytes written to disk (flush+compaction+WAL) / logical bytes
//   - space-amp     : on-disk bytes / live logical bytes
//   - seek-ms/read  : projected per-lookup read latency, blk-rd/read x seekMillis
//                     (a cold-cache upper bound; a warm cache reads fewer blocks)
//
// read-amp and blk-rd/read are the numbers to drive down where random reads are
// expensive; write-amp characterizes compaction I/O. Raise seekMillis to model
// seek-bound storage, lower it for fast random-access storage.

// seekMillis is a representative random-read service time used only to project the
// per-lookup cost; it is never measured here. The default models a seek-bound
// device; adjust it to the target storage's random-read latency.
const seekMillis = 8.5

// ioShape loads n keys of valSize via opts, runs readOps random point lookups,
// and reports the amplification metrics. logicalBytes is the live key+value total.
func ioShape(b *testing.B, name string, memtable int64, n, valSize, readOps int) {
	b.Run(name, func(b *testing.B) {
		for iter := 0; iter < b.N; iter++ {
			dir := b.TempDir()
			o := DefaultOptions(dir)
			o.MemtableSize = memtable
			o.NoSync = true // measure I/O shape, not fsync latency
			db, err := Open(o)
			if err != nil {
				b.Fatal(err)
			}

			val := make([]byte, valSize)
			bulkLoad(b, db, n, val)
			logical := int64(n) * int64(8+valSize)

			pre, err := db.Stats()
			if err != nil {
				b.Fatal(err)
			}
			for i := 0; i < readOps; i++ {
				k := seqKey(int((uint64(i) * 2654435761) % uint64(n)))
				if _, err := db.Get(k); err != nil {
					b.Fatal(err)
				}
			}
			post, err := db.Stats()
			if err != nil {
				b.Fatal(err)
			}

			reads := float64(post.PointReads - pre.PointReads)
			probes := float64(post.TableProbes - pre.TableProbes)
			blockReads := float64(post.CacheMisses - pre.CacheMisses)
			bloomFP := float64(post.BloomFalsePositives - pre.BloomFalsePositives)
			diskWritten := float64(post.FlushBytesWritten + post.CompactionBytesWritten + post.WALBytesWritten)
			userBytes := float64(post.UserBytesWritten) // exact logical volume, from Stats
			onDisk := dirBytes(b, dir)

			b.ReportMetric(probes/reads, "read-amp")
			b.ReportMetric(blockReads/reads, "blk-rd/read")
			b.ReportMetric(bloomFP/reads, "bloomFP/read")
			b.ReportMetric(diskWritten/userBytes, "write-amp")
			b.ReportMetric(float64(onDisk)/float64(logical), "space-amp")
			b.ReportMetric((blockReads/reads)*seekMillis, "seek-ms/read")
			// Suppress the default ns/op: these runs are I/O-shape probes, not timing.
			b.ReportMetric(0, "ns/op")

			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// dirBytes sums the sizes of the regular files in dir (the on-disk footprint).
func dirBytes(b *testing.B, dir string) int64 {
	b.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		b.Fatal(err)
	}
	var total int64
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			b.Fatal(err)
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
	}
	return total
}

// BenchmarkIOShape sweeps representative workloads. Scale n up (and -benchtime 1x)
// for a larger, more realistic picture; the defaults keep a manual run to a few
// seconds.
func BenchmarkIOShape(b *testing.B) {
	const (
		small = 1 << 20 // 1 MiB memtable: many tiers, higher read fan-out
		large = 8 << 20 // default-ish memtable
	)
	// Read-heavy: lots of lookups over a large dataset; read-amp and blk-rd/read
	// dominate the projected read cost.
	ioShape(b, "read-heavy/small-memtable", small, 200_000, 100, 200_000)
	ioShape(b, "read-heavy/large-memtable", large, 200_000, 100, 200_000)
	// Write-heavy bulk load with modest reads: write-amp dominates.
	ioShape(b, "write-heavy", small, 500_000, 200, 10_000)
	// Big values: tests whether large values inflate compaction (no KV separation).
	ioShape(b, "large-values", large, 50_000, 4096, 50_000)
}

// Example output row, read like:
//
//	BenchmarkIOShape/read-heavy/small-memtable-16  1  ... 3.2 read-amp  1.8 blk-rd/read  ... 15.3 seek-ms/read
//
// means ~3.2 tables probed and ~1.8 block reads per lookup -> ~15ms/read projected
// at the configured per-seek cost; driving blk-rd/read toward 1.0 (bloom/cache) or
// read-amp down (fewer overlapping tables) is the win where random reads are slow.
