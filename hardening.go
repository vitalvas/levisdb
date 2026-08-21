package levisdb

// This file documents the durability and concurrency invariants that
// hardening_test.go exercises end to end. They are enforced by code spread
// across db.go, wal.go, engine_shard.go, and the manifest, but collected here
// so the guarantees the hardening tests protect are stated in one place.
//
// Crash recovery:
//   - A crash leaves the on-disk state at a record boundary; WAL replay stops at
//     the last intact record (torn tails and, under lenient recovery, corrupt
//     records are tolerated).
//   - Recovered writes are flushed to tables and captured by a fresh manifest
//     baseline before their WAL segments are removed, so a second crash mid-
//     recovery cannot lose them.
//
// Read/compaction isolation:
//   - A point read or iterator pins its snapshot sequence (and TTL view) so a
//     concurrent compaction cannot reclaim a version it may still return.
//
// Group commit ordering:
//   - Concurrent batches are assigned sequence numbers and applied to memtables
//     in commit order; a batch's waiter is released only after it is durable and
//     applied.
//
// Background-error latching:
//   - The first terminal WAL/flush/compaction failure is latched and rejects all
//     subsequent writes until reopen.
