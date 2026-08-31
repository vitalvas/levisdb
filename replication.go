// Replication helpers expose the committed write stream for catch-up: a lagging
// consumer records the sequence it has applied and later calls GetUpdatesSince to
// replay every committed mutation past that point, in order, from the retained
// and live WAL segments. Enabling retention (Options.WALRetention /
// WALRetentionBytes) keeps flushed segments on disk long enough to serve this;
// with retention off only the live segment is available.

package levisdb

import (
	"fmt"
	"os"
	"time"
)

// walPath returns the on-disk path of a WAL segment. logPath never errors for a
// flat layout, so the error is dropped here for call sites that only need a path.
func (db *DB) walPath(num uint32) string {
	p, _ := db.store.logPath(num)
	return p
}

// reapRetainedWAL deletes flushed WAL segments left on disk for retention once
// they are past BOTH the time and byte horizons. It is disk-driven: it lists the
// .log files (excluding the live segment, whose number is liveNum), sums their
// sizes, and deletes the oldest-first while the total exceeds WALRetentionBytes
// and each file is older than WALRetention. A segment survives while either
// horizon still covers it. Deleting a segment advances reapedThroughSeq to that
// segment's highest sequence, so GetUpdatesSince reports ErrRetentionExpired for
// requests below it. Called under db.checkpointMu. Best-effort: a failed delete
// or stat leaves the segment for the next reap.
func (db *DB) reapRetainedWAL(liveNum uint32) {
	if db.wal == nil {
		return
	}
	nums, err := db.store.listLogs()
	if err != nil {
		return
	}
	// Candidates are every retained (non-live) segment, ascending == oldest first.
	type cand struct {
		num   uint32
		size  int64
		mtime int64
	}
	var cands []cand
	var total int64
	for _, num := range nums {
		if num == liveNum {
			continue
		}
		fi, statErr := os.Stat(db.walPath(num))
		if statErr != nil {
			continue
		}
		cands = append(cands, cand{num: num, size: fi.Size(), mtime: fi.ModTime().UnixNano()})
		total += fi.Size()
	}
	now := time.Now().UnixNano()
	ret := db.opts.WALRetention
	maxBytes := db.opts.WALRetentionBytes
	// Always keep the newest retained segment (the last candidate), even past both
	// horizons, so a consumer only a little behind the live segment can still catch
	// up: the newest committed writes flushed out of the live segment live here.
	// Only older segments are eligible for deletion.
	for i := 0; i+1 < len(cands); i++ {
		c := cands[i]
		ageOK := ret > 0 && time.Duration(now-c.mtime) < ret
		bytesOK := maxBytes > 0 && total <= maxBytes
		if ageOK || bytesOK {
			// Within at least one horizon: keep it and everything newer (later in the
			// ascending list is newer, so stop scanning to preserve the contiguous-seq
			// property GetUpdatesSince relies on). This segment is the oldest survivor;
			// advance the horizon to just below its first sequence so GetUpdatesSince
			// rejects any earlier request exactly. Deriving the horizon from the
			// survivor's FIRST seq (not the deleted segment's max) is immune to a
			// torn/rotted tail under-reporting a deleted segment's contents.
			db.advanceReapHorizon(db.firstSeqBefore(c.num))
			return
		}
		// Past both horizons: remove it. reapRetainedWAL runs under checkpointMu,
		// which GetUpdatesSince also holds, so no reader can observe the file gone
		// before the horizon advances.
		if err := db.store.removeLog(c.num); err != nil {
			continue // retry next reap
		}
		total -= c.size
	}
	// Every deletable segment was removed; the newest retained segment is kept, so
	// the horizon is just below its first sequence.
	if len(cands) > 0 {
		db.advanceReapHorizon(db.firstSeqBefore(cands[len(cands)-1].num))
		return
	}
	// No retained segments at all; nothing below the live segment survives.
	// Advance the horizon to just below the live segment's first sequence.
	db.advanceReapHorizon(db.firstSeqBefore(liveNum))
}

// advanceReapHorizon moves reapedThroughSeq forward to h (never backward).
func (db *DB) advanceReapHorizon(h uint64) {
	if h > db.wal.reapedThroughSeq.Load() {
		db.wal.reapedThroughSeq.Store(h)
	}
}

// firstSeqBefore returns one less than the first (lowest) sequence in the segment
// with the given number: the highest sequence NOT recoverable once every earlier
// segment is deleted. It reads only until the first record. Returns 0 if the
// segment cannot be read or is empty, which leaves the horizon unchanged.
func (db *DB) firstSeqBefore(num uint32) uint64 {
	f, err := os.Open(db.walPath(num))
	if err != nil {
		return 0
	}
	defer f.Close()
	var first uint64
	_, _ = replayWALFileVisit(f, 0, true, func(entries []walEntry) error {
		if len(entries) > 0 {
			first = entries[0].Seq
			return errWALStop // only the first record is needed
		}
		return nil
	})
	if first == 0 {
		return 0
	}
	return first - 1
}

// WALUpdates streams committed mutations with a sequence greater than a caller-
// supplied watermark, in commit order, one batch per Next. GetUpdatesSince fully
// materializes the batches up front (the retained window is bounded by the
// retention options), so the iterator holds no lock or open file and no reaper can
// delete a segment out from under it. It is NOT safe for concurrent use.
type WALUpdates struct {
	batches [][]WALEntry
	pos     int
}

// GetUpdatesSince returns a stream of every committed mutation with sequence
// greater than since, in commit order, for replication or change-data-capture
// catch-up. The consumer typically passes the highest Seq it has durably applied.
//
// It requires WAL retention (Options.WALRetention / WALRetentionBytes) to serve
// sequences already flushed and retired; with retention off only mutations still
// in the live segment are available. If since is below the oldest sequence still
// on disk, GetUpdatesSince returns ErrRetentionExpired and the consumer must
// re-bootstrap from a Snapshot and resume from its Seq. Passing the current
// LatestSeq yields an empty stream (the consumer is already caught up).
//
// The returned stream must be closed. Delivery is at-least-once: the consumer may
// re-see entries it already applied (those with Seq <= its watermark are filtered,
// but a batch boundary can straddle the watermark), so it must be idempotent.
func (db *DB) GetUpdatesSince(since uint64) (*WALUpdates, error) {
	// Hold checkpointMu across the whole read so the retention reaper (which runs
	// under checkpointMu) cannot delete a segment mid-read and open a silent gap.
	// The lock order is checkpointMu-before-db.mu, matching the checkpoint path.
	db.checkpointMu.Lock()
	defer db.checkpointMu.Unlock()

	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	if db.wal == nil {
		db.mu.RUnlock()
		return nil, ErrReadOnly // read-only opens have no live WAL to tail
	}
	// Coverage check. The lowest sequence still on disk is one past whatever the
	// reaper has deleted (reapedThroughSeq); anything at or below that is gone. With
	// retention off, nothing is retained past flush, so the horizon is the
	// flushed-and-retired watermark (filterSafeSeq): a consumer behind it cannot be
	// served from the live segment alone. reapedThroughSeq cannot advance while we
	// hold checkpointMu, so this check stays valid through the read below.
	horizon := db.wal.reapedThroughSeq.Load()
	if db.opts.WALRetention == 0 && db.opts.WALRetentionBytes == 0 {
		if fs := db.filterSafeSeq.Load(); fs > horizon {
			horizon = fs
		}
	}
	if since < horizon {
		db.mu.RUnlock()
		return nil, ErrRetentionExpired
	}
	// Snapshot the on-disk segment list (retained oldest-first plus the live
	// segment). listLogs returns numbers ascending, which is commit/seq order
	// because segment numbers come from the monotonic allocator.
	segs, err := db.store.listLogs()
	db.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	// Materialize while still holding checkpointMu: no reap can delete a listed
	// segment, so a failed open is a genuine error rather than a benign race that
	// would silently drop mutations.
	u := &WALUpdates{}
	var maxSeen uint64
	for _, num := range segs {
		f, oerr := os.Open(db.walPath(num))
		if oerr != nil {
			return nil, fmt.Errorf("wal: open segment for updates: %w", oerr)
		}
		_, verr := replayWALFileVisit(f, 0, true, func(entries []walEntry) error {
			if batch := convertWALBatch(entries, since, &maxSeen); len(batch) > 0 {
				u.batches = append(u.batches, batch)
			}
			return nil
		})
		_ = f.Close()
		// A lenient stop (errWALStop) or clean EOF ends this segment; keep going.
		if verr != nil && verr != errWALStop {
			return nil, fmt.Errorf("wal: read updates: %w", verr)
		}
	}
	return u, nil
}

// convertWALBatch converts one batch's internal entries to public WALEntry,
// keeping only those with Seq greater than the watermark and past any already-
// emitted seq (guards against a duplicate straddling the retained/live boundary).
// maxSeen tracks the highest emitted seq across the whole stream.
func convertWALBatch(entries []walEntry, since uint64, maxSeen *uint64) []WALEntry {
	out := make([]WALEntry, 0, len(entries))
	for i := range entries {
		e := &entries[i]
		if e.Seq <= since || e.Seq <= *maxSeen {
			continue
		}
		*maxSeen = e.Seq
		we := WALEntry{
			Seq:   e.Seq,
			Kind:  publicKind(e.Kind),
			Key:   append([]byte(nil), e.Key...),
			Value: append([]byte(nil), e.Value...),
		}
		if e.ExpiresAt != 0 {
			we.ExpiresAt = e.ExpiresAt
			we.TTL = time.Until(time.Unix(0, e.ExpiresAt))
		}
		out = append(out, we)
	}
	return out
}

// Next advances to the next batch of updates and reports whether one exists.
func (u *WALUpdates) Next() bool {
	return u.pos < len(u.batches)
}

// Batch returns the current batch and advances the cursor. Each batch holds every
// entry in one committed batch with Seq greater than the watermark; its slices are
// owned by the caller (copied out of the WAL buffer), so they stay valid. It
// returns nil when the stream is exhausted, so calling it without Next never
// panics.
func (u *WALUpdates) Batch() []WALEntry {
	if u.pos >= len(u.batches) {
		return nil
	}
	b := u.batches[u.pos]
	u.pos++
	return b
}

// Error returns any error accumulated during iteration. The stream is fully
// materialized up front, so any error is returned from GetUpdatesSince instead
// and this always reports nil; it exists so callers can use the standard idiom.
func (u *WALUpdates) Error() error { return nil }

// Close releases resources. The stream is fully materialized up front, so there
// is nothing to release; the method exists so callers can always defer Close.
func (u *WALUpdates) Close() error { return nil }
