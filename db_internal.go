package levisdb

import (
	"fmt"
	"os"
	"time"
)

// recoverWAL replays leftover WAL segments into shard memtables and flushes
// them to tables. The caller must install a manifest baseline containing those
// tables before removing the WAL segments.
//
// Every intact entry is replayed rather than skipping by a global watermark:
// durability is per-shard, so an entry may still be live in one shard's
// memtable even after another shard's flush advanced the global sequence.
// Re-applying an entry already present in a table is harmless because the
// internal key (user key + seq + kind) is identical, so no version is
// duplicated. A torn tail from the crash is ignored by the WAL reader, so
// replay stops at the last intact record.
func (db *DB) recoverWAL(shardLogs [][]uint32, startSeq uint64) (uint64, error) {
	return db.recoverWALMode(shardLogs, startSeq, true)
}

// logRecovered reports the outcome of WAL replay. Extracted so recoverWALMode
// stays under the cyclomatic-complexity limit.
func (db *DB) logRecovered(startSeq, maxSeq uint64) {
	if maxSeq > startSeq {
		db.log.Info("wal recovery replayed entries",
			"op", "recover", "start_seq", startSeq, "recovered_seq", maxSeq)
	}
}

// errWALStop signals a recoverable stop point during lenient WAL replay: the
// intact prefix up to here is kept and no further segments are replayed.
var errWALStop = fmt.Errorf("wal: recoverable stop")

// recoverWALMode replays each shard's leftover WAL segments into that shard's
// memtable. shardLogs[i] is shard i's segment numbers, ascending. Sequence
// numbers are global (assigned by the DB), but each shard's own segments carry
// ascending seqs, so the monotonicity check is per shard; across shards seqs
// interleave and are not comparable.
func (db *DB) recoverWALMode(shardLogs [][]uint32, startSeq uint64, flush bool) (uint64, error) {
	maxSeq := startSeq
	lenient := !db.opts.StrictWALRecovery
	for shard := 0; shard < len(shardLogs); shard++ {
		var lastWALSeq uint64
		s := db.shards[shard]
		stopped := false
		for _, num := range shardLogs[shard] {
			path, perr := db.store.logPath(shard, num)
			if perr != nil {
				return maxSeq, perr
			}
			f, err := os.Open(path)
			if err != nil {
				return maxSeq, err
			}
			seq, err := replayWALFileVisit(f, startSeq, lenient, func(entries []walEntry) error {
				for i := range entries {
					e := &entries[i]
					if e.Seq <= lastWALSeq {
						// A regressing sequence within a shard's segments is a corruption
						// signal. Lenient recovery stops and keeps the prefix; strict fails.
						if lenient {
							return errWALStop
						}
						return fmt.Errorf("wal: non-increasing sequence %d after %d in shard %d", e.Seq, lastWALSeq, shard)
					}
					lastWALSeq = e.Seq
					// The shard-ownership, empty-key, and bounds checks below are
					// SEMANTIC: a valid-CRC record whose contents are logically
					// impossible signals a format/logic problem or tampering, not bit
					// rot, so they always fail Open even in lenient mode.
					if e.Shard != shard {
						return fmt.Errorf("wal: shard %d segment holds record for shard %d", shard, e.Shard)
					}
					if len(e.Key) == 0 {
						return fmt.Errorf("wal: empty key")
					}
					expectedShard, err := db.shardForKey(e.Key)
					if err != nil {
						return fmt.Errorf("wal: %w", err)
					}
					if expectedShard != e.Shard {
						return fmt.Errorf("wal: key belongs to shard %d, record names shard %d", expectedShard, e.Shard)
					}
					if e.Seq > maxIKeySeq {
						return fmt.Errorf("wal: sequence %d exceeds internal-key limit", e.Seq)
					}
					// An entry past the size limit would corrupt the skiplist arena or a
					// data block on apply; reject it like the other semantic checks. A
					// TTL entry re-encodes the expiry prefix, so count it.
					entrySize := len(e.Key) + len(e.Value)
					if e.Kind == walKindPutTTL {
						entrySize += expiryPrefixLen
					}
					if entrySize > maxEntrySize {
						return fmt.Errorf("wal: entry size %d exceeds limit %d", entrySize, maxEntrySize)
					}
					switch e.Kind {
					case walKindDelete:
						s.del(e.Seq, e.Key)
					case walKindPutTTL:
						s.putTTL(e.Seq, e.Key, e.Value, e.ExpiresAt)
					default:
						s.Put(e.Seq, e.Key, e.Value)
					}
					// Bound skiplist arena growth once a shard reaches its threshold.
					// Writable recovery flushes to an SSTable; read-only recovery seals
					// the memtable in memory so Open preserves its non-mutation contract.
					if s.needFlush() {
						if flush {
							if ferr := s.Flush(); ferr != nil {
								return ferr
							}
						} else {
							s.sealReadOnlyRecoveryMemtable()
						}
					}
				}
				return nil
			})
			closeErr := f.Close()
			if seq > maxSeq {
				maxSeq = seq
			}
			if err == errWALStop {
				// Recoverable stop: keep this shard's prefix, skip its later segments.
				if closeErr != nil {
					return maxSeq, closeErr
				}
				db.log.Warn("wal recovery stopped at corruption; kept intact prefix",
					"op", "recover", "shard", shard, "segment", num, "recovered_seq", maxSeq)
				stopped = true
				break
			}
			if err != nil {
				return maxSeq, err
			}
			if closeErr != nil {
				return maxSeq, closeErr
			}
		}
		_ = stopped
	}

	// Flush the recovered memtables to tables so the data is durable before we
	// delete the WAL segments that held it; otherwise a second crash before the
	// next flush would lose the just-recovered writes. The freshly written
	// tables are captured by openManifest's baseline, which runs next, so no
	// manifest edit is recorded here (db.man does not exist yet).
	if maxSeq > db.readSeq.Load() {
		db.readSeq.Store(maxSeq)
	}
	db.logRecovered(startSeq, maxSeq)
	if !flush {
		return maxSeq, nil
	}
	for _, s := range db.shards {
		if s.memEmpty() {
			continue
		}
		if err := s.Flush(); err != nil {
			return maxSeq, err
		}
	}

	return maxSeq, nil
}

// replayManifest reads the manifest referenced by CURRENT into a State.
func (db *DB) replayManifest(num uint32) (*manifestState, error) {
	f, err := os.Open(db.store.manifestPath(num))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return replayManifestFile(f)
}

// highestFileNum returns the largest file number seen in state so the allocator
// resumes above it. Manifest and WAL numbers are also below this in practice
// because table numbers are allocated from the same counter.
func (db *DB) highestFileNum(state *manifestState) uint32 {
	if state == nil {
		return 0
	}
	var maxNum uint32
	for _, tables := range state.Tables {
		for _, t := range tables {
			if t.Num > maxNum {
				maxNum = t.Num
			}
		}
	}
	return maxNum
}

// restoreTables opens each table recorded in the manifest into its shard in
// deterministic ascending file-number order. Compaction outputs can contain
// older data despite newer file numbers, so point reads compare sequences.
func (db *DB) restoreTables(state *manifestState) error {
	for shardIdx, tables := range state.Tables {
		if shardIdx < 0 || shardIdx >= len(db.shards) {
			return fmt.Errorf("manifest: shard %d outside [0,%d)", shardIdx, len(db.shards))
		}
		ordered := make([]manifestTableInfo, len(tables))
		copy(ordered, tables)
		sortTablesByNum(ordered)
		for _, t := range ordered {
			path, err := db.store.tablePath(shardIdx, t.Num)
			if err != nil {
				return err
			}
			info, err := os.Stat(path)
			if err != nil {
				return err
			}
			if info.Size() != t.Size {
				return fmt.Errorf("manifest: table %d size mismatch: recorded %d, actual %d", t.Num, t.Size, info.Size())
			}
			if err := db.shards[shardIdx].openTable(tableSpec{
				num:        t.Num,
				depth:      t.Depth,
				path:       path,
				size:       t.Size,
				minKey:     t.MinKey,
				maxKey:     t.MaxKey,
				entries:    t.Entries,
				tombstones: t.Tombstones,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func sortTablesByNum(ts []manifestTableInfo) {
	for i := 1; i < len(ts); i++ {
		for j := i; j > 0 && ts[j-1].Num > ts[j].Num; j-- {
			ts[j-1], ts[j] = ts[j], ts[j-1]
		}
	}
}

// walBridge returns the internal WAL observer that forwards committed entries
// to the user's WALObserver, translating internal types to public ones. It
// returns nil when no observer is configured.
func (db *DB) walBridge() walObserver {
	if db.opts.WALObserver == nil {
		return nil
	}
	return &observerBridge{obs: db.opts.WALObserver}
}

type observerBridge struct {
	obs WALObserver
}

func (b *observerBridge) observe(batch []walEntry) {
	out := make([]WALEntry, len(batch))
	for i, e := range batch {
		out[i] = WALEntry{
			Seq:   e.Seq,
			Shard: e.Shard,
			Kind:  publicKind(e.Kind),
			Key:   e.Key,
			Value: e.Value,
		}
		if e.ExpiresAt != 0 {
			out[i].TTL = time.Until(time.Unix(0, e.ExpiresAt))
		}
	}
	b.obs.Observe(out)
}

func publicKind(k walKindType) EntryKind {
	if k == walKindDelete {
		return EntryDelete
	}
	return EntryPut
}

// commitTableChange durably records a table replacement and then installs it
// while holding the manifest serialization lock. Manifest rotation therefore
// snapshots the newly-installed in-memory state rather than a stale one.
func (db *DB) commitTableChange(shard int, inputs, outputs []*tableMeta, install func()) error {
	// Count bytes at this single choke point (flush and compaction both route
	// here). inputs==nil marks a flush; inputs!=nil a compaction.
	var outBytes int64
	for _, t := range outputs {
		outBytes += t.size
	}
	if inputs == nil {
		db.metrics.flushCount.Add(1)
		db.metrics.flushBytesWritten.Add(outBytes)
	} else {
		var inBytes int64
		for _, t := range inputs {
			inBytes += t.size
		}
		db.metrics.compactionCount.Add(1)
		db.metrics.compactionBytesRead.Add(inBytes)
		db.metrics.compactionBytesWritten.Add(outBytes)
	}
	if db.man == nil { // recovery builds a fresh baseline after replay
		install()
		return nil
	}
	edit := manifestEdit{
		HasLastSeq: true,
		LastSeq:    db.readSeq.Load(),
	}
	for _, t := range outputs {
		edit.Added = append(edit.Added, manifestTableInfo{
			Shard:      shard,
			Num:        t.num,
			Depth:      t.depth,
			Size:       t.size,
			MinKey:     t.minKey,
			MaxKey:     t.maxKey,
			Entries:    t.entries,
			Tombstones: t.tombstones,
		})
	}
	for _, t := range inputs {
		edit.Deleted = append(edit.Deleted, manifestTableRef{Shard: shard, Num: t.num})
	}
	db.manMu.Lock()
	defer db.manMu.Unlock()
	if err := db.man.append(&edit); err != nil {
		db.setBackgroundError(err)
		return err
	}
	install()
	db.maybeRotateManifest()
	return nil
}
