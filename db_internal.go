package levisdb

import (
	"bytes"
	"fmt"
	"os"
	"time"
)

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

// recoverWALMode replays leftover WAL segments into the memtable and flushes
// them to tables. logs is the segment numbers, ascending; they carry globally
// ascending seqs, so a regressing sequence signals corruption. The caller must
// install a manifest baseline containing the flushed tables before removing the
// WAL segments.
//
// Re-applying an entry already present in a table is harmless because the
// internal key (user key + seq + kind) is identical, so no version is
// duplicated. A torn tail from the crash is ignored by the WAL reader, so
// replay stops at the last intact record.
func (db *DB) recoverWALMode(logs []uint32, startSeq uint64, flush bool) (uint64, error) {
	maxSeq := startSeq
	lenient := !db.opts.StrictWALRecovery
	s := db.eng
	var lastWALSeq uint64
	for _, num := range logs {
		if num < db.replayLogNum {
			continue // retained CDC history already covered by durable tables
		}
		path, perr := db.store.logPath(num)
		if perr != nil {
			return maxSeq, perr
		}
		f, err := os.Open(path)
		if err != nil {
			return maxSeq, err
		}
		seq, err := replayWALFileVisit(f, startSeq, lenient, false, func(entries []walEntry) error {
			for i := range entries {
				e := &entries[i]
				if e.Seq <= lastWALSeq {
					// A regressing sequence is a corruption signal. Lenient recovery
					// stops and keeps the prefix; strict fails.
					if lenient {
						return errWALStop
					}
					return fmt.Errorf("wal: non-increasing sequence %d after %d", e.Seq, lastWALSeq)
				}
				lastWALSeq = e.Seq
				// The empty-key and bounds checks below are SEMANTIC: a valid-CRC
				// record whose contents are logically impossible signals a format/logic
				// problem or tampering, not bit rot, so they always fail Open even in
				// lenient mode.
				if len(e.Key) == 0 {
					return fmt.Errorf("wal: empty key")
				}
				if e.Kind == walKindRangeDelete &&
					(len(e.Value) == 0 || bytes.Compare(e.Key, e.Value) >= 0) {
					return fmt.Errorf("wal: invalid range delete")
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
				case walKindRangeDelete:
					s.delRange(e.Seq, e.Key, e.Value)
				case walKindPutTTL:
					s.putTTL(e.Seq, e.Key, e.Value, e.ExpiresAt)
				default:
					s.Put(e.Seq, e.Key, e.Value)
				}
				// Bound skiplist arena growth once the memtable reaches its threshold.
				// Writable recovery flushes to an SSTable; read-only recovery seals the
				// memtable in memory so Open preserves its non-mutation contract.
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
			// Recoverable stop: keep the intact prefix, skip later segments.
			if closeErr != nil {
				return maxSeq, closeErr
			}
			db.log.Warn("wal recovery stopped at corruption; kept intact prefix",
				"op", "recover", "segment", num, "recovered_seq", maxSeq)
			break
		}
		if err != nil {
			return maxSeq, err
		}
		if closeErr != nil {
			return maxSeq, closeErr
		}
	}

	// Flush the recovered memtable to a table so the data is durable before we
	// delete the WAL segments that held it; otherwise a second crash before the
	// next flush would lose the just-recovered writes. The freshly written table
	// is captured by openManifest's baseline, which runs next, so no manifest edit
	// is recorded here (db.man does not exist yet).
	if maxSeq > db.readSeq.Load() {
		db.readSeq.Store(maxSeq)
	}
	db.logRecovered(startSeq, maxSeq)
	if !flush {
		return maxSeq, nil
	}
	if !s.memEmpty() {
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
	for _, t := range state.Tables {
		if t.Num > maxNum {
			maxNum = t.Num
		}
	}
	return maxNum
}

// restoreTables opens each table recorded in the manifest into the engine in
// deterministic ascending file-number order. Compaction outputs can contain
// older data despite newer file numbers, so point reads compare sequences.
func (db *DB) restoreTables(state *manifestState) error {
	ordered := make([]manifestTableInfo, len(state.Tables))
	copy(ordered, state.Tables)
	sortTablesByNum(ordered)
	for _, t := range ordered {
		path, err := db.store.tablePath(t.Num)
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
		if err := db.eng.openTable(tableSpec{
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
			Kind:  publicKind(e.Kind),
			Key:   e.Key,
			Value: e.Value,
		}
		if e.ExpiresAt != 0 {
			out[i].ExpiresAt = e.ExpiresAt
			out[i].TTL = time.Until(time.Unix(0, e.ExpiresAt))
		}
	}
	b.obs.Observe(out)
}

func publicKind(k walKindType) EntryKind {
	switch k {
	case walKindDelete:
		return EntryDelete
	case walKindRangeDelete:
		return EntryDeleteRange
	default:
		return EntryPut
	}
}

// commitTableChange durably records a table replacement and then installs it
// while holding the manifest serialization lock. Manifest rotation therefore
// snapshots the newly-installed in-memory state rather than a stale one.
func (db *DB) commitTableChange(inputs, outputs []*tableMeta, install func()) error {
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
		// A flush can include a partially applied batch before readSeq advances.
		// Preserve the allocated watermark so even lenient WAL recovery cannot
		// reuse a sequence already present in an SST.
		LastSeq: db.walSeq.Load(),
	}
	for _, t := range outputs {
		edit.Added = append(edit.Added, manifestTableInfo{
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
		edit.Deleted = append(edit.Deleted, manifestTableRef{Num: t.num})
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
