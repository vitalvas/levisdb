// Verify is a scrub: it proactively reads every block of every on-disk table and
// forces the per-block CRC32C (and codec) check that reads already perform, so
// bit rot or a truncated/garbled file is surfaced before a query happens to hit
// the damaged block. It reports the corrupt tables and changes nothing on disk.

package levisdb

// CorruptTable identifies a table that failed integrity verification and the
// error that exposed it (a block CRC mismatch, a bad codec/format, or an I/O
// error reading the file).
type CorruptTable struct {
	Table uint32
	Err   error
}

// Verify reads and validates every block of every live table, proactively
// surfacing on-disk corruption (bit rot, a truncated or garbled file) that would
// otherwise only be caught the next time a query happened to read the damaged
// block. Every block already carries a CRC32C checked on read; Verify forces that
// check across the whole dataset by reading each block directly (bypassing the
// block cache). It reports the corrupt tables and changes nothing on disk; a
// caller with a replica or backup can then restore or re-replicate the affected
// tables. A nil, empty result means every table is intact.
//
// Verify does not read the memtable or WAL (those are checked on recovery) and
// does not block writes: each table is ref-held during its scan while flush and
// compaction continue. It is I/O-heavy on a large database; schedule it during
// quiet periods.
func (db *DB) Verify() ([]CorruptTable, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	faults := db.eng.verify()
	if len(faults) == 0 {
		return nil, nil
	}
	out := make([]CorruptTable, len(faults))
	for i, f := range faults {
		out[i] = CorruptTable{Table: f.num, Err: f.err}
		db.log.Error("table verification failed",
			"op", "verify", "table", f.num, "err", f.err)
	}
	return out, nil
}
