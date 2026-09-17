package levisdb

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// Observer receives committed entries after they are durable, in commit order.
// It is called synchronously by the committer; it must be fast.
type walObserver interface {
	observe(batch []walEntry)
}

// WAL is the shared write-ahead log. Concurrent Append calls are coalesced by
// a single committer goroutine into grouped journal writes and fsyncs, so the
// disk sees one sequential stream regardless of writer concurrency.
type walT struct {
	file  *os.File
	jw    *journalWriter
	obs   walObserver
	apply func([]walEntry) // installs a durable batch before its waiter is released
	sync  bool             // fsync each committed group; false relies on the OS page cache

	mu      sync.Mutex
	cond    *sync.Cond
	pending []*pendingBatch // batches awaiting commit
	writing bool            // a committer is currently draining
	recbuf  []byte          // reusable record buffer
	closed  bool
	err     error // terminal writer failure (for example sequence exhaustion)

	// written is the cumulative count of record bytes appended, a WAL-volume
	// health signal. It survives rotation because rotate keeps the same walT.
	written atomic.Int64
	// segBytes is the record bytes appended to the CURRENT segment; it resets on
	// rotate. maybeCheckpoint polls it after every write to decide rotation, so it
	// is an atomic read rather than a per-write file Stat syscall.
	segBytes atomic.Int64
}

type pendingBatch struct {
	entries []walEntry
	done    chan error
}

// walConfig carries the tunables for a new WAL.
type walConfig struct {
	// sync fsyncs each committed group when true; false relies on the OS page
	// cache.
	sync bool
}

// newWAL opens a WAL appending to file. When cfg.sync is true group commit
// fsyncs once per drained group (durable); when false the data is left to the OS
// page cache (faster, larger crash window). obs may be nil. Sequence numbers are
// assigned by the caller before append.
func newWAL(file *os.File, obs walObserver, cfg walConfig) *walT {
	w := &walT{
		file: file,
		jw:   newJournalWriter(file),
		obs:  obs,
		sync: cfg.sync,
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

// append durably logs a batch of entries and returns once they are fsync'd and
// the observer (if any) has been notified. Entries must already carry ascending
// Seqs. It is safe for concurrent use. Used by recovery replay and tests; the
// live write path uses enqueue + runCommit so it can order the enqueue under the
// DB's seqMu while committing outside it (see appendWAL).
func (w *walT) append(entries []walEntry) error {
	pb, mustCommit, err := w.enqueue(entries)
	if err != nil {
		return err
	}
	return w.runCommit(pb, mustCommit)
}

// enqueue adds a pre-sequenced batch to the WAL's pending queue and reports
// whether the caller must drive the commit (it is the first writer in) or another
// in-flight committer will drain it. Entries must already carry their Seq. The
// caller enqueues under the DB's seqMu so records are queued in ascending seq
// order (recovery treats a regressing seq as corruption). commit/fsync happens
// after enqueue returns and outside seqMu, so a slow commit does not serialize
// the next batch's enqueue. w.apply installs the batch into the memtable and the
// committer advances readSeq in commit order.
func (w *walT) enqueue(entries []walEntry) (pb *pendingBatch, mustCommit bool, err error) {
	if len(entries) == 0 {
		return nil, false, nil
	}
	pb = &pendingBatch{entries: entries, done: make(chan error, 1)}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, false, os.ErrClosed
	}
	if w.err != nil {
		return nil, false, w.err
	}
	w.pending = append(w.pending, pb)
	if w.writing {
		// Another goroutine is committing; it will drain our batch too.
		return pb, false, nil
	}
	w.writing = true
	return pb, true, nil
}

// runCommit drives the group commit for a batch this WAL made the caller
// responsible for (enqueue returned mustCommit), then waits for it to be durable
// and applied.
func (w *walT) runCommit(pb *pendingBatch, mustCommit bool) error {
	if pb == nil {
		return nil
	}
	if mustCommit {
		w.commit()
	}
	return <-pb.done
}

// commit drains all pending batches, writes them to the journal as one group,
// fsyncs once, then fires the observer and wakes waiters. It loops until no
// batches remain so late arrivals are not stranded. Entry sequence numbers are
// assigned by the caller (the DB's global sequence) before append; the WAL only
// persists them.
func (w *walT) commit() {
	for {
		w.mu.Lock()
		batch := w.pending
		w.pending = nil
		if len(batch) == 0 {
			w.writing = false
			w.cond.Broadcast()
			w.mu.Unlock()
			return
		}
		if w.err != nil {
			err := w.err
			w.mu.Unlock()
			for _, pending := range batch {
				pending.done <- err
			}
			continue
		}
		w.mu.Unlock()

		err := w.writeGroup(batch)
		if err != nil {
			// A short/partial journal write makes the remainder of this segment
			// unusable for recovery. Poison it so no later successful write can be
			// published beyond the corrupt tail. The whole group failed to persist,
			// so every batch in it fails.
			w.mu.Lock()
			if w.err == nil {
				w.err = err
			}
			w.mu.Unlock()
			for _, pb := range batch {
				pb.done <- err
			}
			continue
		}

		// Install every durable batch in WAL sequence order before invoking user
		// observers. An observer panic or stall must not leave later records that
		// are already durable absent from the memtables.
		//
		// A panic inside apply (the memtable install) is caught and turned into a
		// poisoning error, not left to unwind the committer goroutine: an unrecovered
		// panic here would exit commit() with w.writing still true, so rotate() and
		// Close() (which wait for w.writing==false) would deadlock forever. The data
		// is durable on disk but failed to become visible, so it is a genuine engine
		// failure: poison w.err and fail the whole group, mirroring a write failure.
		if applyErr := w.applyGroup(batch); applyErr != nil {
			w.mu.Lock()
			if w.err == nil {
				w.err = applyErr
			}
			w.mu.Unlock()
			for _, pb := range batch {
				pb.done <- applyErr
			}
			continue
		}
		// Observe per batch. Every batch is already durable and applied, so an
		// observer failure is scoped to the batch it panicked on: that one caller
		// learns its CDC/replication hook failed, while the sibling batches (whose
		// data is equally committed) still report success. An observer panic does
		// not corrupt the WAL, so it never poisons w.err.
		for _, pb := range batch {
			var observerErr error
			if w.obs != nil {
				observerErr = callWALObserver(w.obs, pb.entries)
			}
			pb.done <- observerErr
		}
	}
}

// applyGroup installs every batch into the memtable in seq order, recovering a
// panic into an error so a memtable-install failure cannot unwind the committer
// goroutine and leave w.writing set (which would deadlock rotate/Close).
func (w *walT) applyGroup(batch []*pendingBatch) (err error) {
	if w.apply == nil {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("wal: apply panic: %v", recovered)
		}
	}()
	for _, pb := range batch {
		w.apply(pb.entries)
	}
	return nil
}

func callWALObserver(obs walObserver, entries []walEntry) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("wal: observer panic: %v", recovered)
		}
	}()
	obs.observe(entries)
	return nil
}

// writeGroup encodes and writes every batch in the group, then flushes and
// fsyncs once for the whole group.
func (w *walT) writeGroup(batch []*pendingBatch) error {
	for _, pb := range batch {
		if len(pb.entries) == 0 {
			continue
		}
		w.recbuf = encodeBatch(w.recbuf[:0], pb.entries[0].Seq, pb.entries)
		if err := w.jw.Write(w.recbuf); err != nil {
			return err
		}
		w.written.Add(int64(len(w.recbuf)))
		w.segBytes.Add(int64(len(w.recbuf)))
	}
	if err := w.jw.Flush(); err != nil {
		return err
	}
	if !w.sync {
		// NoSync: skip the per-group fsync and leave durability to the OS page
		// cache. The crash-loss window is every write since the last fsync, which
		// the database bounds with the background WALSyncInterval sync.
		return nil
	}
	// Group commit fsyncs once per drained group: durable by default with a
	// crash-loss window bounded to the in-flight group.
	return w.file.Sync()
}

// Close flushes and closes the WAL file. In-flight appends complete first.
func (w *walT) Close() error {
	w.mu.Lock()
	for w.writing {
		w.cond.Wait()
	}
	w.closed = true
	terminalErr := w.err
	w.mu.Unlock()
	if terminalErr != nil {
		_ = w.file.Close()
		return terminalErr
	}

	if err := w.jw.Flush(); err != nil {
		w.file.Close()
		return err
	}
	// Under NoSync the WAL never promised an fsync; a clean Close leaves the
	// buffered bytes in the OS page cache (which survives process exit) instead of
	// forcing a per-segment fsync. Durable mode still fsyncs on Close.
	if w.sync {
		if err := w.file.Sync(); err != nil {
			w.file.Close()
			return err
		}
	}
	return w.file.Close()
}

// rotate makes the drained segment durable and captures its committed cutoff
// before switching files. Loading the watermark afterward could include writes
// that still need the new WAL for recovery. The caller closes the old file.
func (w *walT) rotate(file *os.File, committed *atomic.Uint64) (*os.File, uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.writing {
		w.cond.Wait()
	}
	if w.closed {
		return nil, 0, os.ErrClosed
	}
	if w.err != nil {
		return nil, 0, w.err
	}
	if err := w.jw.Flush(); err != nil {
		w.err = err
		return nil, 0, err
	}
	if err := w.file.Sync(); err != nil {
		w.err = err
		return nil, 0, err
	}
	cutoff := committed.Load()
	old := w.file
	w.file = file
	w.jw = newJournalWriter(file)
	w.segBytes.Store(0) // new segment starts empty
	return old, cutoff, nil
}

// currentSize returns the record bytes appended to the current segment, without
// a file Stat syscall, so the write path can poll it after every batch.
func (w *walT) currentSize() int64 { return w.segBytes.Load() }

// bytesWritten returns the cumulative record bytes appended since the WAL opened.
func (w *walT) bytesWritten() int64 { return w.written.Load() }

// syncNow flushes buffered records and fsyncs the current segment. It bounds the
// crash-loss window under NoSync, where writeGroup skips the per-group fsync.
//
// It never blocks on the group committer: if one is draining (which touches the
// journal writer without w.mu), it skips this call and lets the next one catch
// up. Blocking here could deadlock a background caller against a crash that
// leaves the committer flag set. skipped reports whether the fsync was skipped.
func (w *walT) syncNow() (skipped bool, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.err != nil {
		return false, w.err
	}
	if w.writing {
		return true, nil // a committer owns the journal writer; try again next tick
	}
	if err := w.jw.Flush(); err != nil {
		return false, err
	}
	return false, w.file.Sync()
}

// Replay reads all committed entries from a WAL segment file and returns them
// in order. A torn tail from a crash is ignored. seq is the highest sequence
// number seen, or startSeq if the log was empty.
func replayWALFile(file *os.File, startSeq uint64) (entries []walEntry, seq uint64, err error) {
	seq, err = replayWALFileVisit(file, startSeq, false, false, func(batch []walEntry) error {
		entries = append(entries, batch...)
		return nil
	})
	return entries, seq, err
}

// replayWALFileVisit decodes one record at a time so database recovery does not
// retain an entire long-lived WAL segment in memory.
//
// When lenient is true, a physical corruption in the log framing (bad CRC,
// unframeable chunk, or an undecodable batch record) is treated like a torn
// tail: replay stops at the last intact record and returns nil, recovering the
// prefix instead of failing the whole open. Semantic errors raised by visit
// (e.g. an empty key or a regressing sequence) still propagate regardless, since
// they signal a logic/format problem rather than bit rot.
// strictTail rejects incomplete records in sealed replication segments. Crash
// recovery leaves it false because a torn final record is expected after a crash.
func replayWALFileVisit(file *os.File, startSeq uint64, lenient, strictTail bool, visit func([]walEntry) error) (seq uint64, err error) {
	r := newJournalReader(file)
	r.strictTail = strictTail
	seq = startSeq
	var lastRecordSeq uint64
	for {
		rec, rerr := r.Next()
		if rerr != nil {
			if rerr == io.EOF {
				return seq, nil
			}
			// A lenient stop returns errWALStop (not nil) so the caller can tell it
			// apart from clean EOF and halt ALL further replay: everything after a
			// corruption point is untrusted, including later segments, so recovery
			// must keep only the intact prefix up to here.
			if lenient && rerr == errJournalCorrupt {
				return seq, errWALStop
			}
			return seq, rerr
		}
		batch, derr := decodeBatch(rec)
		if derr != nil {
			if lenient {
				return seq, errWALStop // undecodable record: stop, keep the prefix
			}
			return seq, derr
		}
		if len(batch) == 0 {
			if lenient {
				return seq, errWALStop
			}
			return seq, fmt.Errorf("wal: empty batch record")
		}
		if batch[0].Seq <= lastRecordSeq {
			if lenient {
				return seq, errWALStop // inconsistent sequence: stop, keep the prefix
			}
			return seq, fmt.Errorf("wal: non-increasing sequence %d after %d", batch[0].Seq, lastRecordSeq)
		}
		lastRecordSeq = batch[len(batch)-1].Seq
		for _, e := range batch {
			if e.Seq > seq {
				seq = e.Seq
			}
		}
		if err := visit(batch); err != nil {
			return seq, err
		}
	}
}
