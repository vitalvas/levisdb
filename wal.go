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
	seq     uint64          // last assigned sequence number
	pending []*pendingBatch // batches awaiting commit
	writing bool            // a committer is currently draining
	recbuf  []byte          // reusable record buffer
	closed  bool
	err     error // terminal writer failure (for example sequence exhaustion)

	// written is the cumulative count of record bytes appended, a WAL-volume
	// health signal. It survives rotation because rotate keeps the same walT.
	written atomic.Int64
}

type pendingBatch struct {
	entries []walEntry
	done    chan error
}

// New opens a WAL appending to file. When sync is true group commit fsyncs once
// per drained group (durable); when false the data is left to the OS page cache
// (faster, larger crash window). obs may be nil. startSeq seeds the sequence
// counter (from replay).
func newWAL(file *os.File, obs walObserver, startSeq uint64, syncEach bool) *walT {
	w := &walT{
		file: file,
		jw:   newJournalWriter(file),
		obs:  obs,
		sync: syncEach,
		seq:  startSeq,
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

// Append durably logs a batch of entries and returns once they are fsync'd and
// the observer (if any) has been notified. Entry Seq and the batch order are
// assigned here. It is safe for concurrent use.
func (w *walT) append(entries []walEntry) error {
	if len(entries) == 0 {
		return nil
	}
	pb := &pendingBatch{entries: entries, done: make(chan error, 1)}

	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return os.ErrClosed
	}
	if w.err != nil {
		err := w.err
		w.mu.Unlock()
		return err
	}
	w.pending = append(w.pending, pb)
	if w.writing {
		// Another goroutine is committing; it will drain our batch too.
		w.mu.Unlock()
		return <-pb.done
	}
	w.writing = true
	w.mu.Unlock()

	w.commit()
	return <-pb.done
}

// commit drains all pending batches: assigns sequence numbers, writes them to
// the journal as one group, fsyncs once, then fires the observer and wakes
// waiters. It loops until no batches remain so late arrivals are not stranded.
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
		// Assign sequence numbers under the lock so order is stable.
		var total uint64
		for _, pb := range batch {
			total += uint64(len(pb.entries))
		}
		if w.seq > maxIKeySeq || total > maxIKeySeq-w.seq {
			err := fmt.Errorf("wal: sequence number exhausted")
			w.err = err
			w.mu.Unlock()
			for _, pending := range batch {
				pending.done <- err
			}
			continue
		}
		for _, pb := range batch {
			base := w.seq + 1
			for i := range pb.entries {
				pb.entries[i].Seq = base + uint64(i)
			}
			w.seq = base + uint64(len(pb.entries)) - 1
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
		for _, pb := range batch {
			if w.apply != nil {
				w.apply(pb.entries)
			}
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
	}
	if err := w.jw.Flush(); err != nil {
		return err
	}
	if !w.sync {
		// NoSync: skip the per-group fsync and leave durability to the OS page
		// cache. The crash-loss window is every write since the last fsync, which
		// the database bounds with a periodic background Sync (WALSyncInterval),
		// not just the in-flight group.
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
	if err := w.file.Sync(); err != nil {
		w.file.Close()
		return err
	}
	return w.file.Close()
}

// rotate atomically switches future appends to file after making the old
// segment durable. It waits for the active group committer so no record is split
// across segments.
func (w *walT) rotate(file *os.File) (*os.File, uint64, error) {
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
	old := w.file
	w.file = file
	w.jw = newJournalWriter(file)
	return old, w.seq, nil
}

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

func (w *walT) size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	info, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// Replay reads all committed entries from a WAL segment file and returns them
// in order. A torn tail from a crash is ignored. seq is the highest sequence
// number seen, or startSeq if the log was empty.
func replayWALFile(file *os.File, startSeq uint64) (entries []walEntry, seq uint64, err error) {
	seq, err = replayWALFileVisit(file, startSeq, false, func(batch []walEntry) error {
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
// (e.g. a record naming the wrong shard) still propagate regardless, since they
// signal a logic/format problem rather than bit rot.
func replayWALFileVisit(file *os.File, startSeq uint64, lenient bool, visit func([]walEntry) error) (seq uint64, err error) {
	r := newJournalReader(file)
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
