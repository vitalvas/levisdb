// Snapshot.WriteTo / WriteToDir bridge the snapshot, engine iterator, and SST
// writer to produce a base image for replication bootstrap. WALObserver and
// GetUpdatesSince tail only new writes; these ship the existing data a follower
// needs before it starts tailing, preserving the exact TTL deadline the tail also
// delivers so a re-bootstrapped follower does not diverge.

package levisdb

import (
	"fmt"
	"path/filepath"
	"time"
)

// openImageScan pins the snapshot's read view (both the sequence and the
// wall-clock TTL view) and returns an engine iterator over the whole keyspace at
// that view, plus a release func the caller must defer. It mirrors the ritual
// Snapshot.NewRangeIterator + dbIterator perform; because WriteTo bypasses the
// public iterator to read ValueExpiresAt, it must acquire and release those pins
// itself. Returns ErrClosed if the database is closed.
func (s *Snapshot) openImageScan() (*engineIterator, func(), error) {
	readTime := time.Now().UnixNano()
	s.db.mu.RLock()
	if s.db.closed {
		s.db.mu.RUnlock()
		return nil, nil, ErrClosed
	}
	s.db.snaps.acquire(s.seq)
	s.db.snaps.acquireIteratorTime(readTime)
	src := s.db.eng.newRangeIteratorAt(s.seq, nil, nil, readTime)
	s.db.mu.RUnlock()
	release := func() {
		_ = src.Close()
		s.db.snaps.releaseIteratorTime(readTime)
		s.db.snaps.release(s.seq)
	}
	return src, release, nil
}

// writeEntry writes the iterator's current entry into w, preserving an exact TTL
// deadline (via PutWithExpiry) for a TTL value and a plain Put otherwise.
func writeEntry(w *SstFileWriter, src *engineIterator) error {
	if exp := src.valueExpiresAt(); exp != 0 {
		return w.PutWithExpiry(src.Key(), src.Value(), exp)
	}
	return w.Put(src.Key(), src.Value())
}

// WriteTo writes the snapshot's live data as a single sorted table file at path,
// suitable for IngestExternalFile on a follower, and returns the sequence to
// resume the tail from (the snapshot's Seq). After ingesting the file the
// follower calls GetUpdatesSince with the returned value to replay everything
// committed after the snapshot; the overlap is safe because delivery is
// at-least-once and idempotent.
//
// TTL entries keep their exact stored deadline, unlike SstFileWriter.PutTTL
// which re-derives one from the wall clock. Point tombstones are not written:
// the image carries only live values, which is all a fresh follower needs
// (deletes arrive through the tail). The follower must be fresh or quiesced, as
// IngestExternalFile rejects a file overlapping keys live in its memtable.
//
// An empty snapshot writes no file (any pre-existing file at path is removed) and
// returns (Seq, nil). WriteTo puts the whole snapshot in one file; for a large
// database use WriteToDir, which rolls bounded files a follower can ingest and
// compact as normal-sized tables.
func (s *Snapshot) WriteTo(path string, opts SstWriterOptions) (uint64, error) {
	w, err := NewSstFileWriter(path, opts)
	if err != nil {
		return 0, err
	}
	finished := false
	defer func() {
		if !finished {
			w.discard()
		}
	}()

	src, release, err := s.openImageScan()
	if err != nil {
		return 0, err
	}
	defer release()

	n := 0
	for src.Next() {
		if err := writeEntry(w, src); err != nil {
			return 0, err
		}
		n++
	}
	if err := src.Error(); err != nil {
		return 0, err
	}
	if n == 0 {
		return s.seq, nil // empty snapshot: defer discards the empty file
	}
	if err := w.Finish(); err != nil {
		return 0, err
	}
	finished = true
	return s.seq, nil
}

// WriteToDir writes the snapshot's live data as a set of bounded sorted table
// files in dir, so a large (multi-terabyte) base image is transferred and
// ingested in pieces rather than as one giant file. It rolls to a new file once
// the current one reaches maxFileBytes of compressed on-disk data (a zero or
// negative value uses the engine's FileSizeMax), always at a key boundary so a
// value is never split. Files are named 000001.sst, 000002.sst, ... in ascending
// key order.
//
// It returns the ordered chunk paths and the sequence to resume the tail from
// (the snapshot's Seq). The follower ingests each path in turn with
// IngestExternalFile (the chunks hold disjoint, ascending key ranges, so each
// installs cleanly and none shadows another) and then calls GetUpdatesSince with
// the returned sequence. Exact TTL deadlines and the fresh-follower requirement
// are as for WriteTo. An empty snapshot writes no files and returns (nil, Seq).
//
// One snapshot pins the read view for the whole scan, so every chunk is at the
// same sequence and the image is internally consistent; on a large database that
// scan is long and holds versions on the source until it finishes and the
// snapshot is released, so the source's disk usage grows meanwhile.
func (s *Snapshot) WriteToDir(dir string, opts SstWriterOptions, maxFileBytes int64) ([]string, uint64, error) {
	if maxFileBytes <= 0 {
		maxFileBytes = DefaultFileSizeMax
	}
	src, release, err := s.openImageScan()
	if err != nil {
		return nil, 0, err
	}
	defer release()

	var paths []string
	var w *SstFileWriter
	// Discard an unfinished current chunk on any early return; finished chunks are
	// already closed and stay.
	defer func() {
		if w != nil {
			w.discard()
		}
	}()

	rollChunk := func() error {
		path := filepath.Join(dir, fmt.Sprintf("%06d.sst", len(paths)+1))
		nw, nerr := NewSstFileWriter(path, opts)
		if nerr != nil {
			return nerr
		}
		w = nw
		paths = append(paths, path)
		return nil
	}
	closeChunk := func() error {
		if w == nil {
			return nil
		}
		cur := w
		w = nil
		return cur.Finish()
	}

	for src.Next() {
		if w == nil {
			if err := rollChunk(); err != nil {
				return nil, 0, err
			}
		}
		if err := writeEntry(w, src); err != nil {
			return nil, 0, err
		}
		if w.bytesWritten() >= maxFileBytes {
			if err := closeChunk(); err != nil {
				return nil, 0, err
			}
		}
	}
	if err := src.Error(); err != nil {
		return nil, 0, err
	}
	if err := closeChunk(); err != nil {
		return nil, 0, err
	}
	return paths, s.seq, nil
}
