// The manifest is the durable record of the live table set. It is an
// append-only log of edits framed with the journal format; replaying it
// reconstructs the live state on open. The first edit carries an identity
// marker so a truncated or foreign file is rejected on open.

package levisdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
)

// TableInfo describes one on-disk table in the manifest.
type manifestTableInfo struct {
	Num        uint32
	Depth      int
	Size       int64
	MinKey     []byte // user-key bounds; nil/empty when unknown (e.g. an empty table)
	MaxKey     []byte
	Entries    int // total entries; 0 means unknown (older manifests)
	Tombstones int // tombstone entries, for the density trigger
}

// Edit is one atomic change: an optional identity marker (on the first edit)
// plus tables added and removed, and an optional sequence allocation watermark.
type manifestEdit struct {
	HasIdentity bool

	HasLastSeq bool
	LastSeq    uint64 // allocated watermark; bounds every sequence in persisted tables
	// ReplayLogNum is the first WAL segment still needed for recovery. Earlier
	// segments may remain on disk for replication, but their tables are durable.
	// Zero leaves the previous boundary unchanged (legacy manifests omit it).
	ReplayLogNum uint32

	Added   []manifestTableInfo
	Deleted []manifestTableRef // num identifies a table to remove

	// AddedBlobs and DeletedBlobs track value-separation blob files. They are only
	// ever non-empty when value separation is enabled, so a database that never uses
	// the feature writes no blob records.
	AddedBlobs   []manifestBlobInfo
	DeletedBlobs []manifestBlobRef
}

// TableRef identifies a table for deletion.
type manifestTableRef struct {
	Num uint32
}

// manifestBlobInfo describes one live value-separation blob file. Size is the
// file's byte length; the manifest tracks which blob files exist so orphan cleanup
// and GC can tell a referenced blob from a leftover one.
type manifestBlobInfo struct {
	Num  uint32
	Size int64
}

// manifestBlobRef identifies a blob file for deletion.
type manifestBlobRef struct {
	Num uint32
}

// record tags for the edit encoding.
const (
	tagIdentity  = 1
	tagAdd       = 2
	tagDelete    = 3
	tagLastSeq   = 4
	tagReplayLog = 5
	tagAddBlob   = 6
	tagDelBlob   = 7
	tagEnd       = 0
)

// manifestVersion is the current on-disk manifest format version, written once in
// the identity edit. Readers reject a newer version. There are no releases, so
// version 1 is the baseline; a decoder that finds no version tag treats the file as
// pre-versioned and rejects it rather than guessing.
const manifestVersion = 1

// encode serializes an edit into a self-describing record.
func (e *manifestEdit) encode() []byte {
	var b []byte
	if e.HasIdentity {
		b = append(b, tagIdentity)
		b = binary.AppendUvarint(b, manifestVersion)
	}
	if e.HasLastSeq {
		b = append(b, tagLastSeq)
		b = binary.AppendUvarint(b, e.LastSeq)
	}
	if e.ReplayLogNum != 0 {
		b = append(b, tagReplayLog)
		b = binary.AppendUvarint(b, uint64(e.ReplayLogNum))
	}
	for _, t := range e.Added {
		b = append(b, tagAdd)
		b = binary.AppendUvarint(b, uint64(t.Num))
		b = binary.AppendUvarint(b, uint64(t.Depth))
		b = binary.AppendUvarint(b, uint64(t.Size))
		b = appendKeyBound(b, t.MinKey)
		b = appendKeyBound(b, t.MaxKey)
		b = binary.AppendUvarint(b, uint64(t.Entries))
		b = binary.AppendUvarint(b, uint64(t.Tombstones))
	}
	for _, d := range e.Deleted {
		b = append(b, tagDelete)
		b = binary.AppendUvarint(b, uint64(d.Num))
	}
	for _, bl := range e.AddedBlobs {
		b = append(b, tagAddBlob)
		b = binary.AppendUvarint(b, uint64(bl.Num))
		b = binary.AppendUvarint(b, uint64(bl.Size))
	}
	for _, bl := range e.DeletedBlobs {
		b = append(b, tagDelBlob)
		b = binary.AppendUvarint(b, uint64(bl.Num))
	}
	b = append(b, tagEnd)
	return b
}

func decodeEdit(rec []byte) (manifestEdit, error) {
	var e manifestEdit
	for len(rec) > 0 {
		tag := rec[0]
		rec = rec[1:]
		switch tag {
		case tagEnd:
			if len(rec) != 0 {
				return e, fmt.Errorf("manifest: trailing bytes after end tag")
			}
			return e, nil
		case tagIdentity:
			if e.HasIdentity {
				return e, fmt.Errorf("manifest: duplicate identity tag")
			}
			ver, n := binary.Uvarint(rec)
			if n <= 0 {
				return e, fmt.Errorf("manifest: bad version")
			}
			if ver != manifestVersion {
				return e, fmt.Errorf("manifest: unsupported version %d (want %d)", ver, manifestVersion)
			}
			e.HasIdentity = true
			rec = rec[n:]
		case tagLastSeq:
			if e.HasLastSeq {
				return e, fmt.Errorf("manifest: duplicate last-sequence tag")
			}
			seq, n := binary.Uvarint(rec)
			if n <= 0 {
				return e, fmt.Errorf("manifest: bad last seq")
			}
			if seq > maxIKeySeq {
				return e, fmt.Errorf("manifest: last sequence out of range")
			}
			e.HasLastSeq = true
			e.LastSeq = seq
			rec = rec[n:]
		case tagReplayLog:
			num, n := binary.Uvarint(rec)
			if n <= 0 || num == 0 || num > math.MaxUint32 || e.ReplayLogNum != 0 {
				return e, fmt.Errorf("manifest: bad recovery log boundary")
			}
			e.ReplayLogNum = uint32(num)
			rec = rec[n:]
		case tagAdd:
			t, r, err := readTable(rec)
			if err != nil {
				return e, err
			}
			e.Added = append(e.Added, t)
			rec = r
		case tagDelete:
			num, n := binary.Uvarint(rec)
			if n <= 0 {
				return e, fmt.Errorf("manifest: bad delete")
			}
			if num > math.MaxUint32 {
				return e, fmt.Errorf("manifest: delete out of range")
			}
			e.Deleted = append(e.Deleted, manifestTableRef{Num: uint32(num)})
			rec = rec[n:]
		case tagAddBlob, tagDelBlob:
			r, err := readBlobTag(&e, tag, rec)
			if err != nil {
				return e, err
			}
			rec = r
		default:
			return e, fmt.Errorf("manifest: unknown tag %d", tag)
		}
	}
	return e, fmt.Errorf("manifest: missing end tag")
}

// readBlobTag decodes a blob add (num+size) or blob delete (num) record into e and
// returns the remaining bytes. Split out of decodeEdit to keep its complexity bound.
func readBlobTag(e *manifestEdit, tag byte, rec []byte) ([]byte, error) {
	num, n := binary.Uvarint(rec)
	if n <= 0 || num == 0 || num > math.MaxUint32 {
		return nil, fmt.Errorf("manifest: bad blob record")
	}
	rec = rec[n:]
	if tag == tagDelBlob {
		e.DeletedBlobs = append(e.DeletedBlobs, manifestBlobRef{Num: uint32(num)})
		return rec, nil
	}
	size, n := binary.Uvarint(rec)
	if n <= 0 || size > math.MaxInt64 {
		return nil, fmt.Errorf("manifest: bad blob size")
	}
	rec = rec[n:]
	e.AddedBlobs = append(e.AddedBlobs, manifestBlobInfo{Num: uint32(num), Size: int64(size)})
	return rec, nil
}

func readTable(rec []byte) (manifestTableInfo, []byte, error) {
	// binary.Uvarint returns n<=0 on a truncated or overflowing value; each
	// field must be validated before advancing so a corrupt record errors
	// rather than slicing out of range.
	bad := func() (manifestTableInfo, []byte, error) {
		return manifestTableInfo{}, nil, fmt.Errorf("manifest: bad table record")
	}
	num, n := binary.Uvarint(rec)
	if n <= 0 {
		return bad()
	}
	rec = rec[n:]
	depth, n := binary.Uvarint(rec)
	if n <= 0 {
		return bad()
	}
	rec = rec[n:]
	size, n := binary.Uvarint(rec)
	if n <= 0 {
		return bad()
	}
	rec = rec[n:]
	if num > math.MaxUint32 || depth > uint64(^uint(0)>>1) || size > math.MaxInt64 {
		return bad()
	}
	minKey, rec, err := readKeyBound(rec)
	if err != nil {
		return bad()
	}
	maxKey, rec, err := readKeyBound(rec)
	if err != nil {
		return bad()
	}
	entries, n := binary.Uvarint(rec)
	if n <= 0 {
		return bad()
	}
	rec = rec[n:]
	tombstones, n := binary.Uvarint(rec)
	if n <= 0 {
		return bad()
	}
	rec = rec[n:]
	if entries > uint64(^uint(0)>>1) || tombstones > uint64(^uint(0)>>1) {
		return bad()
	}
	return manifestTableInfo{
		Num:        uint32(num),
		Depth:      int(depth),
		Size:       int64(size),
		MinKey:     minKey,
		MaxKey:     maxKey,
		Entries:    int(entries),
		Tombstones: int(tombstones),
	}, rec, nil
}

// appendKeyBound and readKeyBound carry a length-prefixed key bound. A nil
// slice encodes as length 0 and decodes back to nil, so unknown bounds round-
// trip. readKeyBound returns an owned copy since the record buffer is transient.
func appendKeyBound(b, v []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

func readKeyBound(rec []byte) ([]byte, []byte, error) {
	l, n := binary.Uvarint(rec)
	if n <= 0 || uint64(len(rec[n:])) < l {
		return nil, nil, fmt.Errorf("manifest: bad key bound")
	}
	rec = rec[n:]
	if l == 0 {
		return nil, rec, nil
	}
	return append([]byte(nil), rec[:l]...), rec[l:], nil
}

// Writer appends edits to a manifest file.
type manifestWriter struct {
	file      *os.File
	jw        *journalWriter
	syncFile  func() error // test hook; nil uses file.Sync
	closeFile func() error // test hook; nil uses file.Close
	edits     int          // edits appended since creation, for rotation decisions
	err       error        // terminal append failure; retrying past a partial tail is unsafe
}

// Create opens path for a new manifest and writes the identity edit.
func createManifest(path string) (*manifestWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	w := &manifestWriter{file: f, jw: newJournalWriter(f)}
	id := manifestEdit{HasIdentity: true}
	if err := w.append(&id); err != nil {
		_ = f.Close()
		_ = removeFileDurable(path)
		return nil, err
	}
	return w, nil
}

// Append durably writes an edit.
func (w *manifestWriter) append(e *manifestEdit) error {
	if w.err != nil {
		return w.err
	}
	if err := w.jw.Write(e.encode()); err != nil {
		w.err = err
		return err
	}
	if err := w.jw.Flush(); err != nil {
		w.err = err
		return err
	}
	syncFile := w.syncFile
	if syncFile == nil {
		syncFile = w.file.Sync
	}
	if err := syncFile(); err != nil {
		w.err = err
		return err
	}
	w.edits++
	return nil
}

// editCount returns the number of edits appended since this manifest was
// created, used to decide when to rotate to a fresh baseline.
func (w *manifestWriter) editCount() int {
	return w.edits
}

// Close closes the manifest file.
func (w *manifestWriter) Close() error {
	closeFile := w.closeFile
	if closeFile == nil {
		closeFile = w.file.Close
	}
	err := closeFile()
	if w.err != nil {
		return w.err
	}
	return err
}

// State is the reconstructed live view after replaying a manifest.
type manifestState struct {
	ReplayLogNum uint32              // first WAL segment required by this table set
	LastSeq      uint64              // highest sequence allocation watermark recorded
	Tables       []manifestTableInfo // the live table set
	Blobs        []manifestBlobInfo  // the live value-separation blob files
}

// Replay reads a manifest file and reconstructs the live table set. It applies
// adds and deletes in order; a torn tail is ignored.
func replayManifestFile(r io.Reader) (*manifestState, error) {
	st := &manifestState{}
	live := map[manifestTableRef]manifestTableInfo{}
	liveBlobs := map[manifestBlobRef]manifestBlobInfo{}
	// File numbers come from one global, never-reused allocator. Tracking every
	// table number seen (not just the live set) catches a corrupt manifest that
	// reuses a number, which would collide two tables on one block-cache identity.
	seenTableNums := map[uint32]bool{}
	seenBlobNums := map[uint32]bool{}

	jr := newJournalReader(r)
	haveIdentity := false
	for {
		rec, err := jr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		edit, err := decodeEdit(rec)
		if err != nil {
			return nil, err
		}
		if edit.HasIdentity {
			if haveIdentity {
				return nil, fmt.Errorf("manifest: repeated identity")
			}
			haveIdentity = true
		}
		if !haveIdentity {
			return nil, fmt.Errorf("manifest: missing identity")
		}
		if edit.HasLastSeq && edit.LastSeq > st.LastSeq {
			st.LastSeq = edit.LastSeq
		}
		if edit.ReplayLogNum != 0 {
			if edit.ReplayLogNum < st.ReplayLogNum {
				return nil, fmt.Errorf("manifest: regressing recovery log boundary")
			}
			st.ReplayLogNum = edit.ReplayLogNum
		}
		for _, t := range edit.Added {
			if t.Num == 0 {
				return nil, fmt.Errorf("manifest: table file number 0 is reserved")
			}
			if seenTableNums[t.Num] {
				return nil, fmt.Errorf("manifest: table file number %d reused", t.Num)
			}
			seenTableNums[t.Num] = true
			live[manifestTableRef{Num: t.Num}] = t
		}
		for _, d := range edit.Deleted {
			if _, exists := live[d]; !exists {
				return nil, fmt.Errorf("manifest: delete of unknown table %d", d.Num)
			}
			delete(live, d)
		}
		for _, bl := range edit.AddedBlobs {
			if seenBlobNums[bl.Num] {
				return nil, fmt.Errorf("manifest: blob file number %d reused", bl.Num)
			}
			seenBlobNums[bl.Num] = true
			liveBlobs[manifestBlobRef{Num: bl.Num}] = bl
		}
		for _, bd := range edit.DeletedBlobs {
			if _, exists := liveBlobs[bd]; !exists {
				return nil, fmt.Errorf("manifest: delete of unknown blob %d", bd.Num)
			}
			delete(liveBlobs, bd)
		}
	}

	if !haveIdentity {
		return nil, fmt.Errorf("manifest: missing identity")
	}
	for _, t := range live {
		st.Tables = append(st.Tables, t)
	}
	for _, bl := range liveBlobs {
		st.Blobs = append(st.Blobs, bl)
	}
	return st, nil
}
