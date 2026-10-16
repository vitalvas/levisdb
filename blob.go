package levisdb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

// Value separation stores a value whose length is at least Options.ValueThreshold
// in a separate append-only blob file instead of inline in an SSTable. The table
// then holds only a small blob pointer (kind ikeyKindBlobRef), so a compaction
// relocates the pointer rather than rewriting the value bytes -- the write
// amplification of large values drops from one rewrite per tier to a single write.
//
// A blob file is a flat sequence of records; a pointer addresses a record by file
// number, byte offset, and length. The file carries no index or trailer: the table
// pointer is the only thing that locates a value, and the manifest tracks which
// blob files are live. The blob file is the unit of garbage collection (see the
// manifest blob records and cleanupBlobs): it is deleted once no live table points
// into it.

// blobPointer locates an out-of-line value. expiresAt carries the TTL deadline so a
// read reports it (and resolves TTL) without fetching the blob: zero means a plain
// value whose bytes are stored verbatim; a positive value means the blob holds the
// raw user value and the deadline is this timestamp. A pointer never stores the
// TTL-prefix form, so TTL handling stays out of the blob layer.
type blobPointer struct {
	num       uint32
	offset    uint64
	length    uint32
	expiresAt int64
}

// blobPointerLen is the fixed encoded size of a blob pointer as stored in a table
// value: num(4) + offset(8) + length(4) + expiresAt(8), little-endian.
const blobPointerLen = 4 + 8 + 4 + 8

// encodeBlobPointer appends p's fixed-width encoding to dst.
func encodeBlobPointer(dst []byte, p blobPointer) []byte {
	var buf [blobPointerLen]byte
	binary.LittleEndian.PutUint32(buf[0:], p.num)
	binary.LittleEndian.PutUint64(buf[4:], p.offset)
	binary.LittleEndian.PutUint32(buf[12:], p.length)
	binary.LittleEndian.PutUint64(buf[16:], uint64(p.expiresAt))
	return append(dst, buf[:]...)
}

// decodeBlobPointer parses a table value previously written by encodeBlobPointer.
func decodeBlobPointer(v []byte) (blobPointer, error) {
	if len(v) != blobPointerLen {
		return blobPointer{}, fmt.Errorf("blob: pointer length %d, want %d", len(v), blobPointerLen)
	}
	return blobPointer{
		num:       binary.LittleEndian.Uint32(v[0:]),
		offset:    binary.LittleEndian.Uint64(v[4:]),
		length:    binary.LittleEndian.Uint32(v[12:]),
		expiresAt: int64(binary.LittleEndian.Uint64(v[16:])),
	}, nil
}

// blobWriter appends values to blob files, rolling to a fresh file once one reaches
// rollSize. Each finished file is fsynced and its directory synced before the caller
// commits any table that references it, so recovery never sees a pointer to a value
// that is not durable. Not safe for concurrent use; the flush path owns one.
type blobWriter struct {
	alloc    fileAllocator
	blobPath func(uint32) (string, error)
	rollSize int64

	cur     *os.File
	curNum  uint32
	curPath string
	offset  int64 // next write offset within cur
	// finished accumulates files sealed during this writer's life, with their final
	// size, so the caller records them in the manifest.
	finished []blobFileMeta
}

// blobFileMeta is a sealed blob file the caller must record in the manifest.
type blobFileMeta struct {
	Num  uint32
	Size int64
}

func newBlobWriter(alloc fileAllocator, blobPath func(uint32) (string, error), rollSize int64) *blobWriter {
	if rollSize <= 0 {
		rollSize = DefaultBlobFileSize
	}
	return &blobWriter{alloc: alloc, blobPath: blobPath, rollSize: rollSize}
}

// append writes value as a new record and returns a pointer to it. expiresAt is
// carried into the pointer (0 for a plain value); value is the raw value bytes
// WITHOUT any TTL prefix. The record is just the raw bytes: the pointer's length
// delimits it. A fresh file is opened lazily and rolled once it reaches rollSize.
func (w *blobWriter) append(value []byte, expiresAt int64) (blobPointer, error) {
	if w.cur == nil || w.offset >= w.rollSize {
		if err := w.roll(); err != nil {
			return blobPointer{}, err
		}
	}
	off := w.offset
	if _, err := w.cur.Write(value); err != nil {
		return blobPointer{}, err
	}
	w.offset += int64(len(value))
	return blobPointer{num: w.curNum, offset: uint64(off), length: uint32(len(value)), expiresAt: expiresAt}, nil
}

// roll seals the current file (if any) and opens a fresh one.
func (w *blobWriter) roll() error {
	if err := w.sealCurrent(); err != nil {
		return err
	}
	num := w.alloc.Next()
	if num == 0 {
		return ErrFileNumberExhausted
	}
	path, err := w.blobPath(num)
	if err != nil {
		return err
	}
	// O_EXCL so a crashed-but-synced blob whose number was recovered is never
	// silently overwritten, matching the table writer's create guard.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	w.cur, w.curNum, w.curPath, w.offset = f, num, path, 0
	return nil
}

// sealCurrent fsyncs, closes, and dir-syncs the current file, then records it. A
// sealed file is durable and safe to reference from a committed table.
func (w *blobWriter) sealCurrent() error {
	if w.cur == nil {
		return nil
	}
	size := w.offset
	f, path, num := w.cur, w.curPath, w.curNum
	w.cur, w.curPath, w.offset = nil, "", 0
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return err
	}
	w.finished = append(w.finished, blobFileMeta{Num: num, Size: size})
	return nil
}

// close seals the final file and returns every blob file this writer produced, for
// the caller to record in the manifest. After close the writer must not be reused.
func (w *blobWriter) close() ([]blobFileMeta, error) {
	if err := w.sealCurrent(); err != nil {
		return nil, err
	}
	return w.finished, nil
}

// abandon closes and removes any partially written current file, discarding the
// writer after an error so no orphan is left for this failed flush. Already-sealed
// files are left for orphan cleanup (they are durable but unreferenced).
func (w *blobWriter) abandon() {
	if w.cur == nil {
		return
	}
	path := w.curPath
	_ = w.cur.Close()
	w.cur, w.curPath, w.offset = nil, "", 0
	_ = removeFileDurable(path)
}

// blobReader reads values back from a blob file via the shared fd pool, so blob
// descriptors obey the same open-file bound as tables.
type blobReader struct {
	handle *openFile
}

func newBlobReader(handle *openFile) *blobReader {
	return &blobReader{handle: handle}
}

// read returns the raw value bytes a pointer addresses. The returned slice is a
// fresh copy owned by the caller.
func (r *blobReader) read(p blobPointer) ([]byte, error) {
	buf := make([]byte, p.length)
	n, err := r.handle.ReadAt(buf, int64(p.offset))
	if err != nil {
		return nil, fmt.Errorf("blob %d: read at %d: %w", p.num, p.offset, err)
	}
	if n != int(p.length) {
		return nil, fmt.Errorf("blob %d: short read %d of %d at %d", p.num, n, p.length, p.offset)
	}
	return buf, nil
}
