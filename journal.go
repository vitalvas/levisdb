// The journal implements the shared write-ahead log framing: a sequence of opaque
// records framed for crash recovery, following LevelDB's log block/chunk format.
//
// Records are split into chunks that fit within fixed-size blocks. Each chunk
// carries a CRC, length, and a type (full, first, middle, last) so a reader can
// reassemble records and detect a torn tail after a crash.

package levisdb

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

const (
	blockSize  = 32 * 1024
	headerSize = 7 // crc(4) + length(2) + type(1)
)

// chunk types
const (
	chunkFull = 1 + iota
	chunkFirst
	chunkMiddle
	chunkLast
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt indicates a record could not be read intact.
var errJournalCorrupt = errors.New("journal: corrupt record")

// Writer appends records to an underlying writer, framing them into blocks.
type journalWriter struct {
	w           io.Writer
	block       [blockSize]byte
	blockOffset int
	// flushedOffset is the prefix of block already written to w. Flush may make
	// a partially-filled logical block durable, but the next record must continue
	// in that same block so reader and writer retain identical 32 KiB boundaries.
	flushedOffset int
}

// NewWriter returns a Writer over w.
func newJournalWriter(w io.Writer) *journalWriter {
	return &journalWriter{w: w}
}

// Write frames and writes a single record. It buffers within the current block
// and flushes full blocks to the underlying writer.
func (w *journalWriter) Write(record []byte) error {
	first := true
	for first || len(record) > 0 {
		avail := blockSize - w.blockOffset
		if avail < headerSize {
			// Not enough room for a header; pad the block and roll over.
			for i := w.blockOffset; i < blockSize; i++ {
				w.block[i] = 0
			}
			if err := w.flushBlock(); err != nil {
				return err
			}
			avail = blockSize
		}

		chunkLen := len(record)
		fragLen := avail - headerSize
		if chunkLen > fragLen {
			chunkLen = fragLen
		}
		last := chunkLen == len(record)

		var typ byte
		switch {
		case first && last:
			typ = chunkFull
		case first:
			typ = chunkFirst
		case last:
			typ = chunkLast
		default:
			typ = chunkMiddle
		}

		w.emitChunk(typ, record[:chunkLen])
		record = record[chunkLen:]
		first = false
	}
	return nil
}

// emitChunk writes one framed chunk into the current block buffer.
func (w *journalWriter) emitChunk(typ byte, data []byte) {
	off := w.blockOffset
	binary.LittleEndian.PutUint16(w.block[off+4:], uint16(len(data)))
	w.block[off+6] = typ
	copy(w.block[off+headerSize:], data)
	crc := crc32.Update(0, castagnoli, w.block[off+6:off+headerSize])
	crc = crc32.Update(crc, castagnoli, data)
	binary.LittleEndian.PutUint32(w.block[off:], crc)
	w.blockOffset += headerSize + len(data)
}

// Flush writes any buffered partial block to the underlying writer. Callers
// fsync the underlying file separately.
func (w *journalWriter) Flush() error {
	if w.flushedOffset == w.blockOffset {
		return nil
	}
	if err := writeAll(w.w, w.block[w.flushedOffset:w.blockOffset]); err != nil {
		return err
	}
	w.flushedOffset = w.blockOffset
	if w.blockOffset == blockSize {
		w.blockOffset = 0
		w.flushedOffset = 0
	}
	return nil
}

func (w *journalWriter) flushBlock() error {
	err := writeAll(w.w, w.block[w.flushedOffset:blockSize])
	w.blockOffset = 0
	w.flushedOffset = 0
	return err
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(p) {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}

// Reader reassembles records from a framed stream.
type journalReader struct {
	r           io.Reader
	block       []byte
	blockLen    int
	blockOffset int
	eof         bool
	strictTail  bool // sealed replication history must not have incomplete records
}

// NewReader returns a Reader over r.
func newJournalReader(r io.Reader) *journalReader {
	return &journalReader{r: r, block: make([]byte, blockSize)}
}

// Next returns the next full record, or io.EOF when the stream is exhausted. A
// torn tail (partial final write before a crash) is reported as io.EOF so
// replay stops cleanly at the last intact record.
func (r *journalReader) Next() ([]byte, error) {
	// inProgress tracks whether a multi-chunk record has begun; it is separate
	// from record's nil-ness because a chunkFirst may legitimately carry an empty
	// payload (when only a header fits before a block boundary), which would leave
	// record nil while a record is genuinely in progress.
	var record []byte
	inProgress := false
	for {
		typ, data, err := r.readChunk()
		if err != nil {
			if err == io.EOF && inProgress {
				if r.strictTail {
					return nil, errJournalCorrupt
				}
				// Record began but stream ended mid-way: torn tail.
				return nil, io.EOF
			}
			return nil, err
		}
		switch typ {
		case chunkFull:
			if inProgress {
				return nil, errJournalCorrupt
			}
			return append([]byte(nil), data...), nil
		case chunkFirst:
			if inProgress {
				return nil, errJournalCorrupt
			}
			inProgress = true
			record = append(record, data...)
		case chunkMiddle:
			if !inProgress {
				return nil, errJournalCorrupt
			}
			record = append(record, data...)
		case chunkLast:
			if !inProgress {
				return nil, errJournalCorrupt
			}
			return append(record, data...), nil
		default:
			return nil, errJournalCorrupt
		}
	}
}

// readChunk reads one framed chunk, refilling the block buffer as needed.
func (r *journalReader) readChunk() (byte, []byte, error) {
	if r.blockLen-r.blockOffset < headerSize {
		if r.strictTail && r.blockLen < blockSize && r.blockLen > r.blockOffset {
			return 0, nil, errJournalCorrupt
		}
		if err := r.fill(); err != nil {
			return 0, nil, err
		}
		// A short final fragment cannot hold a header and is a torn tail.
		if r.blockLen-r.blockOffset < headerSize {
			if r.strictTail {
				return 0, nil, errJournalCorrupt
			}
			return 0, nil, io.EOF
		}
	}
	off := r.blockOffset
	length := int(binary.LittleEndian.Uint16(r.block[off+4:]))
	typ := r.block[off+6]
	if typ == 0 && length == 0 {
		// The writer pads only when fewer than headerSize bytes remain, and
		// that tail is skipped by the refill check above. A complete all-zero
		// header is therefore never valid padding; accepting it would let
		// corruption in an early block silently hide every later record.
		return 0, nil, errJournalCorrupt
	}
	if off+headerSize+length > r.blockLen {
		if r.blockLen < blockSize && !r.strictTail {
			return 0, nil, io.EOF // incomplete final block: torn tail
		}
		return 0, nil, errJournalCorrupt
	}
	data := r.block[off+headerSize : off+headerSize+length]
	want := binary.LittleEndian.Uint32(r.block[off:])
	crc := crc32.Update(0, castagnoli, r.block[off+6:off+headerSize])
	crc = crc32.Update(crc, castagnoli, data)
	if crc != want {
		return 0, nil, errJournalCorrupt
	}
	r.blockOffset = off + headerSize + length
	return typ, data, nil
}

// fill reads the next block from the underlying reader.
func (r *journalReader) fill() error {
	if r.eof {
		return io.EOF
	}
	n, err := io.ReadFull(r.r, r.block)
	switch {
	case err == io.EOF:
		r.eof = true
		return io.EOF
	case err == io.ErrUnexpectedEOF:
		r.eof = true
		r.blockLen = n
		r.blockOffset = 0
		return nil
	case err != nil:
		return err
	}
	r.blockLen = n
	r.blockOffset = 0
	return nil
}
