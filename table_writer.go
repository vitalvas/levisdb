package levisdb

import (
	"encoding/binary"
	"fmt"
	"io"
)

// footer is a fixed-size trailer at the end of every table:
//
//	[index handle][filter handle][padding][magic: 8]
//
// Handles are uvarint-encoded and left-padded within the fixed region.
const (
	footerLen = 48
	magic     = 0xdb1e_5100_0000_0001
)

// Writer builds a single SSTable. Keys must be added in ascending internal-key
// order. It writes data blocks with the given codec, an index block, an
// embedded bloom filter, and a footer.
type tableWriter struct {
	w         io.Writer
	c         blockCodec
	bloom     *bloomFilter
	blockSize int

	offset     uint64
	data       dataBlockBuilder
	index      blockBuilder
	hashes     []uint32
	pendingBH  *blockHandle // handle of last flushed data block awaiting an index entry
	firstKey   []byte       // first internal key added, for the table's min-key bound
	lastKey    []byte
	entries    int // total entries added
	tombstones int // entries added that are tombstones (deletes)
	err        error
}

// entryCount and tombstoneCount report how many entries were written and how
// many of them were tombstones, so the compaction picker can trigger on
// tombstone density and reclaim deleted space early.
func (tw *tableWriter) entryCount() int     { return tw.entries }
func (tw *tableWriter) tombstoneCount() int { return tw.tombstones }

// minUserKey and maxUserKey return the smallest and largest USER keys written,
// or nil for an empty table. They are the table's key-range bounds, recorded in
// the manifest so reads can skip a table whose range excludes the lookup key.
func (tw *tableWriter) minUserKey() []byte {
	if tw.firstKey == nil {
		return nil
	}
	return ikeyUserKey(tw.firstKey)
}

func (tw *tableWriter) maxUserKey() []byte {
	if tw.lastKey == nil {
		return nil
	}
	return ikeyUserKey(tw.lastKey)
}

// NewWriter returns a table Writer over w using codec c, bloom bits per key,
// and a target uncompressed block size.
func newTableWriter(w io.Writer, c blockCodec, bloomBits, blockSize int) *tableWriter {
	return &tableWriter{
		w:         w,
		c:         c,
		bloom:     newBloom(bloomBits),
		blockSize: blockSize,
	}
}

// Add appends an entry. internalKey must be greater than every prior key.
func (tw *tableWriter) Add(internalKey, value []byte) error {
	if tw.err != nil {
		return tw.err
	}
	if len(internalKey) < trailerLen {
		tw.err = fmt.Errorf("table: short internal key")
		return tw.err
	}
	_, kind := ikeySeqKind(internalKey)
	if !validIKeyKind(kind) {
		tw.err = fmt.Errorf("table: unknown internal-key kind %d", kind)
		return tw.err
	}
	if tw.lastKey != nil && ikeyCompare(internalKey, tw.lastKey) <= 0 {
		tw.err = fmt.Errorf("table: keys added out of order")
		return tw.err
	}
	// If a previous block was flushed, emit its index entry now. The index key
	// is that block's full last internal key, so it remains a valid internal
	// key and index lookups can compare it with ikeyCompare.
	if tw.pendingBH != nil {
		tw.index.add(tw.lastKey, tw.pendingBH.encode(nil))
		tw.pendingBH = nil
	}

	tw.data.add(internalKey, value)
	tw.hashes = append(tw.hashes, bloomHash(ikeyUserKey(internalKey)))
	if tw.firstKey == nil {
		tw.firstKey = append([]byte(nil), internalKey...)
	}
	tw.lastKey = append(tw.lastKey[:0], internalKey...)
	tw.entries++
	if kind == ikeyKindDelete {
		tw.tombstones++
	}

	if tw.data.size() >= tw.blockSize {
		tw.flushDataBlock()
	}
	return tw.err
}

func (tw *tableWriter) flushDataBlock() {
	if tw.data.empty() || tw.err != nil {
		return
	}
	// finish() appends the restart trailer to data.buf; reset() clears it after.
	bh := tw.writeBlock(tw.data.finish(), tw.c)
	tw.data.reset()
	tw.pendingBH = &bh
}

// writeBlock finishes and writes a block, returning its handle.
func (tw *tableWriter) writeBlock(payload []byte, c blockCodec) blockHandle {
	if tw.err != nil {
		return blockHandle{}
	}
	block := finishBlock(payload, c)
	if err := writeAll(tw.w, block); err != nil {
		tw.err = err
	}
	bh := blockHandle{offset: tw.offset, length: uint64(len(block))}
	if tw.err == nil {
		tw.offset += uint64(len(block))
	}
	return bh
}

// Finish flushes the final data block, writes the index, filter, and footer,
// and returns the total bytes written.
func (tw *tableWriter) finish() (int64, error) {
	if tw.err != nil {
		return 0, tw.err
	}
	tw.flushDataBlock()
	if tw.pendingBH != nil {
		// Index the last data block with its own last key as separator.
		tw.index.add(tw.lastKey, tw.pendingBH.encode(nil))
		tw.pendingBH = nil
	}

	// Filter block: bloom is always stored uncompressed for direct access.
	filterBytes := tw.bloom.build(tw.hashes)
	filterBH := tw.writeRawBlock(filterBytes)

	// Index block, compressed with the same codec.
	indexBH := tw.writeBlock(tw.index.buf, tw.c)

	if tw.err != nil {
		return 0, tw.err
	}

	footer := tw.encodeFooter(indexBH, filterBH)
	if err := writeAll(tw.w, footer); err != nil {
		return 0, err
	}
	tw.offset += uint64(len(footer))
	return int64(tw.offset), nil
}

// writeRawBlock writes bytes with a CRC trailer but no compression, used for
// the filter block so the reader can address it without a codec.
func (tw *tableWriter) writeRawBlock(payload []byte) blockHandle {
	if tw.err != nil {
		return blockHandle{}
	}
	none, _ := codecFromID(codecNone)
	block := finishBlock(payload, none)
	if err := writeAll(tw.w, block); err != nil {
		tw.err = err
	}
	bh := blockHandle{offset: tw.offset, length: uint64(len(block))}
	if tw.err == nil {
		tw.offset += uint64(len(block))
	}
	return bh
}

func (tw *tableWriter) encodeFooter(index, filterBH blockHandle) []byte {
	buf := make([]byte, footerLen)
	p := index.encode(nil)
	p = filterBH.encode(p)
	copy(buf, p)
	binary.LittleEndian.PutUint64(buf[footerLen-8:], magic)
	return buf
}
