package levisdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Reader reads a single SSTable via random access. Data blocks are read on
// demand and, when a cache is provided, kept in memory across lookups.
//
// The index and filter blocks are parsed lazily and dropped when the file's
// descriptor is evicted from the fd pool, so a database with millions of live
// tables bounds its resident metadata by the open-file count instead of holding
// every table's index+bloom for its whole lifetime. The footer and metadata
// layout are still validated at open so a corrupt table fails fast.
type tableReader struct {
	r          io.ReaderAt
	size       int64
	indexBH    blockHandle // metadata block handles from the footer, for lazy loads
	filterBH   blockHandle
	rangeDelBH blockHandle // zero when the table has no range tombstones

	metaMu sync.Mutex // serializes metadata (re)building
	meta   atomic.Pointer[tableMetaBlocks]

	cache    *blockCacheT // nil when caching is disabled
	tableNum uint32       // identifies this table's blocks in the cache
}

// tableMetaBlocks is the parsed index + filter, built lazily and treated as
// immutable once published. A consumer loads the pointer once and uses it for
// the whole operation, so an eviction that clears tr.meta cannot invalidate an
// in-flight read (the local reference keeps it alive).
type tableMetaBlocks struct {
	filter   []byte
	indexBuf []byte // decoded index block; separators point into it
	indexEnt []indexEntry
	rangeDel []rangeTombstone // parsed range tombstones; nil when the table has none
}

// indexEntry locates a data block compactly: the separator (the block's last
// internal key) is stored as an offset/length into the retained indexBuf rather
// than a 24-byte slice header, and the block handle is inlined. This keeps the
// parsed index small, which matters when many tables are open at once.
type indexEntry struct {
	sepOff uint32 // separator offset into indexBuf
	sepLen uint32 // separator length
	offset uint64 // data block offset in the file
	length uint32 // data block length on disk
}

// NewCachedReader opens a table and caches its decoded data blocks under
// tableNum in the given cache. A nil cache disables caching.
func newCachedTableReader(r io.ReaderAt, size int64, c *blockCacheT, tableNum uint32) (*tableReader, error) {
	if size < footerLen {
		return nil, fmt.Errorf("table: file too small")
	}
	footer := make([]byte, footerLen)
	if _, err := r.ReadAt(footer, size-footerLen); err != nil {
		return nil, fmt.Errorf("table: read footer: %w", err)
	}
	if binary.LittleEndian.Uint64(footer[footerLen-8:]) != magic {
		return nil, fmt.Errorf("table: bad magic")
	}
	indexBH, n := decodeHandle(footer)
	if n == 0 {
		return nil, fmt.Errorf("table: bad index handle")
	}
	filterBH, n2 := decodeHandle(footer[n:])
	if n2 == 0 {
		return nil, fmt.Errorf("table: bad filter handle")
	}
	rangeDelBH, n3 := decodeHandle(footer[n+n2:])
	if n3 == 0 || n+n2+n3 > footerLen-8 {
		return nil, fmt.Errorf("table: bad range-del handle")
	}
	for _, b := range footer[n+n2+n3 : footerLen-8] {
		if b != 0 {
			return nil, fmt.Errorf("table: non-zero footer padding")
		}
	}
	dataEnd := uint64(size - footerLen)
	// Layout: [data...][range-del?][filter][index][footer]. The filter and index
	// are contiguous and end exactly at dataEnd. The range-del block, when present
	// (non-zero handle), sits immediately before the filter; when absent the filter
	// starts anywhere after the data blocks.
	if filterBH.length > dataEnd || filterBH.offset > dataEnd-filterBH.length ||
		indexBH.length > dataEnd || indexBH.offset > dataEnd-indexBH.length ||
		filterBH.offset+filterBH.length != indexBH.offset ||
		indexBH.offset+indexBH.length != dataEnd {
		return nil, fmt.Errorf("table: invalid metadata block layout")
	}
	if rangeDelBH.length != 0 || rangeDelBH.offset != 0 {
		if rangeDelBH.length > dataEnd || rangeDelBH.offset > dataEnd-rangeDelBH.length ||
			rangeDelBH.offset+rangeDelBH.length != filterBH.offset {
			return nil, fmt.Errorf("table: invalid range-del block layout")
		}
	}

	tr := &tableReader{
		r:          r,
		size:       size,
		indexBH:    indexBH,
		filterBH:   filterBH,
		rangeDelBH: rangeDelBH,
		cache:      c,
		tableNum:   tableNum,
	}

	// Parse the metadata once up front to validate the table (bad filter/index
	// bytes must fail Open), then keep it: the first eviction drops it and later
	// reads rebuild it on demand.
	if _, err := tr.ensureMeta(); err != nil {
		return nil, err
	}
	return tr, nil
}

// ensureMeta returns the parsed index+filter, building and publishing them on
// first use or after an eviction dropped them. The result is immutable; callers
// hold the returned pointer for the whole operation.
func (tr *tableReader) ensureMeta() (*tableMetaBlocks, error) {
	if m := tr.meta.Load(); m != nil {
		return m, nil
	}
	tr.metaMu.Lock()
	defer tr.metaMu.Unlock()
	if m := tr.meta.Load(); m != nil { // built while we waited for the lock
		return m, nil
	}
	m, err := tr.loadMeta()
	if err != nil {
		return nil, err
	}
	tr.meta.Store(m)
	return m, nil
}

// dropMeta releases the parsed index+filter so its memory can be reclaimed. A
// later read rebuilds it. Any in-flight read keeps its own reference alive.
func (tr *tableReader) dropMeta() { tr.meta.Store(nil) }

// rangeTombstones returns this table's parsed range tombstones (nil when it has
// none). The returned slice is owned by the reader's metadata and must not be
// mutated; its key bytes are immutable.
func (tr *tableReader) rangeTombstones() ([]rangeTombstone, error) {
	m, err := tr.ensureMeta()
	if err != nil {
		return nil, err
	}
	return m.rangeDel, nil
}

// verify reads and validates every block in the table: it parses the metadata
// (which reads and CRC-checks the filter, index, and range-del blocks) and then
// reads every data block, whose CRC and codec are checked by decodeBlock. It
// returns the first error found, or nil if the whole table is intact. It reads
// blocks directly (bypassing the block cache) so a cached-but-stale copy cannot
// mask on-disk corruption.
func (tr *tableReader) verify() error {
	m, err := tr.ensureMeta()
	if err != nil {
		return err
	}
	for i := range m.indexEnt {
		if _, err := tr.readBlockRaw(m.blockHandleAt(i)); err != nil {
			return fmt.Errorf("table: data block %d: %w", i, err)
		}
	}
	return nil
}

// loadMeta reads and parses the filter, index, and (when present) range-del
// blocks from disk.
func (tr *tableReader) loadMeta() (*tableMetaBlocks, error) {
	filterRaw, err := tr.readBlockRaw(tr.filterBH)
	if err != nil {
		return nil, fmt.Errorf("table: read filter: %w", err)
	}
	if err := validateBloomFilter(filterRaw); err != nil {
		return nil, err
	}
	indexPayload, err := tr.readBlockRaw(tr.indexBH)
	if err != nil {
		return nil, fmt.Errorf("table: read index: %w", err)
	}
	m := &tableMetaBlocks{filter: filterRaw, indexBuf: indexPayload}
	// Data blocks end where the range-del block starts (if present) or the filter
	// block starts otherwise; parseIndex bounds the last data block against that.
	dataEnd := tr.filterBH.offset
	haveRangeDel := tr.rangeDelBH.length != 0 || tr.rangeDelBH.offset != 0
	if haveRangeDel {
		dataEnd = tr.rangeDelBH.offset
	}
	// Walk the index payload directly, recording each separator's position so the
	// entry stores an offset/length instead of a 24-byte slice header.
	if err := tr.parseIndex(m, indexPayload, dataEnd); err != nil {
		return nil, fmt.Errorf("table: parse index: %w", err)
	}
	if haveRangeDel {
		rdPayload, err := tr.readBlockRaw(tr.rangeDelBH)
		if err != nil {
			return nil, fmt.Errorf("table: read range-del: %w", err)
		}
		rts, err := decodeRangeDelBlock(rdPayload)
		if err != nil {
			return nil, err
		}
		// Copy out: rts aliases rdPayload, which is a transient read buffer.
		m.rangeDel = make([]rangeTombstone, len(rts))
		for i := range rts {
			m.rangeDel[i] = rangeTombstone{
				start: append([]byte(nil), rts[i].start...),
				end:   append([]byte(nil), rts[i].end...),
				seq:   rts[i].seq,
			}
		}
	}
	return m, nil
}

// parseIndex parses index entries from payload into m, recording separators by
// position.
func (tr *tableReader) parseIndex(m *tableMetaBlocks, payload []byte, dataEnd uint64) error {
	pos := 0
	var nextOffset uint64
	var previousSep []byte
	for pos < len(payload) {
		keyLen, n := binary.Uvarint(payload[pos:])
		if n <= 0 {
			return fmt.Errorf("bad separator length")
		}
		pos += n
		if keyLen < trailerLen || keyLen > uint64(len(payload)-pos) || keyLen > uint64(^uint32(0)) {
			return fmt.Errorf("separator exceeds index block")
		}
		keyStart := pos
		pos += int(keyLen)

		valLen, n := binary.Uvarint(payload[pos:])
		if n <= 0 {
			return fmt.Errorf("bad handle length")
		}
		pos += n
		if valLen > uint64(len(payload)-pos) {
			return fmt.Errorf("handle exceeds index block")
		}
		handleBytes := payload[pos : pos+int(valLen)]
		pos += int(valLen)

		h, used := decodeHandle(handleBytes)
		if used == 0 || used != len(handleBytes) || h.length > uint64(^uint32(0)) {
			return fmt.Errorf("bad block handle")
		}
		if h.length < blockTrailerLen || h.length > uint64(tr.size-footerLen) || h.offset > uint64(tr.size-footerLen)-h.length {
			return fmt.Errorf("block handle out of range")
		}
		if h.offset != nextOffset || h.length > dataEnd-h.offset {
			return fmt.Errorf("non-contiguous data block layout")
		}
		sep := payload[keyStart : keyStart+int(keyLen)]
		_, kind := ikeySeqKind(sep)
		if !validIKeyKind(kind) {
			return fmt.Errorf("unknown internal-key kind %d", kind)
		}
		if previousSep != nil && ikeyCompare(previousSep, sep) >= 0 {
			return fmt.Errorf("index separators out of order")
		}
		previousSep = sep
		nextOffset = h.offset + h.length
		m.indexEnt = append(m.indexEnt, indexEntry{
			sepOff: uint32(keyStart),
			sepLen: uint32(keyLen),
			offset: h.offset,
			length: uint32(h.length),
		})
	}
	if nextOffset != dataEnd {
		return fmt.Errorf("index does not cover data region")
	}
	return nil
}

// sep returns the separator for entry i as a subslice of the parsed index.
func (m *tableMetaBlocks) sep(i int) []byte {
	e := &m.indexEnt[i]
	return m.indexBuf[e.sepOff : e.sepOff+e.sepLen]
}

// blockHandleAt returns the on-disk handle for index entry i.
func (m *tableMetaBlocks) blockHandleAt(i int) blockHandle {
	e := &m.indexEnt[i]
	return blockHandle{offset: e.offset, length: uint64(e.length)}
}

// readBlockRaw reads a block's bytes and returns its decoded payload. The
// handle is validated against the file size first so a corrupt footer/index
// cannot trigger a huge allocation or an out-of-range read.
func (tr *tableReader) readBlockRaw(h blockHandle) ([]byte, error) {
	if tr.size < footerLen {
		return nil, fmt.Errorf("table: block handle out of range")
	}
	dataSize := uint64(tr.size - footerLen)
	if h.length < blockTrailerLen || h.length > dataSize || h.offset > dataSize-h.length {
		return nil, fmt.Errorf("table: block handle out of range")
	}
	buf := make([]byte, h.length)
	if _, err := tr.r.ReadAt(buf, int64(h.offset)); err != nil {
		return nil, err
	}
	return decodeBlock(buf)
}

// readBlock returns a decoded block payload, consulting the cache first and
// populating it on a miss.
func (tr *tableReader) readBlock(h blockHandle) ([]byte, error) {
	if tr.cache != nil {
		key := blockCacheKey{Table: tr.tableNum, Offset: h.offset}
		if payload, ok := tr.cache.get(key); ok {
			return payload, nil
		}
		payload, err := tr.readBlockRaw(h)
		if err != nil {
			return nil, err
		}
		tr.cache.Add(key, payload)
		return payload, nil
	}
	return tr.readBlockRaw(h)
}

// Get returns the value for userKey at the newest version with seq <= the query
// seq. found reports whether any version was seen; deleted reports a tombstone.
func (tr *tableReader) get(userKey []byte, seq uint64) (value []byte, found, deleted bool, err error) {
	value, _, found, deleted, err = tr.lookupAtTime(userKey, seq, time.Now().UnixNano(), true)
	return value, found, deleted, err
}

// lookupAtTime returns the newest visible version in this table together with
// its sequence. The read path needs the sequence because compaction output file
// creation order is not data recency order. When copyValue is
// false, the returned value aliases the decoded block and is valid only for
// the immediate consumer.
func (tr *tableReader) lookupAtTime(userKey []byte, seq uint64, now int64, copyValue bool) (value []byte, versionSeq uint64, found, deleted bool, err error) {
	// Load the parsed metadata once; a concurrent eviction cannot invalidate this
	// local reference.
	m, err := tr.ensureMeta()
	if err != nil {
		return nil, 0, false, false, err
	}
	// Bloom gate: a definite miss avoids all block reads.
	if !bloomMayContain(m.filter, bloomHash(userKey)) {
		return nil, 0, false, false, nil
	}

	// Build the lookup key in a stack buffer to avoid a heap allocation on the
	// point-lookup hot path. Keys longer than the inline buffer fall back to a
	// heap allocation inside ikeyLookupKey.
	var buf [64]byte
	lookup := ikeyLookupKey(buf[:0], userKey, seq)

	// Find the first index entry whose separator is >= lookup key.
	i := m.findBlock(lookup)
	if i >= len(m.indexEnt) {
		return nil, 0, false, false, nil
	}
	payload, err := tr.readBlock(m.blockHandleAt(i))
	if err != nil {
		return nil, 0, false, false, err
	}

	// A value dataBlockIter stays on the stack, avoiding an allocation. It
	// reconstructs prefix-compressed keys into its own buffer as it advances.
	entries, restarts, nRestart, splitErr := splitDataBlock(payload)
	if splitErr != nil {
		return nil, 0, false, false, splitErr
	}
	it := dataBlockIter{entries: entries, restarts: restarts, nRestart: nRestart}
	// Binary-search restart points to the interval that may hold userKey rather
	// than scanning the whole block from the front.
	it.seek(lookup)
	// it.key is reconstructed into a reused buffer, so the ordering guard keeps
	// its own copy of the prior key rather than aliasing it.
	var previous []byte
	havePrevious := false
	for it.next() {
		if len(it.key) < trailerLen {
			return nil, 0, false, false, fmt.Errorf("table: short internal key")
		}
		if havePrevious && ikeyCompare(previous, it.key) >= 0 {
			return nil, 0, false, false, fmt.Errorf("table: block keys out of order")
		}
		previous = append(previous[:0], it.key...)
		havePrevious = true
		entryUser := ikeyUserKey(it.key)
		cmp := bytes.Compare(entryUser, userKey)
		if cmp > 0 {
			break // passed the key; not present in this block
		}
		if cmp < 0 {
			continue
		}
		kseq, kind := ikeySeqKind(it.key)
		if !validIKeyKind(kind) {
			return nil, 0, false, false, fmt.Errorf("table: unknown internal-key kind %d", kind)
		}
		if kseq > seq {
			continue // version newer than the snapshot
		}
		if kind == ikeyKindDelete {
			return nil, kseq, true, true, nil
		}
		value := it.value
		if kind == ikeyKindSetTTL {
			var expiresAt int64
			value, expiresAt, err = decodeExpiringValue(value)
			if err != nil {
				return nil, 0, false, false, fmt.Errorf("table: %w", err)
			}
			if expiresAt <= now {
				return nil, kseq, true, true, nil
			}
		}
		if copyValue {
			// Copy the value out: it.value aliases the (possibly cached) block
			// payload, which may be evicted after this call returns.
			return append([]byte(nil), value...), kseq, true, false, nil
		}
		return value, kseq, true, false, nil
	}
	if err := it.Error(); err != nil {
		return nil, 0, false, false, err
	}
	return nil, 0, false, false, nil
}

// has reports whether userKey exists at seq without copying its value, making
// it cheaper than get for a pure membership check.
func (tr *tableReader) has(userKey []byte, seq uint64) (found, deleted bool, err error) {
	_, _, found, deleted, err = tr.lookupAtTime(userKey, seq, time.Now().UnixNano(), false)
	return found, deleted, err
}

func (tr *tableReader) hasVersionAtTime(userKey []byte, seq uint64, now int64) (versionSeq uint64, found, deleted bool, err error) {
	_, versionSeq, found, deleted, err = tr.lookupAtTime(userKey, seq, now, false)
	return versionSeq, found, deleted, err
}

// findBlock returns the index of the first block that may contain lookup.
func (m *tableMetaBlocks) findBlock(lookup []byte) int {
	lo, hi := 0, len(m.indexEnt)
	for lo < hi {
		mid := (lo + hi) / 2
		if ikeyCompare(m.sep(mid), lookup) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// Iterator iterates every entry in the table in internal-key order. inner is an
// embedded value so advancing across blocks does not allocate a new iterator. It
// pins the parsed metadata for its lifetime so an eviction cannot invalidate the
// index it walks across blocks.
type tableIterator struct {
	tr      *tableReader
	meta    *tableMetaBlocks
	blk     int
	inner   dataBlockIter
	loaded  bool // whether inner points at a loaded block
	err     error
	lastKey []byte

	// win is a read-ahead window over the contiguous data region. A sequential
	// scan (compaction, range iteration) reads many blocks in offset order, so
	// one large ReadAt per window replaces one pread per block; large values that
	// fill a block each would otherwise cost a syscall apiece.
	win    []byte
	winOff uint64 // file offset of win[0]

	// rawScratch and decScratch are reused across blocks: block N is fully
	// consumed before block N+1 is read, so one buffer each avoids a per-block
	// allocation for the raw copy and the decompressed payload.
	rawScratch []byte
	decScratch []byte
}

// tableReadAheadSize is the read-ahead window for iterator scans. It matches the
// table write buffer so a table written in one buffered pass is read back in a
// comparable number of syscalls.
const tableReadAheadSize = 256 << 10

// blockRaw returns a private copy of the on-disk bytes for the data block at
// handle h, filling a read-ahead window with one ReadAt and refilling when h
// falls outside it. A block larger than the window is read directly. The copy is
// required because decodeBlock returns the input subslice for uncompressed
// blocks, and the caller (a merge scan) may retain that block's keys while a
// later block refills and overwrites the shared window.
func (it *tableIterator) blockRaw(h blockHandle) ([]byte, error) {
	if h.length > tableReadAheadSize {
		buf := make([]byte, h.length)
		if _, err := it.tr.r.ReadAt(buf, int64(h.offset)); err != nil {
			return nil, err
		}
		return buf, nil
	}
	if it.win == nil || h.offset < it.winOff || h.offset+h.length > it.winOff+uint64(len(it.win)) {
		if cap(it.win) < tableReadAheadSize {
			it.win = make([]byte, tableReadAheadSize)
		}
		// Fill from h.offset, clamped to the file so the final window is not padded
		// with a short read past EOF.
		end := h.offset + tableReadAheadSize
		if end > uint64(it.tr.size) {
			end = uint64(it.tr.size)
		}
		buf := it.win[:end-h.offset]
		if _, err := it.tr.r.ReadAt(buf, int64(h.offset)); err != nil {
			return nil, err
		}
		it.win = buf
		it.winOff = h.offset
	}
	start := h.offset - it.winOff
	// Copy the block out of the shared window into a reused scratch buffer: the
	// window is refilled as the scan advances, so the payload must not alias it.
	it.rawScratch = append(it.rawScratch[:0], it.win[start:start+h.length]...)
	return it.rawScratch, nil
}

// NewIterator returns an iterator positioned before the first entry.
func (tr *tableReader) newIterator() *tableIterator {
	return &tableIterator{tr: tr, blk: -1}
}

// Next advances to the next entry and reports whether one is available.
func (it *tableIterator) Next() bool {
	if it.err != nil {
		return false
	}
	if it.meta == nil { // pin metadata on first advance
		m, err := it.tr.ensureMeta()
		if err != nil {
			it.err = err
			return false
		}
		it.meta = m
	}
	for {
		if it.loaded {
			if it.inner.next() {
				if len(it.inner.key) < trailerLen {
					it.err = fmt.Errorf("table: short internal key")
					return false
				}
				_, kind := ikeySeqKind(it.inner.key)
				if !validIKeyKind(kind) {
					it.err = fmt.Errorf("table: unknown internal-key kind %d", kind)
					return false
				}
				if it.lastKey != nil && ikeyCompare(it.lastKey, it.inner.key) >= 0 {
					it.err = fmt.Errorf("table: keys out of order")
					return false
				}
				it.lastKey = append(it.lastKey[:0], it.inner.key...)
				return true
			}
			if err := it.inner.Error(); err != nil {
				it.err = err
				return false
			}
		}
		it.blk++
		if it.blk >= len(it.meta.indexEnt) {
			return false
		}
		raw, err := it.blockRaw(it.meta.blockHandleAt(it.blk))
		if err != nil {
			it.err = err
			return false
		}
		payload, usedDst, err := decodeBlockInto(it.decScratch, raw)
		if err != nil {
			it.err = err
			return false
		}
		// Retain the decompressed buffer for reuse next block, but only when it was
		// used: an uncompressed payload aliases raw, and keeping that as decScratch
		// would let the next decompress write into raw's backing array mid-read.
		if usedDst {
			it.decScratch = payload
		}
		entries, _, _, splitErr := splitDataBlock(payload)
		if splitErr != nil {
			it.err = splitErr
			return false
		}
		// Reset in place so the key-reconstruction buffers are reused across blocks
		// instead of reallocated per block (a full scan walks every block).
		it.inner.resetEntries(entries)
		it.loaded = true
	}
}

// InternalKey returns the current internal key.
func (it *tableIterator) internalKey() []byte { return it.inner.key }

// Value returns the current value.
func (it *tableIterator) Value() []byte { return it.inner.value }

// Error returns any error encountered during iteration.
func (it *tableIterator) Error() error { return it.err }
