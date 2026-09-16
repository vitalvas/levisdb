package levisdb

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// Block wire format on disk:
//
//	[payload (codec-compressed)] [codec-id: 1] [crc32c over payload+id: 4]
//
// Compression is conditional per block: a payload the codec fails to shrink is
// stored raw with codec id codecNone. The per-block codec id records which path
// was taken, so the reader decompresses correctly regardless of the table's
// configured codec.
//
// There are two payload layouts.
//
// Index blocks are flat (blockBuilder / blockIter): a sequence of
//
//	[keylen uvarint][key][vallen uvarint][value]
//
// in ascending internal-key order. Index blocks hold one entry per data block,
// so prefix compression there would save nothing while breaking the reader's
// zero-copy separator subslices.
//
// Data blocks use LevelDB-style prefix compression with restart points
// (dataBlockBuilder / dataBlockIter). Each entry is
//
//	[shared uvarint][non_shared uvarint][vallen uvarint][delta_key][value]
//
// where delta_key is the non_shared suffix and the full key is the previous
// key's first `shared` bytes followed by delta_key. Every restartInterval
// entries a restart point stores the full key (shared == 0). The payload ends
// with a trailer of restart offsets so a lookup can binary-search:
//
//	... entries ... [restart_0 u32] ... [restart_k u32] [num_restarts u32]

const blockTrailerLen = 5 // codec id (1) + crc (4)

var tableCastagnoli = crc32.MakeTable(crc32.Castagnoli)

// blockHandle locates a block within the file.
type blockHandle struct {
	offset uint64
	length uint64 // length of the on-disk block including trailer
}

func (h blockHandle) encode(dst []byte) []byte {
	dst = binary.AppendUvarint(dst, h.offset)
	dst = binary.AppendUvarint(dst, h.length)
	return dst
}

// decodeHandle reads a block handle from src. On malformed input (a uvarint
// that is truncated or overflows, which binary.Uvarint signals with n <= 0) it
// returns a zero handle and 0 rather than panicking on a negative slice index;
// callers detect the bad handle via the file-size range check when reading it.
func decodeHandle(src []byte) (blockHandle, int) {
	off, n1 := binary.Uvarint(src)
	if n1 <= 0 {
		return blockHandle{}, 0
	}
	length, n2 := binary.Uvarint(src[n1:])
	if n2 <= 0 {
		return blockHandle{}, 0
	}
	return blockHandle{offset: off, length: length}, n1 + n2
}

// blockBuilder accumulates sorted entries into a block payload.
type blockBuilder struct {
	buf     []byte
	entries int
	lastKey []byte
}

func (b *blockBuilder) add(key, value []byte) {
	b.buf = binary.AppendUvarint(b.buf, uint64(len(key)))
	b.buf = append(b.buf, key...)
	b.buf = binary.AppendUvarint(b.buf, uint64(len(value)))
	b.buf = append(b.buf, value...)
	b.lastKey = append(b.lastKey[:0], key...)
	b.entries++
}

func (b *blockBuilder) size() int   { return len(b.buf) }
func (b *blockBuilder) empty() bool { return b.entries == 0 }

func (b *blockBuilder) reset() {
	b.buf = b.buf[:0]
	b.entries = 0
	b.lastKey = b.lastKey[:0]
}

// finishBlock builds the on-disk block: [payload][codec-id][crc32c]. The codec
// result is kept only if it is actually smaller than the raw payload; a block is
// never stored larger than raw, so incompressible data falls back to codecNone.
// The per-block codec id records which path was taken so the reader decompresses
// correctly. dst is a reusable scratch buffer the caller owns (pass nil for a
// one-off); the returned slice reuses dst's backing array, so the caller must
// consume the block before calling finishBlock again with the same dst. Reusing
// dst avoids a per-block compression-output allocation on the write paths.
func finishBlock(dst, payload []byte, c blockCodec) []byte {
	use := c
	out := use.compress(dst[:0], payload)
	// Never ship a block larger than its raw payload. Both forms gain the same
	// 1-byte id + 4-byte crc, so comparing the pre-trailer payloads is exact.
	if use.id() != codecNone && len(out) >= len(payload) {
		use = noneCodec{}
		// Reuse out's backing array (it already aliases dst) to hold the raw payload.
		out = append(out[:0], payload...)
	}
	out = append(out, byte(use.id()))
	crc := crc32.Checksum(out, tableCastagnoli)
	var trailer [4]byte
	binary.LittleEndian.PutUint32(trailer[:], crc)
	return append(out, trailer[:]...)
}

// decodeBlock verifies the CRC and decompresses an on-disk block into its
// entry payload.
func decodeBlock(raw []byte) ([]byte, error) {
	payload, _, err := decodeBlockInto(nil, raw)
	return payload, err
}

// decodeBlockInto is decodeBlock with a reusable destination for the decompressed
// payload, so a sequential scan can decompress into one per-iterator scratch
// buffer instead of allocating a fresh block each step. dst may be nil. usedDst
// reports whether the payload was decompressed into dst (true) or aliases raw
// because the block was stored uncompressed (false); the caller reuses dst only
// when usedDst is true so it never ends up aliasing raw. When usedDst is false
// the returned payload aliases raw, so raw must stay valid while it is read.
func decodeBlockInto(dst, raw []byte) (payload []byte, usedDst bool, err error) {
	if len(raw) < blockTrailerLen {
		return nil, false, fmt.Errorf("table: block too short")
	}
	body := raw[:len(raw)-4]
	want := binary.LittleEndian.Uint32(raw[len(raw)-4:])
	if crc32.Checksum(body, tableCastagnoli) != want {
		return nil, false, fmt.Errorf("table: block crc mismatch")
	}
	id := codecID(body[len(body)-1])
	comp := body[:len(body)-1]
	if id == codecNone {
		// Uncompressed: the payload is already a subslice of the caller-owned
		// read buffer, so return it directly instead of copying it out.
		return comp, false, nil
	}
	c, err := codecFromID(id)
	if err != nil {
		return nil, false, err
	}
	out, err := c.decompress(dst[:0], comp)
	return out, true, err
}

// blockIter iterates entries within a decoded block payload.
type blockIter struct {
	payload []byte
	pos     int
	key     []byte
	value   []byte
	err     error
}

func newBlockIter(payload []byte) *blockIter {
	return &blockIter{payload: payload}
}

// next advances to the next entry and reports whether one is available.
func (it *blockIter) next() bool {
	if it.err != nil || it.pos >= len(it.payload) {
		return false
	}
	keyLen, n := binary.Uvarint(it.payload[it.pos:])
	if n <= 0 {
		it.err = fmt.Errorf("table: malformed key length")
		return false
	}
	it.pos += n
	if keyLen > uint64(len(it.payload)-it.pos) {
		it.err = fmt.Errorf("table: key exceeds block")
		return false
	}
	it.key = it.payload[it.pos : it.pos+int(keyLen)]
	it.pos += int(keyLen)

	valLen, n := binary.Uvarint(it.payload[it.pos:])
	if n <= 0 {
		it.err = fmt.Errorf("table: malformed value length")
		return false
	}
	it.pos += n
	if valLen > uint64(len(it.payload)-it.pos) {
		it.err = fmt.Errorf("table: value exceeds block")
		return false
	}
	it.value = it.payload[it.pos : it.pos+int(valLen)]
	it.pos += int(valLen)
	return true
}

func (it *blockIter) Error() error { return it.err }

// defaultRestartInterval matches LevelDB: a restart point (full key) every 16
// entries bounds delta-decode work per seek and keeps the restart array small.
const defaultRestartInterval = 16

// dataBlockBuilder accumulates sorted entries into a prefix-compressed data
// block payload with periodic restart points.
type dataBlockBuilder struct {
	buf             []byte
	restarts        []uint32
	entries         int
	counter         int // entries since the last restart
	restartInterval int
	lastKey         []byte
}

func (b *dataBlockBuilder) restartEvery() int {
	if b.restartInterval > 0 {
		return b.restartInterval
	}
	return defaultRestartInterval
}

func (b *dataBlockBuilder) add(key, value []byte) {
	shared := 0
	if b.counter < b.restartEvery() {
		// Share the longest common prefix with the previous key.
		limit := min(len(b.lastKey), len(key))
		for shared < limit && b.lastKey[shared] == key[shared] {
			shared++
		}
	} else {
		// Start a new restart point: store the full key (shared == 0).
		b.restarts = append(b.restarts, uint32(len(b.buf)))
		b.counter = 0
	}
	nonShared := key[shared:]
	b.buf = binary.AppendUvarint(b.buf, uint64(shared))
	b.buf = binary.AppendUvarint(b.buf, uint64(len(nonShared)))
	b.buf = binary.AppendUvarint(b.buf, uint64(len(value)))
	b.buf = append(b.buf, nonShared...)
	b.buf = append(b.buf, value...)

	b.lastKey = append(b.lastKey[:0], key...)
	b.entries++
	b.counter++
}

func (b *dataBlockBuilder) empty() bool { return b.entries == 0 }

// size approximates the finished payload size (entries + restart trailer) so the
// writer can roll blocks at the target size.
func (b *dataBlockBuilder) size() int {
	return len(b.buf) + (len(b.restarts)+1)*4 + 4
}

func (b *dataBlockBuilder) reset() {
	b.buf = b.buf[:0]
	b.restarts = b.restarts[:0]
	b.entries = 0
	b.counter = 0
	b.lastKey = b.lastKey[:0]
}

// finish appends the restart array and count, returning the full payload. The
// first entry is always a restart point.
func (b *dataBlockBuilder) finish() []byte {
	// The first entry (offset 0) is implicitly the first restart point; add() only
	// records restarts at interval boundaries after it, so prepend offset 0.
	restarts := make([]uint32, 0, len(b.restarts)+1)
	if b.entries > 0 {
		restarts = append(restarts, 0)
	}
	restarts = append(restarts, b.restarts...)
	for _, r := range restarts {
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], r)
		b.buf = append(b.buf, tmp[:]...)
	}
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], uint32(len(restarts)))
	b.buf = append(b.buf, tmp[:]...)
	return b.buf
}

// dataBlockIter iterates a prefix-compressed data block, reconstructing full
// keys. Because a key is built from the previous key's prefix, the iterator
// double-buffers: the key returned by internalKey() stays valid across exactly
// one following next() call. That one-advance stability is what mergeIter and
// the compaction consumer rely on (they read the key before advancing again).
// value aliases the payload and is likewise valid until the next next().
type dataBlockIter struct {
	entries  []byte // the entry region (payload without the restart trailer)
	restarts []byte // the restart-offset array (numRestarts x u32, little-endian)
	nRestart int
	pos      int
	bufs     [2][]byte // alternating key buffers for one-advance stability
	which    int       // index of the buffer holding the current key
	key      []byte    // reconstructed full key (== bufs[which][:len])
	value    []byte    // aliases entries
	err      error
}

// splitDataBlock separates a data-block payload into its entry region and its
// restart-offset array. A malformed trailer yields an error rather than a panic.
func splitDataBlock(payload []byte) (entries, restarts []byte, numRestarts int, err error) {
	if len(payload) < 4 {
		return nil, nil, 0, fmt.Errorf("table: data block too short")
	}
	n := int(binary.LittleEndian.Uint32(payload[len(payload)-4:]))
	// Trailer is n restart offsets (4 bytes each) plus the count word.
	trailer := (n + 1) * 4
	if n < 0 || trailer < 0 || trailer > len(payload) {
		return nil, nil, 0, fmt.Errorf("table: bad data block restart count")
	}
	entryEnd := len(payload) - trailer
	return payload[:entryEnd], payload[entryEnd : len(payload)-4], n, nil
}

func newDataBlockIter(payload []byte) *dataBlockIter {
	entries, restarts, n, err := splitDataBlock(payload)
	return &dataBlockIter{entries: entries, restarts: restarts, nRestart: n, err: err}
}

// resetEntries reinitializes the iterator over a new block's entry region while
// retaining the two key buffers, so a table scan that walks block after block
// (compaction, full iteration) does not reallocate the key-reconstruction
// buffers for every block. Only the per-block cursor state is cleared; bufs are
// kept and reused. The caller has already split the payload's restart trailer
// off, so restart-seek is unavailable on a reset iterator (scans do not seek).
func (it *dataBlockIter) resetEntries(entries []byte) {
	it.entries = entries
	it.restarts = nil
	it.nRestart = 0
	it.pos = 0
	it.which = 0
	it.key = nil
	it.value = nil
	it.err = nil
}

// restartOffset returns the entry offset recorded at restart index i, or -1 if
// it points outside the entry region (corrupt block).
func (it *dataBlockIter) restartOffset(i int) int {
	if (i+1)*4 > len(it.restarts) {
		return -1
	}
	off := int(binary.LittleEndian.Uint32(it.restarts[i*4:]))
	if off < 0 || off >= len(it.entries) {
		return -1
	}
	return off
}

// keyAt decodes the full key of the entry starting at entry offset off. Restart
// entries store shared==0, so seek() only ever calls this at restart offsets
// where the delta is the whole key.
func (it *dataBlockIter) keyAt(off int) ([]byte, bool) {
	if off < 0 || off >= len(it.entries) {
		return nil, false
	}
	shared, n := binary.Uvarint(it.entries[off:])
	if n <= 0 {
		return nil, false
	}
	off += n
	nonShared, n := binary.Uvarint(it.entries[off:])
	if n <= 0 {
		return nil, false
	}
	off += n
	_, n = binary.Uvarint(it.entries[off:]) // skip value length
	if n <= 0 {
		return nil, false
	}
	off += n
	if shared != 0 || nonShared < trailerLen || nonShared > uint64(len(it.entries)-off) {
		// Not a restart entry, too short to be a valid internal key, or truncated.
		return nil, false
	}
	return it.entries[off : off+int(nonShared)], true
}

// seek positions the iterator just before the first entry whose key is >=
// target, using a restart-point binary search so a large block is not scanned
// linearly from the start. After seek, call next() to read entries in order.
// A malformed restart array falls back to a scan from the block start.
func (it *dataBlockIter) seek(target []byte) {
	// Binary-search restart points for the greatest whose key is <= target.
	lo, hi := 0, it.nRestart
	for lo < hi {
		mid := (lo + hi) / 2
		k, ok := it.keyAt(it.restartOffset(mid))
		if !ok {
			// Corrupt restart entry: fall back to a full scan from the start.
			it.pos, it.key = 0, nil
			return
		}
		if ikeyCompare(k, target) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	// lo is the first restart with key >= target; start from the one before it so
	// the linear scan does not skip the matching entry inside that interval.
	it.key = nil // reset prefix reconstruction; a restart entry has shared==0
	if it.nRestart == 0 {
		it.pos = 0
		return
	}
	start := lo - 1
	if start < 0 {
		start = 0
	}
	off := it.restartOffset(start)
	if off < 0 { // corrupt offset: fall back to a full scan from the start
		off = 0
	}
	it.pos = off
}

func (it *dataBlockIter) next() bool {
	if it.err != nil || it.pos >= len(it.entries) {
		return false
	}
	shared, n := binary.Uvarint(it.entries[it.pos:])
	if n <= 0 {
		it.err = fmt.Errorf("table: malformed shared length")
		return false
	}
	it.pos += n
	nonShared, n := binary.Uvarint(it.entries[it.pos:])
	if n <= 0 {
		it.err = fmt.Errorf("table: malformed non-shared length")
		return false
	}
	it.pos += n
	valLen, n := binary.Uvarint(it.entries[it.pos:])
	if n <= 0 {
		it.err = fmt.Errorf("table: malformed value length")
		return false
	}
	it.pos += n

	prev := it.key // previous key, held in bufs[it.which]
	if shared > uint64(len(prev)) {
		it.err = fmt.Errorf("table: shared prefix exceeds previous key")
		return false
	}
	if nonShared > uint64(len(it.entries)-it.pos) {
		it.err = fmt.Errorf("table: key exceeds block")
		return false
	}
	delta := it.entries[it.pos : it.pos+int(nonShared)]
	it.pos += int(nonShared)

	// Reconstruct into the other buffer so the previously returned key stays
	// valid across this advance: shared prefix of prev, then the delta.
	it.which ^= 1
	buf := it.bufs[it.which][:0]
	buf = append(buf, prev[:shared]...)
	buf = append(buf, delta...)
	it.bufs[it.which] = buf
	it.key = buf

	if valLen > uint64(len(it.entries)-it.pos) {
		it.err = fmt.Errorf("table: value exceeds block")
		return false
	}
	it.value = it.entries[it.pos : it.pos+int(valLen)]
	it.pos += int(valLen)
	return true
}

func (it *dataBlockIter) Error() error { return it.err }
