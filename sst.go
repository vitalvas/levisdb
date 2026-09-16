// SST ingest and export let a caller build a sorted table offline and load it
// into a database in bulk, bypassing the per-entry write path. IngestExternalFile
// assigns the whole file one fresh sequence (bumping the database's sequence past
// it) and installs it at the bottom tier, so the ingested keys are the newest
// version of every key they carry. The file must not overlap keys already live in
// the memtable or the fresh (L0) tier, so that single sequence cannot shadow a
// newer in-flight write.

package levisdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ingestBuildSeq is the placeholder sequence SstFileWriter bakes into every key.
// IngestExternalFile rewrites each key to the fresh sequence it assigns, so the
// build-time value only has to make keys sort by user key (a constant seq does).
const ingestBuildSeq = uint64(1)

// SstFileWriter builds a sorted table file offline for IngestExternalFile. Keys
// must be added in ascending order and must be unique. Call Finish exactly once;
// it is an error to add after Finish. A writer is not safe for concurrent use.
type SstFileWriter struct {
	f       *os.File
	path    string
	tw      *tableWriter
	lastKey []byte
	scratch []byte
	entries int
	closed  bool
	err     error
}

// SstWriterOptions configures the block format of a built table. The zero value
// is valid and uses the same defaults as a database (S2, 10 bloom bits, 4 KiB
// blocks); set fields to match the target database's read expectations.
type SstWriterOptions struct {
	Codec     string // block codec name; empty uses CodecS2
	BloomBits int    // bloom bits per key; zero uses DefaultBloomBits
	BlockSize int    // data block size in bytes; zero uses DefaultBlockSize
}

// NewSstFileWriter creates a table-file writer at path.
func NewSstFileWriter(path string, opts SstWriterOptions) (*SstFileWriter, error) {
	if opts.Codec == "" {
		opts.Codec = CodecS2
	}
	if opts.BloomBits == 0 {
		opts.BloomBits = DefaultBloomBits
	}
	if opts.BlockSize == 0 {
		opts.BlockSize = DefaultBlockSize
	}
	c, err := codecFromName(opts.Codec)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &SstFileWriter{
		f:    f,
		path: path,
		tw: newTableWriter(f, tableWriterConfig{
			codec:     c,
			bloomBits: opts.BloomBits,
			blockSize: opts.BlockSize,
		}),
	}, nil
}

// Put adds a key/value pair. Keys must be added in strictly ascending order.
func (w *SstFileWriter) Put(key, value []byte) error {
	return w.add(key, value, ikeyKindSet)
}

// PutTTL adds a key/value pair with a lifetime. A zero or negative ttl is an
// error; use Put for a value without expiration.
func (w *SstFileWriter) PutTTL(key, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	expiresAt, err := ttlExpiresAt(time.Now(), ttl)
	if err != nil {
		return err
	}
	return w.add(key, encodeExpiringValue(value, expiresAt), ikeyKindSetTTL)
}

// bytesWritten reports the compressed on-disk bytes flushed so far, so a caller
// building a multi-file image can roll to a new file at a size target.
func (w *SstFileWriter) bytesWritten() int64 { return w.tw.bytesWritten() }

// PutWithExpiry adds a key/value pair with an absolute expiration (Unix nanos),
// preserving an exact stored deadline rather than re-deriving one from the wall
// clock the way PutTTL does. expiresAt must be positive. Keys must be added in
// strictly ascending order. It is used to reproduce a source value's precise
// expiry when building a bootstrap image (see Snapshot.WriteTo).
func (w *SstFileWriter) PutWithExpiry(key, value []byte, expiresAt int64) error {
	if expiresAt <= 0 {
		return ErrInvalidTTL
	}
	return w.add(key, encodeExpiringValue(value, expiresAt), ikeyKindSetTTL)
}

// Delete adds a tombstone for key, so ingesting the file removes that key.
func (w *SstFileWriter) Delete(key []byte) error {
	return w.add(key, nil, ikeyKindDelete)
}

func (w *SstFileWriter) add(key, value []byte, kind ikeyKind) error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return fmt.Errorf("sst: writer already finished")
	}
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if w.lastKey != nil && bytes.Compare(key, w.lastKey) <= 0 {
		w.err = fmt.Errorf("sst: keys must be added in ascending order")
		return w.err
	}
	w.scratch = ikeyEncode(w.scratch[:0], key, ingestBuildSeq, kind)
	if err := w.tw.Add(w.scratch, value); err != nil {
		w.err = err
		return err
	}
	w.lastKey = append(w.lastKey[:0], key...)
	w.entries++
	return nil
}

// Finish writes the table footer and closes the file. The written file is ready
// for IngestExternalFile. It is an error to Finish an empty writer.
func (w *SstFileWriter) Finish() error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return fmt.Errorf("sst: writer already finished")
	}
	w.closed = true
	if w.entries == 0 {
		_ = w.f.Close()
		_ = removeFileDurable(w.path)
		return fmt.Errorf("sst: cannot finish an empty file")
	}
	if _, err := w.tw.finish(); err != nil {
		_ = w.f.Close()
		return err
	}
	if err := w.f.Sync(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

// discard closes and removes the file without finishing it, for a caller that
// built no entries (Finish rejects empty) or hit an error mid-build and must not
// leave a partial table behind. It mirrors Finish's own empty-file cleanup.
func (w *SstFileWriter) discard() {
	if w.closed {
		return
	}
	w.closed = true
	_ = w.f.Close()
	_ = removeFileDurable(w.path)
}

// IngestExternalFile loads a table built by SstFileWriter into the database in
// bulk. Every key in the file is assigned one fresh sequence (bumping the
// database's sequence past it) and installed at the bottom tier, so the ingested
// entries become the newest version of the keys they carry.
//
// The file's key range must not overlap any key still live in the memtable or the
// fresh (L0) tier: a single sequence assigned above the current watermark would
// otherwise shadow a concurrent write it does not actually supersede. On overlap,
// IngestExternalFile returns an error and changes nothing. The file is copied
// (its keys are rewritten to the assigned sequence), so the source file is left
// untouched and may be ingested into several databases.
func (db *DB) IngestExternalFile(path string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}
	if err := db.backgroundError(); err != nil {
		return err
	}

	src, size, err := openExternalTable(path)
	if err != nil {
		return err
	}
	defer src.handle.close()

	// Reject a source that carries range tombstones: ingest copies only point
	// entries and assigns one bumped sequence at the bottom tier, which cannot
	// correctly reproduce a source range tombstone's shadowing. Accepting it would
	// silently drop the range deletes (covered keys would wrongly reappear), so
	// fail loudly instead. SstFileWriter cannot produce range tombstones, so a
	// file built for ingest never trips this; only a raw levisdb table (flush or
	// compaction output) can, and such a file is not a supported ingest input.
	if rts, rerr := src.reader.rangeTombstones(); rerr != nil {
		return rerr
	} else if len(rts) > 0 {
		return ErrIngestRangeDeletes
	}

	minKey, maxKey, err := externalKeyRange(src.reader)
	if err != nil {
		return err
	}
	if err := db.checkIngestOverlap(minKey, maxKey); err != nil {
		return err
	}

	// Assign the whole file one fresh sequence above every committed write, so its
	// entries are the newest version of their keys and no existing version shadows
	// them.
	seq, err := db.reserveSeq(1)
	if err != nil {
		return err
	}

	num := db.alloc.Next()
	if num == 0 {
		return ErrFileNumberExhausted
	}
	outPath, err := db.store.tablePath(num)
	if err != nil {
		return err
	}
	meta, err := db.writeIngestTable(num, outPath, src.reader, seq)
	if err != nil {
		return err
	}

	// Publish the assigned sequence before recording the manifest edit: the edit
	// stamps LastSeq from readSeq, and it must be at least the ingest sequence so a
	// reopen restores a watermark that makes the ingested keys visible.
	db.publishSeq(seq)

	install := func() {
		db.eng.mu.Lock()
		db.eng.tables = append(db.eng.tables, meta)
		db.eng.mu.Unlock()
	}
	if err := db.commitTableChange(nil, []*tableMeta{meta}, install); err != nil {
		_ = meta.releaseOwner(true)
		return err
	}
	db.log.Info("ingested external file",
		"op", "ingest", "path", path, "table", num, "seq", seq, "bytes", size)
	return nil
}

// externalTable is an opened table file for ingest reading.
type externalTable struct {
	handle *openFile
	reader *tableReader
}

// openExternalTable opens a built SST file for reading, validating its footer.
func openExternalTable(path string) (*externalTable, int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, 0, err
	}
	handle := newFDPool(-1).newHandle(path)
	r, err := newCachedTableReader(handle, info.Size(), nil, 0)
	if err != nil {
		handle.close()
		return nil, 0, fmt.Errorf("sst: open external file: %w", err)
	}
	return &externalTable{handle: handle, reader: r}, info.Size(), nil
}

// externalKeyRange returns the smallest and largest user keys in the table by
// scanning it once. A built table is non-empty (Finish rejects empty), so both
// bounds are set.
func externalKeyRange(r *tableReader) (minKey, maxKey []byte, err error) {
	it := r.newIterator()
	for it.Next() {
		uk := ikeyUserKey(it.internalKey())
		if minKey == nil {
			minKey = append([]byte(nil), uk...)
		}
		maxKey = append(maxKey[:0], uk...)
	}
	if it.Error() != nil {
		return nil, nil, fmt.Errorf("sst: scan external file: %w", it.Error())
	}
	if minKey == nil {
		return nil, nil, fmt.Errorf("sst: external file is empty")
	}
	return minKey, append([]byte(nil), maxKey...), nil
}

// checkIngestOverlap rejects an ingest whose key range [minKey, maxKey] overlaps
// a key still live in the memtable or the flushing memtable. Those hold writes
// that may not have published their sequence yet; a concurrent write could hold a
// reserved sequence below the one this ingest assigns, so ingesting the same key
// would let the ingest wrongly shadow it. Keys already in on-disk tables are
// committed at a lower sequence and are correctly superseded by the newer ingest,
// so table overlap is allowed. Caller holds db.mu.
func (db *DB) checkIngestOverlap(minKey, maxKey []byte) error {
	s := db.eng
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mem.overlapsUserRange(minKey, maxKey) ||
		(s.imm != nil && s.imm.overlapsUserRange(minKey, maxKey)) {
		return fmt.Errorf("sst: ingest range overlaps the active memtable")
	}
	return nil
}

// writeIngestTable copies src into a new bottom-tier table, rewriting every
// entry's sequence to seq. It mirrors engineT.writeTable but reads from a table
// iterator and re-stamps the key trailer.
func (db *DB) writeIngestTable(num uint32, path string, src *tableReader, seq uint64) (*tableMeta, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = f.Close()
			_ = removeFileDurable(path)
		}
	}()

	depth := maxTierDepth
	codecName := resolveLevelCodec(db.eng.cfg.LevelCodecs, depth, db.eng.cfg.FreshCodecName, db.opts.BottomCodec, true)
	c, err := codecFromName(codecName)
	if err != nil {
		return nil, err
	}
	w := newTableWriter(f, tableWriterConfig{
		codec:     c,
		bloomBits: db.eng.cfg.BloomBits,
		blockSize: db.eng.cfg.BlockSize,
	})

	var keyBuf []byte
	it := src.newIterator()
	for it.Next() {
		_, kind := ikeySeqKind(it.internalKey())
		keyBuf = ikeyEncode(keyBuf[:0], ikeyUserKey(it.internalKey()), seq, kind)
		if err := w.Add(keyBuf, it.Value()); err != nil {
			return nil, err
		}
	}
	if it.Error() != nil {
		return nil, fmt.Errorf("sst: read external file: %w", it.Error())
	}

	tsize, err := w.finish()
	if err != nil {
		return nil, err
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	meta, err := db.eng.openTableMeta(tableSpec{
		num:        num,
		depth:      depth,
		path:       path,
		size:       tsize,
		minKey:     w.minUserKey(),
		maxKey:     w.maxUserKey(),
		entries:    w.entryCount(),
		tombstones: w.tombstoneCount(),
	})
	if err != nil {
		return nil, err
	}
	keep = true
	return meta, nil
}
