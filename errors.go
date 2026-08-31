package levisdb

import "errors"

var (
	// ErrNotFound is returned by Get when the key does not exist.
	ErrNotFound = errors.New("levisdb: key not found")

	// ErrClosed is returned when operating on a closed database.
	ErrClosed = errors.New("levisdb: database closed")

	// ErrReadOnly is returned when a write is attempted on a read-only database.
	ErrReadOnly = errors.New("levisdb: database is read-only")

	// ErrEmptyKey is returned when an empty key is passed to a write.
	ErrEmptyKey = errors.New("levisdb: empty key")

	// ErrEntryTooLarge is returned when a single key+value exceeds maxEntrySize.
	// The on-disk block format and the skiplist arena address data with uint32
	// offsets, so an entry near those limits would silently corrupt; it is
	// rejected at the write boundary instead.
	ErrEntryTooLarge = errors.New("levisdb: key/value too large")

	// ErrBatchTooLarge is returned when a single batch would add more than
	// maxEntrySize bytes to the memtable. A whole batch is applied before the
	// flush check runs, so an unbounded batch could push the skiplist arena past
	// its uint32 offset limit even when each entry is small enough on its own.
	ErrBatchTooLarge = errors.New("levisdb: batch too large")

	// ErrInvalidTTL is returned when a put has a negative TTL or its absolute
	// expiration cannot be represented as a Unix nanosecond timestamp.
	ErrInvalidTTL = errors.New("levisdb: invalid TTL")

	// ErrInvalidRange is returned by DeleteRange when the end is empty or not
	// strictly greater than the start, so the half-open range [start, end) is
	// empty or malformed.
	ErrInvalidRange = errors.New("levisdb: invalid key range")

	// ErrIngestRangeDeletes is returned by IngestExternalFile when the source
	// table contains range tombstones, which ingest cannot reproduce and would
	// otherwise silently drop. Build ingest files with SstFileWriter, which does
	// not produce range tombstones.
	ErrIngestRangeDeletes = errors.New("levisdb: external file contains range tombstones")

	// ErrRetentionExpired is returned by GetUpdatesSince when the requested
	// sequence is below the oldest retained WAL record, so the committed stream
	// from that point is no longer on disk. The consumer must re-bootstrap from a
	// snapshot and resume tailing from its sequence.
	ErrRetentionExpired = errors.New("levisdb: requested sequence below retained WAL horizon")

	// ErrFileNumberExhausted is returned rather than wrapping the uint32 file
	// namespace and risking replacement of an existing database file.
	ErrFileNumberExhausted = errors.New("levisdb: file number namespace exhausted")
)
