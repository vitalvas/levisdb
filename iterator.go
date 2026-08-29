package levisdb

import "time"

// dbIterator wraps the engine iterator to manage the snapshot pin and iterator-
// time lifecycle and to hand out stable key/value copies. TTL is evaluated at
// iterator creation, so a long scan is internally consistent even if a value's
// deadline passes while it runs.
type dbIterator struct {
	src         *engineIterator
	curK        []byte
	curV        []byte
	err         error
	snaps       *snapshots
	seq         uint64
	readTime    int64
	pinReleased bool
}

func (db *DB) newRangeIterator(seq uint64, start, end []byte) (Iterator, error) {
	readTime := time.Now().UnixNano()
	db.snaps.acquireIteratorTime(readTime)
	src := db.eng.newRangeIteratorAt(seq, start, end, readTime)
	return &dbIterator{src: src, snaps: db.snaps, seq: seq, readTime: readTime}, nil
}

// Next advances to the next key and reports whether one exists.
func (it *dbIterator) Next() bool {
	if it.err != nil {
		it.releasePin()
		return false
	}
	if !it.src.Next() {
		if err := it.src.Error(); err != nil {
			it.err = err
		}
		it.releasePin()
		return false
	}
	it.curK = append(it.curK[:0], it.src.Key()...)
	it.curV = append(it.curV[:0], it.src.Value()...)
	return true
}

func (it *dbIterator) Key() []byte   { return it.curK }
func (it *dbIterator) Value() []byte { return it.curV }
func (it *dbIterator) Error() error  { return it.err }
func (it *dbIterator) Close() error {
	if err := it.src.Close(); err != nil && it.err == nil {
		it.err = err
	}
	it.releasePin()
	return it.err
}

func (it *dbIterator) releasePin() {
	if !it.pinReleased {
		it.pinReleased = true
		it.snaps.release(it.seq)
		it.snaps.releaseIteratorTime(it.readTime)
	}
}
