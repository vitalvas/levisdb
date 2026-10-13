package levisdb

import "time"

// DBIterator is the Iterator returned by NewIterator, NewRangeIterator, and the
// snapshot iterators. Obtain one through those methods rather than constructing it
// directly. It wraps the engine iterator to manage the snapshot pin and iterator-
// time lifecycle and to hand out stable key/value copies. TTL is evaluated at
// iterator creation, so a long scan is internally consistent even if a value's
// deadline passes while it runs.
type DBIterator struct {
	src         *engineIterator
	curK        []byte
	curV        []byte
	curExpiry   int64
	err         error
	snaps       *snapshots
	seq         uint64
	readTime    int64
	pinReleased bool
}

func (db *DB) newRangeIterator(seq uint64, start, end []byte) (Iterator, error) {
	readTime := time.Now().Unix()
	db.snaps.acquireIteratorTime(readTime)
	src := db.eng.newRangeIteratorAt(seq, start, end, readTime)
	return &DBIterator{src: src, snaps: db.snaps, seq: seq, readTime: readTime}, nil
}

// Next advances to the next key and reports whether one exists.
func (it *DBIterator) Next() bool {
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
	it.curExpiry = it.src.valueExpiresAt()
	return true
}

func (it *DBIterator) Key() []byte      { return it.curK }
func (it *DBIterator) Value() []byte    { return it.curV }
func (it *DBIterator) ExpiresAt() int64 { return it.curExpiry }
func (it *DBIterator) Error() error     { return it.err }
func (it *DBIterator) Close() error {
	if err := it.src.Close(); err != nil && it.err == nil {
		it.err = err
	}
	it.releasePin()
	return it.err
}

func (it *DBIterator) releasePin() {
	if !it.pinReleased {
		it.pinReleased = true
		it.snaps.release(it.seq)
		it.snaps.releaseIteratorTime(it.readTime)
	}
}
