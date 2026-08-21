package levisdb

import (
	"sync"
	"sync/atomic"
)

// Snapshot is a stable read view of the database at a fixed sequence number.
// Reads through a snapshot never see writes committed after it was taken. TTL
// remains wall-clock based: a value can expire while a snapshot is held. Hold
// snapshots briefly: while one is live, compaction retains the versions it may
// need, which uses extra space.
type Snapshot struct {
	seq      uint64
	db       *DB
	released atomic.Bool
}

// Seq returns the sequence number the snapshot reads at.
func (s *Snapshot) Seq() uint64 { return s.seq }

// snapshots tracks live snapshot sequences so compaction knows the oldest
// version it must retain.
type snapshots struct {
	mu            sync.Mutex
	cond          *sync.Cond
	count         map[uint64]int // seq -> number of live snapshots at that seq
	iteratorTimes map[int64]int  // Unix nanos -> live iterators using that TTL view
	filterRuns    int            // compactions currently allowed to filter values
}

func newSnapshots() *snapshots {
	s := &snapshots{
		count:         map[uint64]int{},
		iteratorTimes: map[int64]int{},
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *snapshots) acquire(seq uint64) {
	s.mu.Lock()
	for s.filterRuns != 0 {
		s.cond.Wait()
	}
	s.count[seq]++
	s.mu.Unlock()
}

// acquireCurrent atomically captures and pins the latest committed sequence
// with respect to compaction's oldest-snapshot check. Loading the sequence and
// registering it separately would leave a window where a concurrent write and
// compaction could discard a version the new reader needs.
func (s *snapshots) acquireCurrent(current *atomic.Uint64) uint64 {
	s.mu.Lock()
	for s.filterRuns != 0 {
		s.cond.Wait()
	}
	seq := current.Load()
	s.count[seq]++
	s.mu.Unlock()
	return seq
}

// beginCompactionFilter reserves a snapshot-free interval for a compaction
// filter. Existing read pins make filtering unsafe, so the caller must compact
// without its filter instead. Once reserved, new read pins wait until every
// filtering compaction has ended.
func (s *snapshots) beginCompactionFilter() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.count) != 0 {
		return false
	}
	s.filterRuns++
	return true
}

func (s *snapshots) endCompactionFilter() {
	s.mu.Lock()
	if s.filterRuns <= 0 {
		s.mu.Unlock()
		panic("levisdb: unbalanced compaction filter reservation")
	}
	s.filterRuns--
	if s.filterRuns == 0 {
		s.cond.Broadcast()
	}
	s.mu.Unlock()
}

func (s *snapshots) release(seq uint64) {
	s.mu.Lock()
	if s.count[seq] > 1 {
		s.count[seq]--
	} else {
		delete(s.count, seq)
	}
	s.mu.Unlock()
}

// acquireIteratorTime pins the wall-clock view used by an iterator. Unlike
// point reads, an iterator acquires its shard sources incrementally and may run
// for an arbitrary time. Compaction must not reclaim a TTL value that was live
// at this instant until the iterator has acquired and released every source.
func (s *snapshots) acquireIteratorTime(readTime int64) {
	s.mu.Lock()
	s.iteratorTimes[readTime]++
	s.mu.Unlock()
}

func (s *snapshots) releaseIteratorTime(readTime int64) {
	s.mu.Lock()
	if s.iteratorTimes[readTime] > 1 {
		s.iteratorTimes[readTime]--
	} else {
		delete(s.iteratorTimes, readTime)
	}
	s.mu.Unlock()
}

// oldestIteratorTime returns the oldest TTL view held by a live iterator, or
// fallback when none exist. Reclaiming expiration at this cutoff is safe for
// every iterator currently being constructed or consumed.
func (s *snapshots) oldestIteratorTime(fallback int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	oldest := fallback
	for readTime := range s.iteratorTimes {
		if readTime < oldest {
			oldest = readTime
		}
	}
	return oldest
}

// oldest returns the smallest live snapshot sequence, or fallback if none are
// held. Compaction must retain every version with seq >= this value.
func (s *snapshots) oldest(fallback uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.count) == 0 {
		return fallback
	}
	oldest := fallback
	first := true
	for seq := range s.count {
		if first || seq < oldest {
			oldest = seq
			first = false
		}
	}
	return oldest
}

// live returns the number of currently-held snapshots across all sequences.
func (s *snapshots) live() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.count {
		n += c
	}
	return n
}

// Snapshot returns a stable read view at the latest committed sequence.
func (db *DB) Snapshot() (*Snapshot, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	seq := db.snaps.acquireCurrent(&db.readSeq)
	return &Snapshot{seq: seq, db: db}, nil
}

// Release frees the snapshot, allowing compaction to reclaim versions it
// pinned. Using a snapshot after Release is undefined.
func (s *Snapshot) Release() {
	if s != nil && s.released.CompareAndSwap(false, true) {
		s.db.snaps.release(s.seq)
	}
}

// Get returns the value for key as of the snapshot, or ErrNotFound.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	return s.db.getAt(s.seq, key)
}

// Has reports whether key exists as of the snapshot, without copying its value.
func (s *Snapshot) Has(key []byte) (bool, error) {
	return s.db.hasAt(s.seq, key)
}

// NewIterator returns an iterator over the whole keyspace as of the snapshot.
func (s *Snapshot) NewIterator() (Iterator, error) {
	return s.NewRangeIterator(nil, nil)
}

// NewRangeIterator returns an iterator over [start, end) as of the snapshot.
func (s *Snapshot) NewRangeIterator(start, end []byte) (Iterator, error) {
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()
	if s.db.closed {
		return nil, ErrClosed
	}
	s.db.snaps.acquire(s.seq)
	return s.db.newRangeIterator(s.seq, start, end)
}
