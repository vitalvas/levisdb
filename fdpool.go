package levisdb

import (
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// fdPool bounds the number of table files kept open at once. Every live SSTable
// otherwise holds an open descriptor for its lifetime, which exhausts the OS
// file-descriptor limit long before a 24 TiB disk fills. Each table reads
// through an openFile handle; the pool lazily opens a descriptor on first read
// and, when the open count exceeds the limit, closes descriptors using a clock
// (second-chance) sweep, reopening on demand for a later read.
//
// Reads never touch the pool mutex: a read only sets the handle's atomic
// recently-used flag, so parallel reads do not serialize. The
// pool mutex guards the clock ring and open counter, taken only on the
// infrequent open/evict/close paths.
type fdPool struct {
	mu    sync.Mutex
	limit int
	ring  []*openFile // clock ring of open handles
	hand  int         // clock hand position
	open  int
	// now returns the current Unix-nanosecond time, stamped on each access and read
	// by the idle reaper. A field so tests can inject a deterministic clock.
	now func() int64
}

// newFDPool returns a pool allowing at most limit descriptors open at once. A
// limit <= 0 disables bounding: descriptors are opened once and kept.
func newFDPool(limit int) *fdPool {
	return &fdPool{limit: limit, now: func() int64 { return time.Now().UnixNano() }}
}

func (p *fdPool) newHandle(path string) *openFile {
	return &openFile{pool: p, path: path}
}

type openFile struct {
	pool *fdPool
	path string

	rw   sync.RWMutex
	file *os.File // nil when closed; guarded by rw

	// onEvict, if set, runs when the clock closes this descriptor (not on the
	// permanent close path). The table reader uses it to drop its lazily-parsed
	// index/filter so that metadata memory is bounded by the open-file count, not
	// by the number of live tables. Guarded by rw; called without rw held.
	onEvict func()

	used atomic.Bool // recently-used flag for the clock; set lock-free by reads
	// lastUsed is the Unix-nanosecond time of the most recent access, set lock-free
	// alongside used. The idle reaper closes descriptors untouched for longer than
	// the configured timeout; separate from used so the count-based clock keeps its
	// own second-chance semantics unchanged.
	lastUsed atomic.Int64
	inflight atomic.Int32 // reads currently in progress; the clock never evicts a busy handle
	ringIx   int          // index in pool.ring, or -1; guarded by pool.mu
	inRing   bool         // guarded by pool.mu
}

// ReadAt implements io.ReaderAt, opening the descriptor on demand and retrying
// once if it is evicted between opening and reading.
func (h *openFile) ReadAt(b []byte, off int64) (int, error) {
	for {
		if err := h.ensureOpen(); err != nil {
			return 0, err
		}
		// Mark the read in progress so the clock does not evict this descriptor
		// mid-read and force an immediate reopen. Set before re-checking file so a
		// concurrent evictor either sees inflight>0 and skips, or has already
		// nil'd file and we retry.
		h.inflight.Add(1)
		h.rw.RLock()
		f := h.file
		if f == nil {
			h.rw.RUnlock()
			h.inflight.Add(-1)
			continue // evicted after ensureOpen; reopen and retry
		}
		n, err := f.ReadAt(b, off)
		h.rw.RUnlock()
		h.inflight.Add(-1)
		return n, err
	}
}

// markUsed records an access for both the clock (second-chance bit) and the idle
// reaper (last-used time), lock-free.
func (h *openFile) markUsed() {
	h.used.Store(true)
	h.lastUsed.Store(h.pool.now())
}

// ensureOpen guarantees the descriptor is open and marks it recently used. The
// fast path (already open) sets an atomic flag and takes no pool lock.
func (h *openFile) ensureOpen() error {
	h.rw.RLock()
	if h.file != nil {
		h.rw.RUnlock()
		h.markUsed()
		return nil
	}
	h.rw.RUnlock()

	h.rw.Lock()
	if h.file != nil {
		h.rw.Unlock()
		h.markUsed()
		return nil
	}
	f, err := os.Open(h.path)
	if err != nil {
		h.rw.Unlock()
		return err
	}
	h.file = f
	h.rw.Unlock()

	h.markUsed()
	h.pool.admit(h)
	return nil
}

// close releases the descriptor permanently (table dropped).
func (h *openFile) close() error {
	h.pool.remove(h)
	h.rw.Lock()
	defer h.rw.Unlock()
	if h.file == nil {
		return nil
	}
	err := h.file.Close()
	h.file = nil
	return err
}

// admit registers a freshly-opened handle in the clock ring and evicts using a
// second-chance sweep until within the limit.
func (p *fdPool) admit(h *openFile) {
	p.mu.Lock()
	if !h.inRing {
		h.ringIx = len(p.ring)
		p.ring = append(p.ring, h)
		h.inRing = true
		p.open++
	}
	if p.limit <= 0 {
		p.mu.Unlock()
		return // bounding disabled; track the descriptor but never evict it
	}

	var victims []*openFile
	for p.open > p.limit {
		v := p.clockEvict(h)
		if v == nil {
			break // nothing evictable (e.g. only the just-admitted handle)
		}
		victims = append(victims, v)
	}
	p.mu.Unlock()

	p.closeVictims(victims)
}

// closeVictims closes descriptors removed from the ring, outside pool.mu. A reader
// may have reopened one (back inRing), so re-check under the handle lock and skip
// those. Shared by the count-based clock (admit) and the idle reaper.
func (p *fdPool) closeVictims(victims []*openFile) {
	for _, v := range victims {
		v.rw.Lock()
		p.mu.Lock()
		reopened := v.inRing
		p.mu.Unlock()
		evicted := false
		if !reopened && v.file != nil {
			v.file.Close()
			v.file = nil
			evicted = true
		}
		onEvict := v.onEvict
		v.rw.Unlock()
		// Drop the reader's cached metadata after releasing rw so the callback can
		// take its own locks without ordering against the handle lock.
		if evicted && onEvict != nil {
			onEvict()
		}
	}
}

// reapIdle removes and closes every open descriptor untouched for at least
// timeout, returning those file descriptors to the OS during quiet periods. A
// handle with a read in progress is left alone and gets another full timeout once
// idle. Count-based eviction (admit) still runs independently; this only adds
// time-based reclamation. Returns the number of descriptors closed.
func (p *fdPool) reapIdle(timeout int64) int {
	cutoff := p.now() - timeout
	var victims []*openFile
	p.mu.Lock()
	// Iterate a snapshot of the ring: removeLocked swaps the last element into the
	// removed slot, so collect first, then remove, to avoid skipping entries.
	for _, v := range append([]*openFile(nil), p.ring...) {
		if v.inflight.Load() > 0 {
			continue
		}
		if v.lastUsed.Load() > cutoff {
			continue
		}
		p.removeLocked(v)
		victims = append(victims, v)
	}
	p.mu.Unlock()

	p.closeVictims(victims)
	return len(victims)
}

// clockEvict runs a second-chance sweep and removes+returns the first handle
// whose used bit is clear (skipping the just-admitted keep), or nil if none is
// evictable. Caller holds pool.mu. Bounded to two full passes: the first clears
// all used bits, so the second is guaranteed to find a candidate.
func (p *fdPool) clockEvict(keep *openFile) *openFile {
	if len(p.ring) == 0 {
		return nil
	}
	for steps := 2 * len(p.ring); steps > 0; steps-- {
		if p.hand >= len(p.ring) {
			p.hand = 0
		}
		v := p.ring[p.hand]
		if v == keep {
			p.hand++
			continue
		}
		if v.inflight.Load() > 0 {
			// A read is in progress; do not evict it out from under the reader.
			// Leave the used bit so it keeps its second chance once idle.
			p.hand++
			continue
		}
		if v.used.Load() {
			v.used.Store(false) // second chance
			p.hand++
			continue
		}
		p.removeLocked(v) // removes v at p.hand; leaves hand pointing at its successor
		return v
	}
	return nil
}

// remove drops h from the ring (called on close).
func (p *fdPool) remove(h *openFile) {
	p.mu.Lock()
	if h.inRing {
		p.removeLocked(h)
	}
	p.mu.Unlock()
}

// removeLocked unlinks h from the ring by swapping the last element into its
// slot. Caller holds pool.mu.
func (p *fdPool) removeLocked(h *openFile) {
	last := len(p.ring) - 1
	ix := h.ringIx
	p.ring[ix] = p.ring[last]
	p.ring[ix].ringIx = ix
	p.ring = p.ring[:last]
	if p.hand > last {
		p.hand = 0
	}
	h.inRing = false
	h.ringIx = -1
	p.open--
}
