package levisdb

import "sync"

// scheduler serializes shard flush and compaction across the database. Writers
// signal a shard as dirty; a fixed pool of workers drains dirty shards, running
// at most CompactionConcurrency at once so a single-spindle deployment sees one
// sequential compaction stream (concurrency 1).
type scheduler struct {
	run     func(shard int) // flush + compaction for one shard
	workers int

	mu      sync.Mutex
	cond    *sync.Cond
	dirty   map[int]bool // shards awaiting work
	queue   []int        // FIFO order of dirty shards for fairness
	active  map[int]bool // shards currently being processed
	closed  bool
	pending sync.WaitGroup // outstanding queued work, for graceful drain
}

func newScheduler(workers int, run func(int)) *scheduler {
	s := &scheduler{
		run:     run,
		workers: workers,
		dirty:   map[int]bool{},
		active:  map[int]bool{},
	}
	s.cond = sync.NewCond(&s.mu)
	for i := 0; i < workers; i++ {
		go s.worker()
	}
	return s
}

// Signal marks a shard dirty. It is non-blocking and idempotent: a shard
// already queued or active is coalesced, and if it is currently active it is
// re-queued so work triggered during processing is not lost.
func (s *scheduler) Signal(shard int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.dirty[shard] {
		return
	}
	s.dirty[shard] = true
	s.queue = append(s.queue, shard)
	s.pending.Add(1)
	s.cond.Signal()
}

// worker pulls dirty shards and runs work for them, never two workers on the
// same shard at once.
func (s *scheduler) worker() {
	for {
		s.mu.Lock()
		for !s.closed && !s.hasRunnable() {
			s.cond.Wait()
		}
		if s.closed && !s.hasRunnable() {
			s.mu.Unlock()
			return
		}
		shard := s.takeRunnable()
		s.mu.Unlock()

		s.run(shard)

		s.mu.Lock()
		delete(s.active, shard)
		s.pending.Done()
		// Another worker may be waiting for this shard to free up.
		s.cond.Broadcast()
		s.mu.Unlock()
	}
}

// hasRunnable reports whether a queued shard is not currently active.
func (s *scheduler) hasRunnable() bool {
	for _, shard := range s.queue {
		if !s.active[shard] {
			return true
		}
	}
	return false
}

// takeRunnable removes and returns the first queued shard not already active.
func (s *scheduler) takeRunnable() int {
	for i, shard := range s.queue {
		if s.active[shard] {
			continue
		}
		s.queue = append(s.queue[:i], s.queue[i+1:]...)
		delete(s.dirty, shard)
		s.active[shard] = true
		return shard
	}
	return -1
}

// Drain blocks until all signaled work has completed.
func (s *scheduler) drain() {
	s.pending.Wait()
}

// Close drains outstanding work and stops the workers.
func (s *scheduler) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.pending.Wait()
		return
	}
	s.closed = true
	s.cond.Broadcast()
	s.mu.Unlock()
	// Marking closed first prevents a concurrent Signal from calling Add while
	// Wait is in progress. Workers continue draining the queue before exiting.
	s.pending.Wait()
}
