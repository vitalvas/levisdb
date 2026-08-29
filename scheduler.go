package levisdb

import "sync"

// scheduler serializes flush and compaction off the write path. Writers signal
// that work is pending; a single worker drains it, so at most one flush or
// compaction runs at a time (the single-engine store has one compaction stream).
// A signal that arrives while work is already queued or running is coalesced,
// and one that arrives during processing re-arms the worker so it is not lost.
type scheduler struct {
	run func() // flush + compaction for the engine

	mu      sync.Mutex
	cond    *sync.Cond
	pending bool // work awaiting the worker
	active  bool // worker currently running
	closed  bool
	wg      sync.WaitGroup // outstanding work, for graceful drain
}

func newScheduler(run func()) *scheduler {
	s := &scheduler{run: run}
	s.cond = sync.NewCond(&s.mu)
	go s.worker()
	return s
}

// Signal marks work pending. It is non-blocking and idempotent: a signal
// arriving while work is queued is coalesced, and one arriving while the worker
// is active re-arms it so work triggered during processing is not lost.
func (s *scheduler) Signal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.pending {
		return
	}
	s.pending = true
	s.wg.Add(1)
	s.cond.Signal()
}

// worker drains pending work, never running two passes concurrently.
func (s *scheduler) worker() {
	for {
		s.mu.Lock()
		for !s.closed && (!s.pending || s.active) {
			s.cond.Wait()
		}
		if s.closed && !s.pending {
			s.mu.Unlock()
			return
		}
		s.pending = false
		s.active = true
		s.mu.Unlock()

		s.run()

		s.mu.Lock()
		s.active = false
		s.wg.Done()
		s.cond.Broadcast()
		s.mu.Unlock()
	}
}

// drain blocks until all signaled work has completed.
func (s *scheduler) drain() {
	s.wg.Wait()
}

// Close drains outstanding work and stops the worker.
func (s *scheduler) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.wg.Wait()
		return
	}
	s.closed = true
	s.cond.Broadcast()
	s.mu.Unlock()
	// Marking closed first prevents a concurrent Signal from calling Add while
	// Wait is in progress. The worker continues draining before it exits.
	s.wg.Wait()
}
