package levisdb

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchedulerRunsSignaledWork(t *testing.T) {
	t.Parallel()
	var ran atomic.Int32
	s := newScheduler(func() { ran.Add(1) })

	s.Signal()
	s.drain()
	s.Close()

	assert.GreaterOrEqual(t, ran.Load(), int32(1), "signaled work must run")
}

func TestSchedulerRunsSerially(t *testing.T) {
	t.Parallel()
	// The single worker never overlaps runs.
	var inFlight atomic.Int32
	var maxSeen atomic.Int32
	s := newScheduler(func() {
		n := inFlight.Add(1)
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		inFlight.Add(-1)
	})
	for i := 0; i < 10; i++ {
		s.Signal()
	}
	s.drain()
	s.Close()
	assert.Equal(t, int32(1), maxSeen.Load(), "a single worker must never overlap runs")
}

func TestSchedulerCoalescesDuplicateSignals(t *testing.T) {
	t.Parallel()
	var count atomic.Int32
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	s := newScheduler(func() {
		select {
		case started <- struct{}{}:
		default:
		}
		<-block
		count.Add(1)
	})

	// First signal starts the worker (which blocks). Further signals while it is
	// pending coalesce; a signal while it is active re-arms it at most once.
	s.Signal()
	<-started
	for i := 0; i < 10; i++ {
		s.Signal()
	}
	close(block)
	s.drain()
	s.Close()
	// The initial run plus at most one coalesced re-run; never 11.
	assert.LessOrEqual(t, count.Load(), int32(2))
}

func TestSchedulerReArmsWhenSignaledWhileActive(t *testing.T) {
	t.Parallel()
	// A signal that arrives while the worker is running must trigger a second run,
	// so work produced during a run is not lost.
	var runs atomic.Int32
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	s := newScheduler(func() {
		if runs.Add(1) == 1 {
			select {
			case started <- struct{}{}:
			default:
			}
			<-block // hold the first run open so the next Signal lands while active
		}
	})

	s.Signal()
	<-started  // first run is active and blocked
	s.Signal() // arrives while active: must re-arm a second run
	close(block)
	s.drain()
	s.Close()
	assert.GreaterOrEqual(t, runs.Load(), int32(2), "a signal during a run must trigger another run")
}

func BenchmarkSchedulerSignal(b *testing.B) {
	s := newScheduler(func() {})
	b.Cleanup(s.Close)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Signal()
	}
}

func TestAsyncFlushPersists(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.MemtableSize = 512 })
	const entries = 120
	for i := 0; i < entries; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte{byte(i), byte(i >> 8)}, Value: []byte("v")}))
	}
	// Drain background flushes, then every key must be readable.
	db.sched.drain()
	for i := 0; i < entries; i++ {
		v, err := db.Get([]byte{byte(i), byte(i >> 8)})
		require.NoError(t, err, i)
		assert.Equal(t, []byte("v"), v)
	}
}

func TestSchedulerCloseRejectsConcurrentSignals(t *testing.T) {
	t.Parallel()
	s := newScheduler(func() {})
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				s.Signal()
			}
		}
	}()
	s.Close()
	close(stop)
	<-done
	s.Close()
}
