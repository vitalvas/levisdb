package levisdb

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchedulerRunsSignaledWork(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	ran := map[int]int{}
	s := newScheduler(2, func(shard int) {
		mu.Lock()
		ran[shard]++
		mu.Unlock()
	})

	for i := 0; i < 5; i++ {
		s.Signal(i)
	}
	s.drain()
	s.Close()

	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < 5; i++ {
		assert.Equal(t, 1, ran[i], "shard %d", i)
	}
}

func TestSchedulerConcurrencyOne(t *testing.T) {
	t.Parallel()
	// With one worker, no two runs overlap.
	var inFlight atomic.Int32
	var maxSeen atomic.Int32
	s := newScheduler(1, func(int) {
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
		s.Signal(i)
	}
	s.drain()
	s.Close()
	assert.Equal(t, int32(1), maxSeen.Load(), "concurrency 1 must never overlap")
}

func TestSchedulerCoalescesDuplicateSignals(t *testing.T) {
	t.Parallel()
	var count atomic.Int32
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	s := newScheduler(1, func(int) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-block
		count.Add(1)
	})

	// First signal starts the worker (which blocks). Further signals for the
	// same shard while it is queued coalesce.
	s.Signal(0)
	<-started
	for i := 0; i < 10; i++ {
		s.Signal(0) // dropped: shard 0 is active, not re-queued here
	}
	close(block)
	s.drain()
	s.Close()
	// The initial run plus at most one coalesced re-run; never 11.
	assert.LessOrEqual(t, count.Load(), int32(2))
}

func TestSchedulerSkipsActiveShardForOthers(t *testing.T) {
	t.Parallel()
	// Two workers. Shard 0 starts and blocks. It is re-signaled (re-queued) while
	// active, then shard 1 is signaled. The idle worker must skip the active
	// shard 0 in takeRunnable and pick up shard 1.
	var mu sync.Mutex
	ran := map[int]int{}
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	ran1 := make(chan struct{}, 1)
	s := newScheduler(2, func(shard int) {
		if shard == 0 {
			select {
			case started <- struct{}{}:
			default:
			}
			<-block
		}
		mu.Lock()
		ran[shard]++
		mu.Unlock()
		if shard == 1 {
			select {
			case ran1 <- struct{}{}:
			default:
			}
		}
	})

	s.Signal(0)
	<-started   // shard 0 is now active and blocked
	s.Signal(0) // re-queued: shard 0 in queue but active, must be skipped
	s.Signal(1) // the idle worker must take shard 1 past the active shard 0
	<-ran1      // shard 1 ran while shard 0 was still active

	close(block)
	s.drain()
	s.Close()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, ran[1])
	assert.GreaterOrEqual(t, ran[0], 1)
}

func BenchmarkSchedulerSignal(b *testing.B) {
	const shards = 32
	s := newScheduler(1, func(int) {})
	b.Cleanup(s.Close)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Signal(i % shards)
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
	s := newScheduler(2, func(int) {})
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				s.Signal(i % 4)
			}
		}
	}()
	s.Close()
	close(stop)
	<-done
	s.Close()
}
