package levisdb

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeTempFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, data, 0o644))
	return p
}

func TestFDPoolBoundsOpenDescriptors(t *testing.T) {
	dir := t.TempDir()
	const files = 50
	const limit = 4
	pool := newFDPool(limit)

	handles := make([]*openFile, files)
	contents := make([][]byte, files)
	for i := range handles {
		contents[i] = []byte(fmt.Sprintf("file-%03d-content", i))
		p := writeTempFile(t, dir, fmt.Sprintf("f%03d", i), contents[i])
		handles[i] = pool.newHandle(p)
	}

	// Read every file in a shuffled-ish order; reads must succeed even though
	// far more files exist than the descriptor limit.
	for round := 0; round < 3; round++ {
		for i := 0; i < files; i++ {
			buf := make([]byte, len(contents[i]))
			n, err := handles[i].ReadAt(buf, 0)
			require.NoError(t, err, "file %d round %d", i, round)
			assert.Equal(t, contents[i], buf[:n])
		}
	}

	// At most `limit` descriptors are open at once.
	pool.mu.Lock()
	open := pool.open
	pool.mu.Unlock()
	assert.LessOrEqual(t, open, limit, "open descriptors must stay within the limit")

	for _, h := range handles {
		require.NoError(t, h.close())
	}
	pool.mu.Lock()
	assert.Equal(t, 0, pool.open, "all descriptors closed")
	pool.mu.Unlock()
}

func TestFDPoolConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	const files = 30
	pool := newFDPool(3)
	handles := make([]*openFile, files)
	want := make([][]byte, files)
	for i := range handles {
		want[i] = []byte(fmt.Sprintf("payload-%d", i))
		handles[i] = pool.newHandle(writeTempFile(t, dir, fmt.Sprintf("f%d", i), want[i]))
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < 200; r++ {
				i := r % files
				buf := make([]byte, len(want[i]))
				n, err := handles[i].ReadAt(buf, 0)
				assert.NoError(t, err)
				assert.Equal(t, want[i], buf[:n])
			}
		}()
	}
	wg.Wait()

	pool.mu.Lock()
	assert.LessOrEqual(t, pool.open, 3)
	pool.mu.Unlock()
}

func TestFDPoolUnbounded(t *testing.T) {
	dir := t.TempDir()
	pool := newFDPool(-1) // disabled bounding
	data := []byte("hello")
	h := pool.newHandle(writeTempFile(t, dir, "x", data))
	buf := make([]byte, len(data))
	n, err := h.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, data, buf[:n])
	pool.mu.Lock()
	assert.Equal(t, 1, pool.open)
	pool.mu.Unlock()
	require.NoError(t, h.close())
	pool.mu.Lock()
	assert.Zero(t, pool.open)
	pool.mu.Unlock()
}

func TestFDPoolReadError(t *testing.T) {
	pool := newFDPool(2)
	h := pool.newHandle(filepath.Join(t.TempDir(), "does-not-exist"))
	_, err := h.ReadAt(make([]byte, 4), 0)
	assert.Error(t, err)
}

func FuzzFDPoolOps(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 0, 1})
	f.Add([]byte{5, 5, 5, 200, 5})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, script []byte) {
		dir := t.TempDir()
		const files = 8
		const limit = 3
		pool := newFDPool(limit)
		handles := make([]*openFile, files)
		contents := make([][]byte, files)
		for i := range handles {
			contents[i] = []byte{byte(i), byte(i + 1), byte(i + 2)}
			p := filepath.Join(dir, string(rune('a'+i)))
			require.NoError(t, os.WriteFile(p, contents[i], 0o644))
			handles[i] = pool.newHandle(p)
		}

		// Each script byte drives a read of some handle; the high bit closes it.
		// Reads must return correct bytes, the pool must never exceed the limit,
		// and nothing may panic.
		for _, op := range script {
			idx := int(op) % files
			if op&0x80 != 0 {
				require.NoError(t, handles[idx].close())
				continue
			}
			buf := make([]byte, len(contents[idx]))
			n, err := handles[idx].ReadAt(buf, 0)
			require.NoError(t, err)
			assert.Equal(t, contents[idx], buf[:n])

			pool.mu.Lock()
			open := pool.open
			pool.mu.Unlock()
			assert.LessOrEqual(t, open, limit)
		}
		for _, h := range handles {
			_ = h.close()
		}
	})
}

func BenchmarkFDPoolReadHit(b *testing.B) {
	dir := b.TempDir()
	p := filepath.Join(dir, "f")
	require.NoError(b, os.WriteFile(p, make([]byte, 4096), 0o644))
	pool := newFDPool(16)
	h := pool.newHandle(p)
	buf := make([]byte, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := h.ReadAt(buf, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFDPoolReadParallel(b *testing.B) {
	dir := b.TempDir()
	const files = 32
	pool := newFDPool(16) // fewer than files, so reads trigger clock evictions
	handles := make([]*openFile, files)
	for i := range handles {
		p := filepath.Join(dir, fmt.Sprintf("f%02d", i))
		require.NoError(b, os.WriteFile(p, make([]byte, 4096), 0o644))
		handles[i] = pool.newHandle(p)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		buf := make([]byte, 128)
		i := 0
		for pb.Next() {
			if _, err := handles[i%files].ReadAt(buf, 0); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

func TestFDPoolDoesNotEvictInflightHandle(t *testing.T) {
	dir := t.TempDir()
	const limit = 2
	pool := newFDPool(limit)

	h := make([]*openFile, 3)
	for i := range h {
		h[i] = pool.newHandle(writeTempFile(t, dir, fmt.Sprintf("f%d", i), []byte("data")))
		buf := make([]byte, 4)
		_, err := h[i].ReadAt(buf, 0) // opens and admits each; evicts to stay within limit
		require.NoError(t, err)
	}

	// Mark h[0] as having a read in progress and clear its used bit so it would
	// otherwise be the prime eviction candidate.
	h[0].inflight.Add(1)
	h[0].used.Store(false)
	defer h[0].inflight.Add(-1)

	pool.mu.Lock()
	// Force an eviction sweep; the in-flight handle must be skipped if present.
	inRingBefore := h[0].inRing
	victim := pool.clockEvict(nil)
	pool.mu.Unlock()

	if inRingBefore {
		assert.NotEqual(t, h[0], victim, "a handle with an in-flight read must not be evicted")
	}
}
