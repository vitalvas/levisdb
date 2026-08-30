package levisdb

import (
	"container/heap"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sliceSource is a test entrySource over a pre-sorted list of internal-key/value
// pairs.
type sliceSource struct {
	keys [][]byte
	vals [][]byte
	i    int
	err  error
}

func (s *sliceSource) Next() bool {
	if s.i >= len(s.keys) {
		return false
	}
	s.i++
	return true
}
func (s *sliceSource) internalKey() []byte { return s.keys[s.i-1] }
func (s *sliceSource) Value() []byte       { return s.vals[s.i-1] }
func (s *sliceSource) Error() error        { return s.err }

// srcFrom builds a source from user keys, each at the given seq, sorted by
// internal key (which the merge requires).
func srcFrom(seq uint64, pairs ...[2]string) *sliceSource {
	s := &sliceSource{}
	for _, p := range pairs {
		s.keys = append(s.keys, ikeyEncode(nil, []byte(p[0]), seq, ikeyKindSet))
		s.vals = append(s.vals, []byte(p[1]))
	}
	return s
}

func drainMerge(m *mergeIter) []string {
	var out []string
	for m.Next() {
		out = append(out, fmt.Sprintf("%s=%s", ikeyUserKey(m.internalKey()), m.Value()))
	}
	return out
}

func TestMergeIter(t *testing.T) {
	t.Parallel()
	t.Run("empty", func(t *testing.T) {
		assert.Empty(t, drainMerge(newMergeIter()))
	})

	t.Run("single source passthrough", func(t *testing.T) {
		m := newMergeIter(srcFrom(1, [2]string{"a", "1"}, [2]string{"b", "2"}))
		assert.Equal(t, []string{"a=1", "b=2"}, drainMerge(m))
	})

	t.Run("interleaves disjoint sources in order", func(t *testing.T) {
		a := srcFrom(1, [2]string{"a", "1"}, [2]string{"c", "3"})
		b := srcFrom(1, [2]string{"b", "2"}, [2]string{"d", "4"})
		m := newMergeIter(a, b)
		assert.Equal(t, []string{"a=1", "b=2", "c=3", "d=4"}, drainMerge(m))
	})

	t.Run("newest version of a key sorts first", func(t *testing.T) {
		older := srcFrom(1, [2]string{"k", "old"})
		newer := srcFrom(5, [2]string{"k", "new"})
		m := newMergeIter(older, newer)
		// Same user key, higher seq first (internal-key descending on seq).
		assert.Equal(t, []string{"k=new", "k=old"}, drainMerge(m))
	})
}

func TestMergeIterManySources(t *testing.T) {
	t.Parallel()
	// Four sources, each contributing one key; verify a fully ordered stream.
	srcs := []entrySource{
		srcFrom(1, [2]string{"d", "4"}),
		srcFrom(1, [2]string{"a", "1"}),
		srcFrom(1, [2]string{"c", "3"}),
		srcFrom(1, [2]string{"b", "2"}),
	}
	m := newMergeIter(srcs...)
	got := drainMerge(m)
	require.Equal(t, []string{"a=1", "b=2", "c=3", "d=4"}, got)
}

func TestMergeHeapPush(t *testing.T) {
	t.Parallel()
	// The heap's Push exists to satisfy heap.Interface; the merger uses
	// Init/Fix/Pop, so exercise Push directly.
	ka := ikeyEncode(nil, []byte("a"), 1, ikeyKindSet)
	kb := ikeyEncode(nil, []byte("b"), 1, ikeyKindSet)
	var h mergeHeap
	heap.Push(&h, &mergeSource{key: kb})
	heap.Push(&h, &mergeSource{key: ka})
	require.Equal(t, 2, h.Len())
	assert.Equal(t, ka, h[0].key)
	popped := heap.Pop(&h).(*mergeSource)
	assert.Equal(t, ka, popped.key)
}

func BenchmarkMergeIterNext(b *testing.B) {
	// Pre-build four sources of 1000 pre-sorted entries each outside the timer
	// so the benchmark measures the merge's per-Next cost, which should not
	// allocate after the initial heap setup.
	const perSrc = 1000
	type srcData struct {
		keys, vals [][]byte
	}
	datas := make([]srcData, 4)
	for s := range datas {
		for i := 0; i < perSrc; i++ {
			uk := []byte(fmt.Sprintf("key%08d", s+i*4))
			datas[s].keys = append(datas[s].keys, ikeyEncode(nil, uk, uint64(i+1), ikeyKindSet))
			datas[s].vals = append(datas[s].vals, []byte("v"))
		}
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		srcs := make([]entrySource, 4)
		for s := range datas {
			srcs[s] = &sliceSource{keys: datas[s].keys, vals: datas[s].vals}
		}
		m := newMergeIter(srcs...)
		count := 0
		for m.Next() {
			count++
		}
		if count != perSrc*4 {
			b.Fatalf("drained %d, want %d", count, perSrc*4)
		}
	}
}

// TestWriteMergedDetectsSameSeqValueConflict guards the compaction integrity
// check: two input tables carrying the same internal key (same user key, seq, and
// kind) but different values is an impossible-under-normal-operation anomaly that
// writeMerged must reject. A regression here is silent data corruption: the check
// compares against lastValue, which must be an owned copy - if it aliases the
// merge iterator's reused curVal buffer, the comparison is buffer-vs-itself and
// always passes, swallowing the conflict.
func TestWriteMergedDetectsSameSeqValueConflict(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	// Two sources, each one entry for key "k" at seq 5, kind Set, DIFFERENT values.
	a := srcFrom(5, [2]string{"k", "valueA"})
	b := srcFrom(5, [2]string{"k", "valueB"})
	m := newMergeIter(a, b)

	_, err := s.writeMerged(mergeWrite{
		depth:     1,
		merged:    m,
		retainSeq: uint64(1) << 62,
		cc:        testCompactionConfig(),
	})
	require.Error(t, err, "same-seq different-value entries must be rejected")
	assert.Contains(t, err.Error(), "conflicting values")
}
