package levisdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllocatorMonotonic(t *testing.T) {
	t.Parallel()
	a := newAllocator(0)
	assert.Equal(t, uint32(1), a.Next())
	assert.Equal(t, uint32(2), a.Next())

	seen := map[uint32]bool{}
	for i := 0; i < 200; i++ {
		n := a.Next()
		assert.False(t, seen[n], "duplicate file number %d", n)
		seen[n] = true
	}
}

func TestAllocatorStopsBeforeWraparound(t *testing.T) {
	t.Parallel()
	a := newAllocator(^uint32(0) - 1)
	assert.Equal(t, ^uint32(0), a.Next())
	assert.Zero(t, a.Next())
	assert.Zero(t, a.Next(), "exhaustion must be sticky rather than wrapping into live numbers")
}
