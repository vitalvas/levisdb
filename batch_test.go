package levisdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchResetAndReuse(t *testing.T) {
	t.Parallel()
	var b Batch
	b.Put(PutOptions{Key: []byte("a"), Value: []byte("1")})
	b.Delete([]byte("b"))
	require.Equal(t, 2, b.Len())

	b.Reset()
	assert.Equal(t, 0, b.Len())

	// Reusable after reset.
	b.Put(PutOptions{Key: []byte("c"), Value: []byte("3")})
	assert.Equal(t, 1, b.Len())
	assert.Equal(t, EntryPut, b.ops[0].kind)
}

func TestWalKindMapping(t *testing.T) {
	t.Parallel()
	assert.Equal(t, walKindPut, walKind(EntryPut))
	assert.Equal(t, walKindDelete, walKind(EntryDelete))
}

func TestBatchOwnsInputSlices(t *testing.T) {
	t.Parallel()
	key := []byte("key")
	value := []byte("value")
	var b Batch
	b.Put(PutOptions{Key: key, Value: value})
	b.Delete(key)
	key[0] = 'X'
	value[0] = 'X'

	assert.Equal(t, []byte("key"), b.ops[0].key)
	assert.Equal(t, []byte("value"), b.ops[0].value)
	assert.Equal(t, []byte("key"), b.ops[1].key)
}

func TestBatchRetainsPutTTL(t *testing.T) {
	var b Batch
	b.Put(PutOptions{Key: []byte("key"), Value: []byte("value"), TTL: time.Minute})
	require.Len(t, b.ops, 1)
	assert.Equal(t, time.Minute, b.ops[0].ttl)
}
