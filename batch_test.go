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

func TestBatchRetainsInputSlicesByReference(t *testing.T) {
	t.Parallel()
	// Batch keeps the caller's slices by reference (no copy). The contract is that
	// the caller must not mutate them until Write returns.
	key := []byte("key")
	value := []byte("value")
	var b Batch
	b.Put(PutOptions{Key: key, Value: value})
	assert.Same(t, &key[0], &b.ops[0].key[0], "key retained by reference, not copied")
	assert.Same(t, &value[0], &b.ops[0].value[0], "value retained by reference, not copied")
}

func TestBatchWriteThenCallerMayMutate(t *testing.T) {
	t.Parallel()
	// The durable contract: after Write returns, the key/value have been copied
	// into the WAL and memtable, so the caller may reuse or mutate its buffers
	// without affecting stored data.
	db := openTestDB(t, nil)
	key := []byte("key")
	value := append([]byte(nil), "value"...)
	var b Batch
	b.Put(PutOptions{Key: key, Value: value})
	require.NoError(t, db.Write(&b))

	value[0] = 'X' // mutate after Write; stored value must be unaffected
	got, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), got)
}

func TestBatchRetainsPutTTL(t *testing.T) {
	var b Batch
	b.Put(PutOptions{Key: []byte("key"), Value: []byte("value"), TTL: time.Minute})
	require.Len(t, b.ops, 1)
	assert.Equal(t, time.Minute, b.ops[0].ttl)
}
