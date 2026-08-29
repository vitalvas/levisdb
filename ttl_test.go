package levisdb

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutTTLExpiresAndShadowsOlderValue(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) {
		o.MemtableSize = 1 << 30
	})
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("old")}))
	require.NoError(t, db.eng.Flush())
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("temporary"), TTL: 200 * time.Millisecond}))

	value, err := db.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("temporary"), value)

	require.Eventually(t, func() bool {
		_, err := db.Get([]byte("key"))
		return errors.Is(err, ErrNotFound)
	}, 2*time.Second, 5*time.Millisecond)
	has, err := db.Has([]byte("key"))
	require.NoError(t, err)
	assert.False(t, has, "expiration must not reveal the older table value")
}

func TestTTLIsPersistedAcrossReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value"), TTL: 100 * time.Millisecond}))
	require.NoError(t, db.Close())

	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()
	require.Eventually(t, func() bool {
		_, err := db.Get([]byte("key"))
		return errors.Is(err, ErrNotFound)
	}, 2*time.Second, 5*time.Millisecond)
}

func TestWALRecoveryDoesNotRestartTTL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1 << 30
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value"), TTL: 100 * time.Millisecond}))
	db.crash()
	time.Sleep(150 * time.Millisecond)

	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Get([]byte("key"))
	assert.ErrorIs(t, err, ErrNotFound, "replay must use the original absolute deadline")
}

func TestTTLIteratorUsesCreationTime(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) {
		o.MemtableSize = 1 << 30
	})
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value"), TTL: 200 * time.Millisecond}))
	it, err := db.NewIterator()
	require.NoError(t, err)
	time.Sleep(250 * time.Millisecond)
	require.True(t, it.Next(), "an iterator keeps the wall-clock view captured at creation")
	assert.Equal(t, []byte("key"), it.Key())
	assert.Equal(t, []byte("value"), it.Value())
	require.NoError(t, it.Close())

	it, err = db.NewIterator()
	require.NoError(t, err)
	assert.False(t, it.Next(), "a later iterator must omit the expired value")
	require.NoError(t, it.Close())
}

func TestTTLValidationIsAtomic(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	assert.ErrorIs(t, db.Put(PutOptions{Key: []byte("bad"), Value: []byte("value"), TTL: -time.Second}), ErrInvalidTTL)
	assert.ErrorIs(t, db.Put(PutOptions{Key: []byte("overflow"), Value: []byte("value"), TTL: time.Duration(math.MaxInt64)}), ErrInvalidTTL)

	var batch Batch
	batch.Put(PutOptions{Key: []byte("valid"), Value: []byte("value")})
	batch.Put(PutOptions{Key: []byte("invalid"), Value: []byte("value"), TTL: -1})
	assert.ErrorIs(t, db.Write(&batch), ErrInvalidTTL)
	_, err := db.Get([]byte("valid"))
	assert.ErrorIs(t, err, ErrNotFound, "a rejected batch must publish no prefix")
}

func TestCompactionReclaimsExpiredTTLAndShadowedValue(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	s.Put(1, []byte("key"), []byte("old"))
	s.putTTL(2, []byte("key"), []byte("expired"), time.Now().Add(-time.Second).UnixNano())
	require.NoError(t, s.Flush())
	require.NoError(t, s.CompactAll(maxIKeySeq, testCompactionConfig()))
	assert.Empty(t, s.Tables())
}

func TestCompactionPreservesTTLForLiveIteratorTime(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	const expiresAt = int64(10_000)
	s.putTTL(1, []byte("key"), []byte("value"), expiresAt)
	require.NoError(t, s.Flush())

	cc := testCompactionConfig()
	cc.ExpireBefore = expiresAt - 1
	require.NoError(t, s.CompactAll(maxIKeySeq, cc))
	value, found, deleted, err := s.getAtTime(1, []byte("key"), expiresAt-1)
	require.NoError(t, err)
	assert.True(t, found)
	assert.False(t, deleted)
	assert.Equal(t, []byte("value"), value)

	cc.ExpireBefore = expiresAt
	require.NoError(t, s.CompactAll(maxIKeySeq, cc))
	assert.Empty(t, s.Tables(), "TTL can be reclaimed once every iterator view sees it expired")
}

func TestTableTTLUsesPersistedAbsoluteDeadline(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	const expiresAt = int64(10_000)
	s.putTTL(1, []byte("key"), []byte("value"), expiresAt)
	require.NoError(t, s.Flush())

	value, found, deleted, err := s.getAtTime(1, []byte("key"), expiresAt-1)
	require.NoError(t, err)
	assert.True(t, found)
	assert.False(t, deleted)
	assert.Equal(t, []byte("value"), value)

	_, found, deleted, err = s.getAtTime(1, []byte("key"), expiresAt)
	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, deleted)
}

type ttlObserver struct{ entries []WALEntry }

func (o *ttlObserver) Observe(entries []WALEntry) {
	o.entries = append(o.entries, entries...)
}

func TestWALObserverReceivesTTL(t *testing.T) {
	t.Parallel()
	observer := &ttlObserver{}
	db := openTestDB(t, func(o *Options) { o.WALObserver = observer })
	const ttl = time.Minute
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value"), TTL: ttl}))
	require.Len(t, observer.entries, 1)
	assert.Equal(t, EntryPut, observer.entries[0].Kind)
	assert.Positive(t, observer.entries[0].TTL)
	assert.LessOrEqual(t, observer.entries[0].TTL, ttl)
}
