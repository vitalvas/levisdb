package levisdb

import (
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
	expiresAt := time.Now().Add(time.Hour).Unix()
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("temporary"), ExpiresAt: expiresAt}))

	// Live before the deadline via the public read path.
	value, err := db.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("temporary"), value)

	// After the deadline the newest value is gone and, crucially, the older shadowed
	// table value is not revealed. Checked at an explicit clock past the deadline
	// rather than waiting on the wall clock.
	value, found, deleted, err := db.eng.getAtTime(db.LatestSeq(), []byte("key"), expiresAt)
	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, deleted, "expired TTL behaves as a tombstone")
	assert.Nil(t, value, "expiration must not reveal the older table value")
}

func TestTTLIsPersistedAcrossReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	db, err := Open(opts)
	require.NoError(t, err)
	expiresAt := time.Now().Add(time.Hour).Unix()
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value"), ExpiresAt: expiresAt}))
	require.NoError(t, db.Close())

	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()
	// The deadline survived the flush+reopen: live before it, expired after. Checked
	// at explicit clocks rather than waiting on the wall clock.
	_, found, deleted, err := db.eng.getAtTime(db.LatestSeq(), []byte("key"), expiresAt-1)
	require.NoError(t, err)
	assert.True(t, found)
	assert.False(t, deleted)
	_, _, deleted, err = db.eng.getAtTime(db.LatestSeq(), []byte("key"), expiresAt)
	require.NoError(t, err)
	assert.True(t, deleted, "the persisted deadline, not a restarted TTL, governs expiry")
}

func TestWALRecoveryDoesNotRestartTTL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1 << 30
	db, err := Open(opts)
	require.NoError(t, err)
	expiresAt := time.Now().Add(time.Hour).Unix()
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value"), ExpiresAt: expiresAt}))
	db.crash()

	db, err = Open(opts)
	require.NoError(t, err)
	defer db.Close()
	// Replay keeps the original absolute deadline rather than restarting it from
	// recovery time: the value is still live just before it and expired at it.
	_, found, deleted, err := db.eng.getAtTime(db.LatestSeq(), []byte("key"), expiresAt-1)
	require.NoError(t, err)
	assert.True(t, found)
	assert.False(t, deleted)
	_, _, deleted, err = db.eng.getAtTime(db.LatestSeq(), []byte("key"), expiresAt)
	require.NoError(t, err)
	assert.True(t, deleted, "replay must not restart the TTL")
}

func TestTTLIteratorUsesCreationTime(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) {
		o.MemtableSize = 1 << 30
	})
	expiresAt := time.Now().Add(time.Hour).Unix()
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value"), ExpiresAt: expiresAt}))

	// The public iterator captures time.Now() at creation (well before the deadline),
	// so it keeps seeing the value; no wall-clock wait is needed to prove this.
	it, err := db.NewIterator()
	require.NoError(t, err)
	require.True(t, it.Next(), "an iterator keeps the wall-clock view captured at creation")
	assert.Equal(t, []byte("key"), it.Key())
	assert.Equal(t, []byte("value"), it.Value())
	require.NoError(t, it.Close())

	// An iterator whose view is past the deadline omits the value. Created with an
	// explicit post-deadline read time so the check is deterministic and instant.
	src := db.eng.newRangeIteratorAt(db.LatestSeq(), nil, nil, expiresAt)
	assert.False(t, src.Next(), "a view past the deadline must omit the expired value")
	require.NoError(t, src.Close())
}

func TestTTLValidationIsAtomic(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, nil)
	assert.ErrorIs(t, db.Put(PutOptions{Key: []byte("bad"), Value: []byte("value"), ExpiresAt: -1}), ErrInvalidTTL)
	assert.ErrorIs(t, db.Put(PutOptions{Key: []byte("past"), Value: []byte("value"), ExpiresAt: time.Now().Add(-time.Second).Unix()}), ErrInvalidTTL)

	var batch Batch
	batch.Put(PutOptions{Key: []byte("valid"), Value: []byte("value")})
	batch.Put(PutOptions{Key: []byte("invalid"), Value: []byte("value"), ExpiresAt: -1})
	assert.ErrorIs(t, db.Write(&batch), ErrInvalidTTL)
	_, err := db.Get([]byte("valid"))
	assert.ErrorIs(t, err, ErrNotFound, "a rejected batch must publish no prefix")
}

func TestCompactionReclaimsExpiredTTLAndShadowedValue(t *testing.T) {
	t.Parallel()
	s := newTestEngine(t, 1<<20)
	s.Put(1, []byte("key"), []byte("old"))
	s.putTTL(2, []byte("key"), []byte("expired"), time.Now().Add(-time.Second).Unix())
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
	expiresAt := time.Now().Add(time.Minute).Unix()
	require.NoError(t, db.Put(PutOptions{Key: []byte("key"), Value: []byte("value"), ExpiresAt: expiresAt}))
	require.Len(t, observer.entries, 1)
	assert.Equal(t, EntryPut, observer.entries[0].Kind)
	assert.Equal(t, expiresAt, observer.entries[0].ExpiresAt)
}
