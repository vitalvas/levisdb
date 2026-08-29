package levisdb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeBatch(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		baseSeq uint64
		entries []walEntry
	}{
		{"empty", 1, nil},
		{"single-put", 5, []walEntry{
			{Kind: walKindPut, Key: []byte("k"), Value: []byte("v")},
		}},
		{"delete-has-no-value", 10, []walEntry{
			{Kind: walKindDelete, Key: []byte("gone"), Value: []byte("ignored")},
		}},
		{"mixed", 100, []walEntry{
			{Kind: walKindPut, Key: []byte("a"), Value: []byte("1")},
			{Kind: walKindDelete, Key: []byte("b")},
			{Kind: walKindPut, Key: []byte(""), Value: []byte("")},
		}},
		{"ttl", 200, []walEntry{
			{Kind: walKindPutTTL, Key: []byte("ttl"), Value: []byte("value"), ExpiresAt: 123456789},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := encodeBatch(nil, tc.baseSeq, tc.entries)
			got, err := decodeBatch(rec)
			require.NoError(t, err)
			require.Len(t, got, len(tc.entries))
			for i, e := range tc.entries {
				assert.Equal(t, tc.baseSeq+uint64(i), got[i].Seq)
				assert.Equal(t, e.Kind, got[i].Kind)
				assert.Equal(t, e.Key, got[i].Key)
				assert.Equal(t, e.ExpiresAt, got[i].ExpiresAt)
				if e.Kind == walKindPut || e.Kind == walKindPutTTL {
					assert.Equal(t, e.Value, got[i].Value)
				} else {
					assert.Nil(t, got[i].Value)
				}
			}
		})
	}
}

func TestEncodeBatchAppendsToDst(t *testing.T) {
	t.Parallel()
	prefix := []byte("KEEP")
	rec := encodeBatch(prefix, 1, []walEntry{{Kind: walKindDelete, Key: []byte("x")}})
	assert.Equal(t, prefix, rec[:len(prefix)])
	got, err := decodeBatch(rec[len(prefix):])
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []byte("x"), got[0].Key)
}

func TestDecodeBatchErrors(t *testing.T) {
	t.Parallel()
	valid := encodeBatch(nil, 1, []walEntry{
		{Kind: walKindPut, Key: []byte("key"), Value: []byte("val")},
	})

	// rec builds a record with an 8-byte baseSeq header followed by payload.
	rec := func(payload ...byte) []byte {
		return append(make([]byte, 8), payload...)
	}

	cases := []struct {
		name string
		rec  []byte
	}{
		{"too-short", []byte{0, 1, 2}},
		{"truncated-mid-entry", valid[:len(valid)-1]},
		{"truncated-after-count", valid[:9]},
		// count uvarint unreadable (empty payload): "bad entry count".
		{"bad-entry-count", rec()},
		// count=1, then kind byte, then nothing: readBytes "bad length" on key.
		{"bad-key-length", rec(1, byte(walKindPut))},
		// count=1, kind, keylen=5 but no key bytes: "truncated data".
		{"truncated-key-data", rec(1, byte(walKindPut), 5)},
		// count=1, kind=put, keylen=0, then nothing for value length.
		{"bad-value-length", rec(1, byte(walKindPut), 0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeBatch(tc.rec)
			assert.Error(t, err)
		})
	}
}

func BenchmarkEncodeBatch(b *testing.B) {
	entries := make([]walEntry, 10)
	for i := range entries {
		entries[i] = walEntry{
			Kind:  walKindPut,
			Key:   []byte("some-key"),
			Value: []byte("some-value-payload"),
		}
	}
	var dst []byte

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = encodeBatch(dst[:0], 1, entries)
	}
	_ = dst
}

func FuzzWALRecordRoundTrip(f *testing.F) {
	f.Add([]byte("key"), []byte("value"), uint64(1), uint8(0), uint8(0))
	f.Add([]byte(""), []byte(""), uint64(1000), uint8(3), uint8(1))

	f.Fuzz(func(t *testing.T, k, v []byte, seq uint64, shardByte, kindByte uint8) {
		// Build 1-3 entries derived from the inputs and assert encode->decode
		// reconstructs them exactly. shardByte is unused since sharding was removed
		// but kept so existing fuzz corpus entries still apply.
		_ = shardByte
		base := seq % 1_000_000
		kind := walKindPut
		switch kindByte % 3 {
		case 1:
			kind = walKindDelete
		case 2:
			kind = walKindPutTTL
		}
		count := 1 + int(kindByte%3)

		entries := make([]walEntry, count)
		for i := range entries {
			entries[i] = walEntry{
				Seq:       base + uint64(i),
				Kind:      kind,
				Key:       append([]byte{byte(i)}, k...),
				Value:     v,
				ExpiresAt: 1 + int64(seq%1_000_000),
			}
			if kind != walKindPutTTL {
				entries[i].ExpiresAt = 0
			}
		}

		got, err := decodeBatch(encodeBatch(nil, entries[0].Seq, entries))
		require.NoError(t, err)
		require.Len(t, got, len(entries))
		for i, e := range entries {
			assert.Equal(t, e.Seq, got[i].Seq)
			assert.Equal(t, e.Kind, got[i].Kind)
			assert.Equal(t, e.ExpiresAt, got[i].ExpiresAt)
			assert.True(t, bytes.Equal(e.Key, got[i].Key))
			if e.Kind == walKindPut || e.Kind == walKindPutTTL {
				// A nil value encodes as len 0 and decodes as an empty slice.
				assert.True(t, bytes.Equal(e.Value, got[i].Value))
			} else {
				assert.Nil(t, got[i].Value)
			}
		}
	})
}

func FuzzDecodeBatch(f *testing.F) {
	f.Add(encodeBatch(nil, 1, []walEntry{{Kind: walKindPut, Key: []byte("k"), Value: []byte("v")}}))
	f.Add(encodeBatch(nil, 5, []walEntry{{Kind: walKindDelete, Key: []byte("d")}}))
	f.Add(encodeBatch(nil, 7, []walEntry{{Kind: walKindPutTTL, Key: []byte("ttl"), Value: []byte("v"), ExpiresAt: 99}}))
	f.Add([]byte{})
	f.Add(make([]byte, 8))

	f.Fuzz(func(t *testing.T, rec []byte) {
		// Contract: decodeBatch never panics on arbitrary WAL-record bytes; a
		// decode that succeeds must survive an encode/decode round trip.
		entries, err := decodeBatch(rec)
		if err != nil {
			return
		}
		if len(entries) == 0 {
			return
		}
		got, err := decodeBatch(encodeBatch(nil, entries[0].Seq, entries))
		require.NoError(t, err)
		require.Len(t, got, len(entries))
	})
}
