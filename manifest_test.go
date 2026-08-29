package levisdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManifestEditEncodeDecode(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		edit manifestEdit
	}{
		{"empty", manifestEdit{}},
		{"identity", manifestEdit{HasIdentity: true}},
		{"last-seq", manifestEdit{HasLastSeq: true, LastSeq: 999}},
		{"add-delete", manifestEdit{
			Added: []manifestTableInfo{
				{Num: 7, Depth: 2, Size: 4096},
				{Num: 8, Depth: 0, Size: 0},
			},
			Deleted: []manifestTableRef{{Num: 5}},
		}},
		{"add-with-key-bounds", manifestEdit{
			Added: []manifestTableInfo{
				{
					Num:        9,
					Depth:      1,
					Size:       512,
					MinKey:     []byte("aaa"),
					MaxKey:     []byte("zzz"),
					Entries:    100,
					Tombstones: 40,
				},
				{Num: 10, Depth: 1, Size: 256}, // nil bounds/zero counts round-trip
			},
		}},
		{"full", manifestEdit{
			HasIdentity: true,
			HasLastSeq:  true,
			LastSeq:     42,
			Added: []manifestTableInfo{{
				Num:   1,
				Depth: 1,
				Size:  100,
			}},
			Deleted: []manifestTableRef{{
				Num: 2,
			}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeEdit(tc.edit.encode())
			require.NoError(t, err)
			assert.Equal(t, tc.edit.HasIdentity, got.HasIdentity)
			assert.Equal(t, tc.edit.HasLastSeq, got.HasLastSeq)
			assert.Equal(t, tc.edit.LastSeq, got.LastSeq)
			assert.Equal(t, tc.edit.Added, got.Added)
			assert.Equal(t, tc.edit.Deleted, got.Deleted)
		})
	}
}

func TestDecodeEditUnknownTag(t *testing.T) {
	t.Parallel()
	_, err := decodeEdit([]byte{0x7f})
	assert.Error(t, err)
}

func TestManifestWriteAndReplay(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "MANIFEST-00000001")

	w, err := createManifest(path)
	require.NoError(t, err)
	require.NoError(t, w.append(&manifestEdit{
		HasLastSeq: true,
		LastSeq:    5,
		Added:      []manifestTableInfo{{Num: 10, Depth: 0, Size: 512}},
	}))
	require.NoError(t, w.append(&manifestEdit{
		HasLastSeq: true,
		LastSeq:    9,
		Added:      []manifestTableInfo{{Num: 11, Depth: 0, Size: 256}},
		Deleted:    []manifestTableRef{{Num: 10}},
	}))
	require.NoError(t, w.Close())

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	st, err := replayManifestFile(f)
	require.NoError(t, err)
	assert.Equal(t, uint64(9), st.LastSeq)

	// Table 10 was deleted, only 11 survives.
	require.Len(t, st.Tables, 1)
	assert.Equal(t, uint32(11), st.Tables[0].Num)
}

func TestDecodeEditErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		rec  []byte
	}{
		// tagLastSeq with no following uvarint.
		{"bad-last-seq", []byte{tagLastSeq}},
		// tagAdd with an incomplete table record.
		{"bad-table-record", []byte{tagAdd}},
		// tagDelete with no following uvarints.
		{"bad-delete", []byte{tagDelete}},
		{"missing-end-tag", []byte{tagLastSeq, 1}},
		{"duplicate-identity", []byte{tagIdentity, tagIdentity, tagEnd}},
		{"duplicate-last-seq", []byte{tagLastSeq, 1, tagLastSeq, 2, tagEnd}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeEdit(tc.rec)
			assert.Error(t, err)
		})
	}
}

func TestCreateManifestOpenError(t *testing.T) {
	t.Parallel()
	// Directory does not exist, so OpenFile fails.
	_, err := createManifest(filepath.Join(t.TempDir(), "nope", "MANIFEST-1"))
	assert.Error(t, err)
}

func TestManifestAppendAfterClose(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "MANIFEST-00000001")
	w, err := createManifest(path)
	require.NoError(t, err)

	// Close the underlying file so a subsequent append's Sync (or Write) fails.
	require.NoError(t, w.Close())
	assert.Error(t, w.append(&manifestEdit{HasLastSeq: true, LastSeq: 1}))
}

func TestReplayManifestFileEmpty(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "empty")
	require.NoError(t, os.WriteFile(path, nil, 0o644))

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	_, err = replayManifestFile(f)
	assert.ErrorContains(t, err, "missing identity")
}

func BenchmarkManifestEncodeDecode(b *testing.B) {
	edit := manifestEdit{HasLastSeq: true, LastSeq: 12345}
	for i := 0; i < 10; i++ {
		edit.Added = append(edit.Added, manifestTableInfo{
			Num:   uint32(i * 7),
			Depth: i % 4,
			Size:  int64(i) * 4096,
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc := edit.encode()
		if _, err := decodeEdit(enc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkManifestReplay(b *testing.B) {
	path := filepath.Join(b.TempDir(), "MANIFEST-bench")
	w, err := createManifest(path)
	require.NoError(b, err)
	for i := 0; i < 200; i++ {
		require.NoError(b, w.append(&manifestEdit{
			HasLastSeq: true,
			LastSeq:    uint64(i),
			Added:      []manifestTableInfo{{Num: uint32(i + 1), Depth: i % 4, Size: int64(i) * 512}},
		}))
	}
	require.NoError(b, w.Close())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		f, err := os.Open(path)
		require.NoError(b, err)
		b.StartTimer()

		if _, err := replayManifestFile(f); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		require.NoError(b, f.Close())
		b.StartTimer()
	}
}

func TestReplayManifestRejectsImpossibleTableHistory(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		edits   []manifestEdit
		wantErr string
	}{
		{
			name:    "reserved zero number",
			edits:   []manifestEdit{{Added: []manifestTableInfo{{Num: 0, Size: 100}}}},
			wantErr: "number 0 is reserved",
		},
		{
			name: "duplicate number",
			edits: []manifestEdit{
				{Added: []manifestTableInfo{{Num: 7, Size: 100}}},
				{Added: []manifestTableInfo{{Num: 7, Size: 100}}},
			},
			wantErr: "file number 7 reused",
		},
		{
			name:    "delete unknown table",
			edits:   []manifestEdit{{Deleted: []manifestTableRef{{Num: 7}}}},
			wantErr: "delete of unknown table",
		},
		{
			name: "reuse after delete",
			edits: []manifestEdit{
				{Added: []manifestTableInfo{{Num: 7, Size: 100}}},
				{Deleted: []manifestTableRef{{Num: 7}}},
				{Added: []manifestTableInfo{{Num: 7, Size: 100}}},
			},
			wantErr: "file number 7 reused",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "MANIFEST-test")
			w, err := createManifest(path)
			require.NoError(t, err)
			for i := range tt.edits {
				require.NoError(t, w.append(&tt.edits[i]))
			}
			require.NoError(t, w.Close())
			f, err := os.Open(path)
			require.NoError(t, err)
			defer f.Close()
			_, err = replayManifestFile(f)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func FuzzDecodeEdit(f *testing.F) {
	// Seed with valid encoded edits.
	f.Add((&manifestEdit{HasIdentity: true}).encode())
	f.Add((&manifestEdit{Added: []manifestTableInfo{{Num: 2, Depth: 0, Size: 9}}}).encode())
	f.Add([]byte{})
	f.Add([]byte{0x02, 0x00})

	f.Fuzz(func(t *testing.T, rec []byte) {
		// Contract: decodeEdit never panics on arbitrary manifest-record bytes;
		// a decode that succeeds must re-encode/re-decode consistently.
		e, err := decodeEdit(rec)
		if err != nil {
			return
		}
		got, err := decodeEdit(e.encode())
		require.NoError(t, err)
		assert.Equal(t, e, got)
	})
}
