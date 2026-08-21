package levisdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJournalRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		records [][]byte
	}{
		{"empty-slice", [][]byte{{}}},
		{"single", [][]byte{[]byte("hello")}},
		{"multiple", [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")}},
		{"spans-blocks", [][]byte{bytes.Repeat([]byte("x"), blockSize+123)}},
		{"exact-block-fragment", [][]byte{bytes.Repeat([]byte("y"), blockSize-headerSize)}},
		{"many-small", func() [][]byte {
			out := make([][]byte, 200)
			for i := range out {
				out[i] = bytes.Repeat([]byte{byte(i)}, i%300)
			}
			return out
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := newJournalWriter(&buf)
			for _, rec := range tc.records {
				require.NoError(t, w.Write(rec))
			}
			require.NoError(t, w.Flush())

			r := newJournalReader(&buf)
			for _, want := range tc.records {
				got, err := r.Next()
				require.NoError(t, err)
				if len(want) == 0 {
					assert.Empty(t, got)
				} else {
					assert.Equal(t, want, got)
				}
			}
			_, err := r.Next()
			assert.Equal(t, io.EOF, err)
		})
	}
}

func TestJournalFlushEmpty(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	w := newJournalWriter(&buf)
	require.NoError(t, w.Flush())
	assert.Zero(t, buf.Len())
}

func TestJournalFlushPreservesBlockAlignment(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	w := newJournalWriter(&buf)
	first := bytes.Repeat([]byte("a"), 10_000)
	second := bytes.Repeat([]byte("b"), 30_000)
	require.NoError(t, w.Write(first))
	require.NoError(t, w.Flush())
	require.NoError(t, w.Write(second))
	require.NoError(t, w.Flush())

	r := newJournalReader(bytes.NewReader(buf.Bytes()))
	got, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, first, got)
	got, err = r.Next()
	require.NoError(t, err)
	assert.Equal(t, second, got)
	_, err = r.Next()
	assert.ErrorIs(t, err, io.EOF)
}

// TestJournalHeaderBoundaryRecord is a regression test: a record that leaves the
// writer exactly headerSize bytes before a block boundary must not cause the next
// record to be framed as a zero-length chunkFirst (which the reader would treat
// as corrupt). Both records must round-trip cleanly.
func TestJournalHeaderBoundaryRecord(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	w := newJournalWriter(&buf)

	// A first record of blockSize-2*headerSize bytes frames as one chunkFull that
	// consumes headerSize + (blockSize-2*headerSize) bytes, leaving blockOffset at
	// exactly blockSize-headerSize.
	first := bytes.Repeat([]byte("a"), blockSize-2*headerSize)
	require.NoError(t, w.Write(first))
	// The next non-empty record starts with only headerSize bytes left in the
	// block. It must roll to a fresh block, not emit an empty first chunk.
	second := []byte("hello")
	require.NoError(t, w.Write(second))
	require.NoError(t, w.Flush())

	r := newJournalReader(bytes.NewReader(buf.Bytes()))
	got, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, first, got, "first record round-trips")
	got, err = r.Next()
	require.NoError(t, err, "record at the header boundary must not read as corrupt")
	assert.Equal(t, second, got, "second record round-trips")
	_, err = r.Next()
	assert.ErrorIs(t, err, io.EOF)
}

func TestJournalReaderEmptyStream(t *testing.T) {
	t.Parallel()
	r := newJournalReader(bytes.NewReader(nil))
	_, err := r.Next()
	assert.Equal(t, io.EOF, err)
}

func TestJournalTornTail(t *testing.T) {
	t.Parallel()
	// A record split across blocks whose trailing bytes are truncated must be
	// reported as a clean EOF rather than a corrupt error.
	var buf bytes.Buffer
	w := newJournalWriter(&buf)
	require.NoError(t, w.Write(bytes.Repeat([]byte("z"), blockSize*2)))
	require.NoError(t, w.Flush())

	full := buf.Bytes()
	truncated := full[:blockSize+headerSize+10] // first chunk + partial second

	r := newJournalReader(bytes.NewReader(truncated))
	_, err := r.Next()
	assert.Equal(t, io.EOF, err)
}

func TestJournalWriteBlockRollover(t *testing.T) {
	t.Parallel()
	// Fill a block to within 3 bytes of the end (avail < headerSize), then
	// write another record so Write pads the tail and rolls to a new block.
	var buf bytes.Buffer
	w := newJournalWriter(&buf)
	require.NoError(t, w.Write(bytes.Repeat([]byte("a"), blockSize-headerSize-3)))
	require.NoError(t, w.Write([]byte("b")))
	require.NoError(t, w.Flush())

	r := newJournalReader(&buf)
	first, err := r.Next()
	require.NoError(t, err)
	assert.Len(t, first, blockSize-headerSize-3)
	second, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("b"), second)
	_, err = r.Next()
	assert.Equal(t, io.EOF, err)
}

func TestJournalUnknownChunkType(t *testing.T) {
	t.Parallel()
	// Hand-craft a single chunk with a valid CRC but an unknown type so Next's
	// default branch returns errJournalCorrupt.
	data := []byte("x")
	block := make([]byte, headerSize+len(data))
	binary.LittleEndian.PutUint16(block[4:], uint16(len(data)))
	block[6] = 99 // unknown type
	copy(block[headerSize:], data)
	crc := crc32.Update(0, castagnoli, block[6:headerSize])
	crc = crc32.Update(crc, castagnoli, data)
	binary.LittleEndian.PutUint32(block[0:], crc)

	r := newJournalReader(bytes.NewReader(block))
	_, err := r.Next()
	assert.ErrorIs(t, err, errJournalCorrupt)
}

func TestJournalCorruptCRC(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	w := newJournalWriter(&buf)
	require.NoError(t, w.Write([]byte("payload")))
	require.NoError(t, w.Flush())

	data := buf.Bytes()
	data[headerSize] ^= 0xff // flip a payload byte, breaking the CRC

	r := newJournalReader(bytes.NewReader(data))
	_, err := r.Next()
	assert.ErrorIs(t, err, errJournalCorrupt)
}

func TestJournalZeroHeaderIsCorruption(t *testing.T) {
	t.Parallel()
	// Valid padding is always shorter than a complete header. Treating a full
	// zero header as padding could skip an entire damaged block and resume at a
	// later record, silently omitting committed mutations.
	r := newJournalReader(bytes.NewReader(make([]byte, blockSize)))
	_, err := r.Next()
	assert.ErrorIs(t, err, errJournalCorrupt)
}

func TestJournalOversizedChunkInFullBlockIsCorruption(t *testing.T) {
	t.Parallel()
	data := make([]byte, blockSize)
	binary.LittleEndian.PutUint16(data[4:], uint16(blockSize))
	data[6] = chunkFull

	r := newJournalReader(bytes.NewReader(data))
	_, err := r.Next()
	assert.ErrorIs(t, err, errJournalCorrupt)
}

func TestJournalOversizedChunkInShortFinalBlockIsTornTail(t *testing.T) {
	t.Parallel()
	data := make([]byte, headerSize+3)
	binary.LittleEndian.PutUint16(data[4:], 10)
	data[6] = chunkFull

	r := newJournalReader(bytes.NewReader(data))
	_, err := r.Next()
	assert.ErrorIs(t, err, io.EOF)
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

func TestJournalReaderReadError(t *testing.T) {
	t.Parallel()
	// A non-EOF error from the underlying reader must propagate out of Next.
	sentinel := errors.New("disk failure")
	r := newJournalReader(errReader{err: sentinel})
	_, err := r.Next()
	assert.ErrorIs(t, err, sentinel)
}

func BenchmarkJournalWrite(b *testing.B) {
	var buf bytes.Buffer
	w := newJournalWriter(&buf)
	record := bytes.Repeat([]byte("x"), 200)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Write(record); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := w.Flush(); err != nil {
		b.Fatal(err)
	}
}

func FuzzJournalRoundTrip(f *testing.F) {
	f.Add([]byte("a"), []byte("bb"), []byte("ccc"))
	f.Add([]byte(""), []byte("hello"), bytes.Repeat([]byte("x"), blockSize+123))

	f.Fuzz(func(t *testing.T, r1, r2, r3 []byte) {
		records := [][]byte{r1, r2, r3}
		var buf bytes.Buffer
		w := newJournalWriter(&buf)
		for _, rec := range records {
			require.NoError(t, w.Write(rec))
		}
		require.NoError(t, w.Flush())

		r := newJournalReader(&buf)
		for _, want := range records {
			got, err := r.Next()
			require.NoError(t, err)
			// A zero-length record round-trips as nil, so compare by bytes.
			assert.True(t, bytes.Equal(want, got))
		}
		_, err := r.Next()
		assert.Equal(t, io.EOF, err)
	})
}

func FuzzJournalReader(f *testing.F) {
	// Seed with a real framed stream.
	var buf bytes.Buffer
	w := newJournalWriter(&buf)
	_ = w.Write([]byte("hello"))
	_ = w.Write(bytes.Repeat([]byte("x"), 5000))
	_ = w.Flush()
	f.Add(buf.Bytes())
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})

	f.Fuzz(func(_ *testing.T, data []byte) {
		// Contract: the reader never panics on arbitrary framed bytes; it drains
		// to an error or EOF. Bounded iterations guard against a malformed frame
		// that fails to advance.
		r := newJournalReader(bytes.NewReader(data))
		for i := 0; i < 100000; i++ {
			if _, err := r.Next(); err != nil {
				return
			}
		}
	})
}
