package levisdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failWriter returns errFailWrite once more than n total bytes have been
// written, letting tests fail a table write at a chosen point.
type failWriter struct {
	n       int
	written int
}

var errFailWrite = errors.New("failWriter: write failed")

func (w *failWriter) Write(p []byte) (int, error) {
	if w.written+len(p) > w.n {
		w.written = w.n
		return 0, errFailWrite
	}
	w.written += len(p)
	return len(p), nil
}

func TestTableWriter(t *testing.T) {
	t.Parallel()
	c, err := codecFromName("none")
	require.NoError(t, err)

	t.Run("finish_reports_size", func(t *testing.T) {
		var buf bytes.Buffer
		tw := newTableWriter(&buf, tableWriterConfig{codec: c, bloomBits: 10, blockSize: 4096})
		require.NoError(t, tw.Add(ikeyEncode(nil, []byte("a"), 1, ikeyKindSet), []byte("va")))
		require.NoError(t, tw.Add(ikeyEncode(nil, []byte("b"), 1, ikeyKindSet), []byte("vb")))

		size, err := tw.finish()
		require.NoError(t, err)
		assert.Equal(t, int64(buf.Len()), size)
		assert.Greater(t, size, int64(footerLen))
	})

	t.Run("out_of_order_rejected", func(t *testing.T) {
		var buf bytes.Buffer
		tw := newTableWriter(&buf, tableWriterConfig{codec: c, bloomBits: 10, blockSize: 4096})
		require.NoError(t, tw.Add(ikeyEncode(nil, []byte("b"), 1, ikeyKindSet), []byte("vb")))

		err := tw.Add(ikeyEncode(nil, []byte("a"), 1, ikeyKindSet), []byte("va"))
		assert.Error(t, err)

		// Subsequent operations return the sticky error.
		_, err = tw.finish()
		assert.Error(t, err)
	})

	t.Run("empty_table", func(t *testing.T) {
		var buf bytes.Buffer
		tw := newTableWriter(&buf, tableWriterConfig{codec: c, bloomBits: 10, blockSize: 4096})
		size, err := tw.finish()
		require.NoError(t, err)
		assert.Equal(t, int64(buf.Len()), size)
	})

	t.Run("multi_block", func(t *testing.T) {
		var buf bytes.Buffer
		// Small block size forces multiple data blocks.
		tw := newTableWriter(&buf, tableWriterConfig{codec: c, bloomBits: 10, blockSize: 64})
		for i := 0; i < 50; i++ {
			k := ikeyEncode(nil, []byte{byte(i)}, 1, ikeyKindSet)
			require.NoError(t, tw.Add(k, bytes.Repeat([]byte("x"), 16)))
		}
		size, err := tw.finish()
		require.NoError(t, err)
		assert.Equal(t, int64(buf.Len()), size)
	})
}

func BenchmarkTableWriterAdd(b *testing.B) {
	const entries = 2000
	c, err := codecFromName("s2")
	require.NoError(b, err)

	// Pre-build sorted internal keys once; only Add/finish are timed.
	keys := make([][]byte, entries)
	for i := range keys {
		uk := make([]byte, 8)
		binary.BigEndian.PutUint64(uk, uint64(i))
		keys[i] = ikeyEncode(nil, uk, uint64(i), ikeyKindSet)
	}
	val := bytes.Repeat([]byte("x"), 64)

	var buf bytes.Buffer
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		buf.Reset()
		tw := newTableWriter(&buf, tableWriterConfig{codec: c, bloomBits: 10, blockSize: 4096})
		b.StartTimer()

		for _, k := range keys {
			if err := tw.Add(k, val); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := tw.finish(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestTableWriterWriteErrors(t *testing.T) {
	t.Parallel()
	c, err := codecFromName("none")
	require.NoError(t, err)

	t.Run("data block write fails and Add is sticky", func(t *testing.T) {
		// The table writer buffers block writes, so a failure surfaces once the
		// buffer flushes to the underlying writer. Write enough to exceed the
		// buffer and force a flush inside Add, then assert Add reports it and stays
		// sticky.
		w := &failWriter{n: 0}
		tw := newTableWriter(w, tableWriterConfig{codec: c, bloomBits: 10, blockSize: 8})
		val := bytes.Repeat([]byte("x"), 1024)
		var got error
		for i := 0; i < tableWriteBufferSize/1024+16; i++ {
			k := ikeyEncode(nil, []byte{byte(i >> 8), byte(i)}, 1, ikeyKindSet)
			if err := tw.Add(k, val); err != nil {
				got = err
				break
			}
		}
		require.ErrorIs(t, got, errFailWrite, "a flush inside Add must surface the write error")
		// Subsequent Add returns the sticky error too.
		assert.ErrorIs(t, tw.Add(ikeyEncode(nil, []byte{0xff, 0xff}, 1, ikeyKindSet), []byte("y")), errFailWrite)
	})

	t.Run("finish returns sticky error", func(t *testing.T) {
		w := &failWriter{n: 0}
		tw := newTableWriter(w, tableWriterConfig{codec: c, bloomBits: 10, blockSize: 8})
		for i := 0; i < 20; i++ {
			k := ikeyEncode(nil, []byte{byte(i)}, 1, ikeyKindSet)
			_ = tw.Add(k, bytes.Repeat([]byte("x"), 16))
		}
		_, err := tw.finish()
		assert.ErrorIs(t, err, errFailWrite)
	})

	t.Run("filter block write fails in finish", func(t *testing.T) {
		var probe bytes.Buffer
		ptw := newTableWriter(&probe, tableWriterConfig{codec: c, bloomBits: 10, blockSize: 4096})
		require.NoError(t, ptw.Add(ikeyEncode(nil, []byte("a"), 1, ikeyKindSet), []byte("va")))
		_, err := ptw.finish()
		require.NoError(t, err)

		// Allow every byte through until just before the filter block, so the
		// last-data-block write in finish succeeds but writeRawBlock fails.
		w := &failWriter{n: 30}
		tw := newTableWriter(w, tableWriterConfig{codec: c, bloomBits: 10, blockSize: 4096})
		require.NoError(t, tw.Add(ikeyEncode(nil, []byte("a"), 1, ikeyKindSet), []byte("va")))
		_, err = tw.finish()
		assert.ErrorIs(t, err, errFailWrite)
	})

	t.Run("footer write fails in finish", func(t *testing.T) {
		// Allow all block writes but fail on the fixed-size footer at the end.
		var full bytes.Buffer
		ftw := newTableWriter(&full, tableWriterConfig{codec: c, bloomBits: 10, blockSize: 4096})
		require.NoError(t, ftw.Add(ikeyEncode(nil, []byte("a"), 1, ikeyKindSet), []byte("va")))
		_, err := ftw.finish()
		require.NoError(t, err)

		w := &failWriter{n: full.Len() - footerLen}
		tw := newTableWriter(w, tableWriterConfig{codec: c, bloomBits: 10, blockSize: 4096})
		require.NoError(t, tw.Add(ikeyEncode(nil, []byte("a"), 1, ikeyKindSet), []byte("va")))
		_, err = tw.finish()
		assert.ErrorIs(t, err, errFailWrite)
	})
}
