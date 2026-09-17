package levisdb

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureHandler records the op attribute and component of every emitted record,
// so tests can assert which storage events fired without depending on formatting.
type captureHandler struct {
	mu   *sync.Mutex
	ops  *[]string
	comp *string
}

func (h captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "op":
			*h.ops = append(*h.ops, a.Value.String())
		case "component":
			*h.comp = a.Value.String()
		}
		return true
	})
	return nil
}

func (h captureHandler) WithAttrs(as []slog.Attr) slog.Handler {
	// The root logger adds component=levisdb via WithAttrs; capture it and keep the
	// same sinks so child loggers still record.
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, a := range as {
		if a.Key == "component" {
			*h.comp = a.Value.String()
		}
	}
	return h
}
func (h captureHandler) WithGroup(string) slog.Handler { return h }

func TestLoggingEmitsStorageEvents(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var ops []string
	var comp string
	logger := slog.New(captureHandler{mu: &mu, ops: &ops, comp: &comp})

	db := openTestDB(t, func(o *Options) {
		o.Logger = logger
		o.MemtableSize = 1024
	})
	// Each write fills a memtable; enough flushed tables trigger compaction.
	value := bytes.Repeat([]byte("v"), 1024)
	for i := 0; i < db.opts.TierRatio; i++ {
		require.NoError(t, db.Put(PutOptions{Key: []byte(fmt.Sprintf("k%06d", i)), Value: value}))
		db.sched.drain()
	}
	require.NoError(t, db.Close())

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, "levisdb", comp, "root logger tags events with component=levisdb")
	seen := map[string]bool{}
	for _, o := range ops {
		seen[o] = true
	}
	// The lifecycle and per-operation events LevelDB-style logging should surface.
	for _, want := range []string{"open", "flush", "compaction", "close"} {
		assert.Truef(t, seen[want], "expected a %q event; saw ops %v", want, ops)
	}
}

// TestLoggingNilIsSafe confirms a nil Logger disables logging without error.
func TestLoggingNilIsSafe(t *testing.T) {
	t.Parallel()
	db := openTestDB(t, func(o *Options) { o.Logger = nil })
	require.NoError(t, db.Put(PutOptions{Key: []byte("k"), Value: []byte("v")}))
	v, err := db.Get([]byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), v)
	require.NoError(t, db.Close())
}

// TestDiscardHandlerDisabled pins that the discard handler reports Enabled=false,
// so nil-logger builds skip the cost of assembling routine Debug events.
func TestDiscardHandlerDisabled(t *testing.T) {
	t.Parallel()
	l := newRootLogger(nil)
	assert.False(t, l.Enabled(context.Background(), slog.LevelDebug))
	assert.False(t, l.Enabled(context.Background(), slog.LevelError))
	var buf bytes.Buffer
	realLogger := slog.New(slog.NewTextHandler(&buf, nil))
	assert.True(t, newRootLogger(realLogger).Enabled(context.Background(), slog.LevelInfo))
}
