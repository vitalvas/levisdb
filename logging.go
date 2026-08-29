package levisdb

import (
	"context"
	"log/slog"
)

// discardHandler is a slog.Handler that drops every record. It lets the engine
// always hold a non-nil *slog.Logger (so call sites never nil-check) while a
// caller who set no Options.Logger pays only a level check per event. Go 1.24+
// ships slog.DiscardHandler, but this keeps the engine independent of that
// version and lets Enabled short-circuit routine Debug events cheaply.
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (h discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h discardHandler) WithGroup(string) slog.Handler           { return h }

// newRootLogger returns the engine's root logger: the caller's logger tagged
// with the "component"="levisdb" attribute so every event is attributable, or a
// discarding logger when none was provided. Subsystems derive child loggers from
// this with per-operation attributes (op, table numbers), forming the
// init->leaf hierarchy: root -> subsystem -> operation.
func newRootLogger(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.New(discardHandler{})
	}
	return l.With("component", "levisdb")
}
