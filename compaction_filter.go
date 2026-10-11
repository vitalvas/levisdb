package levisdb

import (
	"fmt"
)

// CompactionFilterEntry is a non-deleted, unexpired value being rewritten by
// compaction. ExpiresAt is the value's absolute expiry as a Unix-second timestamp
// (the same deadline a Put set via PutOptions.ExpiresAt); zero means the value has
// no expiration. Key and Value are copies owned by the callback.
type CompactionFilterEntry struct {
	Key       []byte
	Value     []byte
	ExpiresAt int64
}

// CompactionFilter decides whether a live value survives compaction. Returning
// true keeps the value; returning false discards it without revealing an older
// version of the same key. A value that survives may be presented again by a
// later compaction.
type CompactionFilter func(entry CompactionFilterEntry) bool

// callCompactionFilter invokes a user filter, converting a panic into an error
// so a misbehaving callback cannot crash a background compaction.
func callCompactionFilter(filter CompactionFilter, entry CompactionFilterEntry) (keep bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("compaction filter panic: %v", recovered)
		}
	}()
	return filter(entry), nil
}
