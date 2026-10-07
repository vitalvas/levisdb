package levisdb

import (
	"encoding/binary"
	"fmt"
	"time"
)

const expiryPrefixLen = 8

// PutOptions describes one put operation. Key and Value are copied before the
// call returns. A zero ExpiresAt stores the value without expiration; a positive
// ExpiresAt is an absolute expiry instant in Unix seconds (as from
// time.Time.Unix). Sub-second TTLs are not supported, so the unit is seconds. A
// negative or already-past value is rejected with ErrInvalidTTL. The deadline is
// absolute, not relative, so re-writing the same key/value/ExpiresAt is a
// byte-identical record (see Options.Deduplication).
type PutOptions struct {
	Key       []byte
	Value     []byte
	ExpiresAt int64
}

func encodeExpiringValue(value []byte, expiresAt int64) []byte {
	out := make([]byte, expiryPrefixLen, expiryPrefixLen+len(value))
	binary.LittleEndian.PutUint64(out, uint64(expiresAt))
	return append(out, value...)
}

func decodeExpiringValue(stored []byte) (value []byte, expiresAt int64, err error) {
	if len(stored) < expiryPrefixLen {
		return nil, 0, fmt.Errorf("ttl: truncated expiration metadata")
	}
	expiresAt = int64(binary.LittleEndian.Uint64(stored[:expiryPrefixLen]))
	if expiresAt <= 0 {
		return nil, 0, fmt.Errorf("ttl: invalid expiration timestamp %d", expiresAt)
	}
	return stored[expiryPrefixLen:], expiresAt, nil
}

// validateExpiresAt checks an absolute expiry (Unix seconds) supplied on a put.
// Zero means no expiration. A negative value, or one already at or before now, is
// rejected: an already-expired write stores a value no read would ever return.
func validateExpiresAt(now time.Time, expiresAt int64) error {
	if expiresAt == 0 {
		return nil
	}
	if expiresAt <= now.Unix() {
		return ErrInvalidTTL
	}
	return nil
}
