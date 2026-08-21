package levisdb

import (
	"encoding/binary"
	"fmt"
	"time"
)

const expiryPrefixLen = 8

// PutOptions describes one put operation. Key and Value are copied before the
// call returns. A zero TTL stores the value without expiration; a positive TTL
// is measured from Write submission.
type PutOptions struct {
	Key   []byte
	Value []byte
	TTL   time.Duration
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

func ttlExpiresAt(now time.Time, ttl time.Duration) (int64, error) {
	if ttl < 0 {
		return 0, ErrInvalidTTL
	}
	if ttl == 0 {
		return 0, nil
	}
	nowNanos := now.UnixNano()
	if int64(ttl) > int64(^uint64(0)>>1)-nowNanos {
		return 0, ErrInvalidTTL
	}
	return nowNanos + int64(ttl), nil
}
