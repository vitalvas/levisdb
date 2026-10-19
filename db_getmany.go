package levisdb

import (
	"bytes"
	"sort"
)

// GetManyResult is one found key's value and stored expiry in a GetMany result,
// carrying the same fields as GetResult.
type GetManyResult struct {
	Value     []byte
	ExpiresAt int64
}

// GetMany reads many keys at a single consistent snapshot of the latest committed
// sequence and returns a map from key to value for the keys that exist. A key that
// is absent, deleted, or expired is simply omitted from the map (check with the
// comma-ok form), so a missing key never fails the batch. Reading the whole batch
// at one sequence, under one read lock, and in sorted key order makes co-located
// keys reuse cached data blocks instead of each paying a random seek, so this is
// faster and lower-latency than calling Get in a loop for a large key set. Empty
// keys are skipped. The call errors only when the database is closed or a read
// fails; a nil/empty input returns an empty map.
func (db *DB) GetMany(keys [][]byte) (map[string]GetManyResult, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	seq := db.snaps.acquireCurrent(&db.readSeq)
	db.mu.RUnlock()
	defer db.snaps.release(seq)
	return db.getManyAt(seq, keys)
}

// getManyAt resolves a batch of keys at a specific sequence into a key->value map,
// omitting keys that do not exist. It deduplicates and sorts the keys so the engine
// resolves them in one ordered, block-cache-friendly sweep.
func (db *DB) getManyAt(seq uint64, keys [][]byte) (map[string]GetManyResult, error) {
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	// Deduplicate by key (a repeated key resolves once) and skip empty keys, which
	// are never valid. Build the sorted batch the engine sweep expects.
	seen := make(map[string]struct{}, len(keys))
	batch := make([]batchKey, 0, len(keys))
	for _, k := range keys {
		if len(k) == 0 {
			continue
		}
		if _, dup := seen[string(k)]; dup {
			continue
		}
		seen[string(k)] = struct{}{}
		batch = append(batch, batchKey{key: k})
	}
	if len(batch) == 0 {
		return map[string]GetManyResult{}, nil
	}
	sort.Slice(batch, func(i, j int) bool { return bytes.Compare(batch[i].key, batch[j].key) < 0 })
	if err := db.eng.getManyWithExpiry(seq, batch); err != nil {
		return nil, err
	}
	out := make(map[string]GetManyResult, len(batch))
	for i := range batch {
		if batch[i].found {
			out[string(batch[i].key)] = GetManyResult{Value: batch[i].value, ExpiresAt: batch[i].expiresAt}
		}
	}
	return out, nil
}
