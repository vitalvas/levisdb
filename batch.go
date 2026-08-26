package levisdb

// Put appends opts as a set operation to the batch.
//
// The batch retains opts.Key and opts.Value by reference; it does not copy them.
// The caller must not mutate those slices until Write returns, after which the
// key and value have been copied into the WAL record and the memtable and the
// caller's buffers may be reused. (This matches the batch contract of goleveldb,
// RocksDB, and Pebble; the single-op db.Put is unaffected because it commits
// synchronously.)
func (b *Batch) Put(opts PutOptions) {
	b.ops = append(b.ops, batchOp{
		kind:  EntryPut,
		key:   opts.Key,
		value: opts.Value,
		ttl:   opts.TTL,
	})
}

// Delete appends a delete operation to the batch. As with Put, key is retained
// by reference and must not be mutated until Write returns.
func (b *Batch) Delete(key []byte) {
	b.ops = append(b.ops, batchOp{kind: EntryDelete, key: key})
}

// Reset clears the batch for reuse.
func (b *Batch) Reset() {
	clear(b.ops)
	b.ops = b.ops[:0]
}

// Len returns the number of operations in the batch.
func (b *Batch) Len() int {
	return len(b.ops)
}

// walKind maps a public EntryKind to the internal WAL kind.
func walKind(k EntryKind) walKindType {
	if k == EntryDelete {
		return walKindDelete
	}
	return walKindPut
}
