package levisdb

// Put appends opts as a set operation to the batch.
func (b *Batch) Put(opts PutOptions) {
	b.ops = append(b.ops, batchOp{
		kind:  EntryPut,
		key:   append([]byte(nil), opts.Key...),
		value: append([]byte(nil), opts.Value...),
		ttl:   opts.TTL,
	})
}

// Delete appends a delete operation to the batch.
func (b *Batch) Delete(key []byte) {
	b.ops = append(b.ops, batchOp{kind: EntryDelete, key: append([]byte(nil), key...)})
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
