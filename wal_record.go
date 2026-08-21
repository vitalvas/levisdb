// WAL records encode batches of mutations for the shared write-ahead log.

package levisdb

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Kind identifies a mutation kind. Values match the public levisdb.EntryKind.
type walKindType uint8

const (
	// KindPut sets a key to a value.
	walKindPut walKindType = iota
	// KindDelete removes a key.
	walKindDelete
	// walKindPutTTL stores an absolute expiration before the value so replay
	// preserves the original deadline instead of restarting the TTL.
	walKindPutTTL
)

// Entry is one mutation in a batch. Seq and Shard are assigned by the writer.
type walEntry struct {
	Seq       uint64
	Shard     int
	Kind      walKindType
	Key       []byte
	Value     []byte
	ExpiresAt int64
}

// Record encoding: a batch is [count][entry...], each entry
// [kind(1)][shard(uvarint)][keylen(uvarint)][key][optional expiry(varint)]
// [vallen(uvarint)][val]. Expiry is present only for walKindPutTTL.
// The base sequence number for the batch is stored once at the front so the
// per-entry Seq can be reconstructed on replay without storing it per entry.
//
// layout: [baseSeq(8)][count(uvarint)][entries...]

// encodeBatch serializes entries into a single record. baseSeq is the sequence
// number of the first entry; subsequent entries increment by one.
func encodeBatch(dst []byte, baseSeq uint64, entries []walEntry) []byte {
	var seq [8]byte
	binary.LittleEndian.PutUint64(seq[:], baseSeq)
	dst = append(dst, seq[:]...)
	dst = binary.AppendUvarint(dst, uint64(len(entries)))
	for i := range entries {
		e := &entries[i]
		dst = append(dst, byte(e.Kind))
		dst = binary.AppendUvarint(dst, uint64(e.Shard))
		dst = binary.AppendUvarint(dst, uint64(len(e.Key)))
		dst = append(dst, e.Key...)
		if e.Kind == walKindPutTTL {
			dst = binary.AppendVarint(dst, e.ExpiresAt)
		}
		if e.Kind == walKindPut || e.Kind == walKindPutTTL {
			dst = binary.AppendUvarint(dst, uint64(len(e.Value)))
			dst = append(dst, e.Value...)
		}
	}
	return dst
}

// decodeBatch parses a record into entries with reconstructed Seq values.
func decodeBatch(rec []byte) ([]walEntry, error) {
	if len(rec) < 8 {
		return nil, fmt.Errorf("wal: short record")
	}
	baseSeq := binary.LittleEndian.Uint64(rec[:8])
	rest := rec[8:]
	count, n := binary.Uvarint(rest)
	if n <= 0 {
		return nil, fmt.Errorf("wal: bad entry count")
	}
	rest = rest[n:]

	// Each entry occupies at least one byte in rest, so a count larger than the
	// remaining bytes is corrupt. Bounding the preallocation this way stops a
	// malformed record from triggering a huge allocation.
	if count > uint64(len(rest)) {
		return nil, fmt.Errorf("wal: entry count %d exceeds record size", count)
	}
	if count > 0 && baseSeq > math.MaxUint64-(count-1) {
		return nil, fmt.Errorf("wal: sequence range overflows")
	}
	entries := make([]walEntry, 0, count)
	for i := uint64(0); i < count; i++ {
		if len(rest) < 1 {
			return nil, fmt.Errorf("wal: truncated entry kind")
		}
		kind := walKindType(rest[0])
		rest = rest[1:]
		if kind != walKindPut && kind != walKindDelete && kind != walKindPutTTL {
			return nil, fmt.Errorf("wal: unknown entry kind %d", kind)
		}

		shard, key, r, err := readShardKey(rest)
		if err != nil {
			return nil, err
		}
		rest = r

		e := walEntry{Seq: baseSeq + i, Shard: shard, Kind: kind, Key: key}
		if kind == walKindPutTTL {
			expiresAt, used := binary.Varint(rest)
			if used <= 0 || expiresAt <= 0 {
				return nil, fmt.Errorf("wal: invalid expiration")
			}
			e.ExpiresAt = expiresAt
			rest = rest[used:]
		}
		if kind == walKindPut || kind == walKindPutTTL {
			val, r2, err := readBytes(rest)
			if err != nil {
				return nil, fmt.Errorf("wal: value: %w", err)
			}
			e.Value = val
			rest = r2
		}
		entries = append(entries, e)
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("wal: trailing bytes")
	}
	return entries, nil
}

func readShardKey(rest []byte) (shard int, key, remaining []byte, err error) {
	s, n := binary.Uvarint(rest)
	if n <= 0 {
		return 0, nil, nil, fmt.Errorf("wal: bad shard")
	}
	if s > uint64(^uint(0)>>1) {
		return 0, nil, nil, fmt.Errorf("wal: shard overflows int")
	}
	rest = rest[n:]
	key, rest, err = readBytes(rest)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("wal: key: %w", err)
	}
	return int(s), key, rest, nil
}

func readBytes(rest []byte) (data, remaining []byte, err error) {
	length, n := binary.Uvarint(rest)
	if n <= 0 {
		return nil, nil, fmt.Errorf("wal: bad length")
	}
	rest = rest[n:]
	if uint64(len(rest)) < length {
		return nil, nil, fmt.Errorf("wal: truncated data")
	}
	return rest[:length], rest[length:], nil
}
