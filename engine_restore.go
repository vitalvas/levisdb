package levisdb

import (
	"os"
)

// TableSnapshot describes a live table for the manifest.
type tableSnapshotT struct {
	Num        uint32
	Depth      int
	Size       int64
	MinKey     []byte // user-key bounds (nil when unknown, e.g. an empty table)
	MaxKey     []byte
	Entries    int
	Tombstones int
}

// Tables returns a snapshot of the engine's live tables.
func (s *engineT) Tables() []tableSnapshotT {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]tableSnapshotT, len(s.tables))
	for i, t := range s.tables {
		out[i] = tableSnapshotT{
			Num:        t.num,
			Depth:      t.depth,
			Size:       t.size,
			MinKey:     t.minKey,
			MaxKey:     t.maxKey,
			Entries:    t.entries,
			Tombstones: t.tombstones,
		}
	}
	return out
}

// openTable opens an existing on-disk table file and installs it in the engine,
// used to restore state from the manifest on open. Tables are installed in
// ascending file-number order for deterministic snapshots; reads compare the
// visible sequence because compaction file order is not data recency. A zero
// spec.size is resolved by stat.
func (s *engineT) openTable(spec tableSpec) error {
	if spec.size == 0 {
		info, serr := os.Stat(spec.path)
		if serr != nil {
			return serr
		}
		spec.size = info.Size()
	}
	meta, err := s.openTableMeta(spec)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.tables = append(s.tables, meta)
	s.mu.Unlock()
	return nil
}
