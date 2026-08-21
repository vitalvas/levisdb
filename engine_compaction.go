package levisdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// CompactionConfig tunes the size-tiered picker and the codecs/file sizes used
// for merged output.
type compactionConfigT struct {
	TierRatio          int
	BloomBits          int
	BlockSize          int
	FreshCodecName     string
	BottomCodecName    string
	LevelCodecs        []string
	FileSizeBase       int64
	FileSizeMultiplier int
	FileSizeMax        int64
	// ExpireBefore is the newest wall-clock instant at which TTL entries may be
	// reclaimed. Zero uses the compaction start time. DB compactions set it to
	// the oldest live iterator's read time.
	ExpireBefore int64
	Filter       CompactionFilter
	// FilterThrough is the highest sequence safe for physical filtering because
	// its recovery WAL has been retired. Zero disables the bound for direct
	// shard users, which do not have a DB WAL.
	FilterThrough uint64
	// MaxCompactionBytes caps the total input bytes merged in one non-bottom
	// compaction so a single merge does not monopolize the spindle. Zero disables
	// the cap (merge the whole tier).
	MaxCompactionBytes int64
}

// pickCompaction returns the tier depth to compact, or -1 if none is ready. A
// tier is ready when it holds at least TierRatio tables, OR (when
// tombstoneRatio > 0) when the tier holds >= 2 tables and its aggregate
// tombstone fraction meets tombstoneRatio, so a delete-heavy tier is compacted
// down to reclaim space early instead of waiting for the count threshold. The
// shallowest ready tier is chosen so fresh data is merged first.
func (s *shardT) pickCompaction(ratio int, tombstoneRatio float64) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	counts := map[int]int{}
	entries := map[int]int{}
	tombstones := map[int]int{}
	for _, t := range s.tables {
		counts[t.depth]++
		entries[t.depth] += t.entries
		tombstones[t.depth] += t.tombstones
	}
	best := -1
	for depth, c := range counts {
		ready := c >= ratio
		if !ready && tombstoneRatio > 0 && c >= 2 && entries[depth] > 0 {
			ready = float64(tombstones[depth])/float64(entries[depth]) >= tombstoneRatio
		}
		if ready && (best == -1 || depth < best) {
			best = depth
		}
	}
	return best
}

// Compact merges all tables at the given depth into tables at depth+1, rolling
// output at configured size targets.
// It collapses each user key to its newest version, retaining older versions
// with seq >= retainSeq so live read snapshots still see them, and, when the
// output is the deepest tier, drops tombstones and fully shadowed versions to
// reclaim space. Pass retainSeq 0 (or MaxSeq for "keep only newest") per policy.
func (s *shardT) Compact(depth int, retainSeq uint64, cc compactionConfigT) error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	outDepth := depth + 1
	s.mu.RLock()
	var inputs []*tableMeta
	// deeper holds the key bounds of every table at or below the output tier that
	// is NOT an input, so writeMerged can reclaim a tombstone at an intermediate
	// tier when no such table can hold the key. flushMu serializes compaction per
	// shard, so this snapshot stays valid for the whole merge.
	var deeper [][2][]byte
	maxDepth := 0
	for _, t := range s.tables {
		if t.depth > maxDepth {
			maxDepth = t.depth
		}
		if t.depth == depth {
			inputs = append(inputs, t)
			continue
		}
		if t.depth >= outDepth {
			deeper = append(deeper, [2][]byte{t.minKey, t.maxKey})
		}
	}
	s.mu.RUnlock()

	if len(inputs) < 2 {
		return nil
	}
	// The output is the bottom tier only when no tables live at a deeper tier.
	bottomCodec := outDepth >= maxDepth
	dropTombstones := outDepth > maxDepth

	// Byte-cap: a single compaction should not monopolize the one spindle merging
	// an unbounded tier. Cap the input bytes and leave the rest for the next
	// round. Applies only when NOT dropping tombstones (bottom-tier drops need the
	// whole tier merged). Always keep >= 2 inputs so progress is guaranteed even
	// when a single table exceeds the cap.
	if !dropTombstones && cc.MaxCompactionBytes > 0 {
		full := inputs
		inputs = capCompactionInputs(inputs, cc.MaxCompactionBytes)
		// A same-tier table the cap left uncompacted can still hold an older
		// version of a key whose tombstone this compaction might reclaim. Treat
		// its bounds like a deeper tier so noDeeperTier never drops such a
		// tombstone and resurrects the shadowed value.
		for _, t := range full[len(inputs):] {
			deeper = append(deeper, [2][]byte{t.minKey, t.maxKey})
		}
	}

	// Build a merge over all input table iterators.
	srcs := make([]entrySource, len(inputs))
	for i, t := range inputs {
		srcs[i] = t.reader.newIterator()
	}
	merged := newMergeIter(srcs...)

	// At an intermediate tier, a tombstone can be reclaimed for a key that no
	// deeper-or-destination table's recorded range covers. Skip the predicate at
	// the bottom (dropTombstones already reclaims everything) and when any deeper
	// table has unknown bounds for the key (then it must be assumed present).
	var noDeeperTier func([]byte) bool
	if !dropTombstones && len(deeper) > 0 {
		noDeeperTier = func(user []byte) bool { return !rangesMayContain(deeper, user) }
	}

	metas, err := s.writeMerged(mergeWrite{
		depth:          outDepth,
		merged:         merged,
		bottomCodec:    bottomCodec,
		dropTombstones: dropTombstones,
		noDeeperTier:   noDeeperTier,
		retainSeq:      retainSeq,
		cc:             cc,
	})
	if err != nil {
		return err
	}

	return s.commitCompaction(inputs, metas)
}

// rangesMayContain reports whether any [min,max] bound could contain key. A nil
// bound is unknown and conservatively covers everything, so an intermediate-tier
// tombstone is never dropped when a deeper table's range is unknown.
func rangesMayContain(ranges [][2][]byte, key []byte) bool {
	for _, r := range ranges {
		minKey, maxKey := r[0], r[1]
		if minKey != nil && bytes.Compare(key, minKey) < 0 {
			continue
		}
		if maxKey != nil && bytes.Compare(key, maxKey) > 0 {
			continue
		}
		return true // within [min,max], or unknown bounds cover it
	}
	return false
}

// capCompactionInputs returns the longest prefix of inputs whose cumulative size
// stays within maxBytes, but always at least two tables so a compaction makes
// progress even when one table alone exceeds the cap. inputs is unchanged when
// it already fits.
func capCompactionInputs(inputs []*tableMeta, maxBytes int64) []*tableMeta {
	var total int64
	for i, t := range inputs {
		total += t.size
		if total > maxBytes && i+1 >= 2 {
			return inputs[:i+1]
		}
	}
	return inputs
}

// commitCompaction records the replacement durably before installing it in
// memory and removing the source files. A crash therefore sees either the old
// manifest and all old inputs, or the new manifest and the complete output.
func (s *shardT) commitCompaction(inputs, metas []*tableMeta) error {
	install := func() {
		s.mu.Lock()
		kept := s.tables[:0:0]
		inputSet := make(map[uint32]bool, len(inputs))
		for _, t := range inputs {
			inputSet[t.num] = true
		}
		for _, t := range s.tables {
			if !inputSet[t.num] {
				kept = append(kept, t)
			}
		}
		kept = append(kept, metas...)
		s.tables = kept
		s.mu.Unlock()
	}
	if s.cfg.Commit != nil {
		if err := s.cfg.Commit(inputs, metas, install); err != nil {
			for _, meta := range metas {
				// Preserve ambiguous outputs: a manifest Sync error can be reported
				// after the replacement edit became readable. Recovery either uses
				// these files or later classifies them as unreferenced orphans.
				_ = meta.releaseOwner(false)
			}
			return err
		}
	} else {
		install()
	}
	for _, t := range inputs {
		_ = t.releaseOwner(true)
	}
	return nil
}

// CompactAll merges every table in the shard, regardless of tier, into
// bottom-tier tables. Because the output is the bottom, tombstones and dead
// versions (older than retainSeq) are reclaimed. Used by CompactRange for an
// operator-triggered full compaction.
func (s *shardT) CompactAll(retainSeq uint64, cc compactionConfigT) error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	s.mu.RLock()
	inputs := make([]*tableMeta, len(s.tables))
	copy(inputs, s.tables)
	maxDepth := 0
	for _, t := range s.tables {
		if t.depth > maxDepth {
			maxDepth = t.depth
		}
	}
	s.mu.RUnlock()

	if len(inputs) == 0 {
		return nil
	}
	srcs := make([]entrySource, len(inputs))
	for i, t := range inputs {
		srcs[i] = t.reader.newIterator()
	}
	metas, err := s.writeMerged(mergeWrite{
		depth:          maxDepth,
		merged:         newMergeIter(srcs...),
		bottomCodec:    true,
		dropTombstones: true,
		retainSeq:      retainSeq,
		cc:             cc,
	})
	if err != nil {
		return err
	}
	return s.commitCompaction(inputs, metas)
}

// mergeWrite bundles the inputs for writeMerged.
type mergeWrite struct {
	depth          int
	merged         *mergeIter
	bottomCodec    bool
	dropTombstones bool
	retainSeq      uint64
	cc             compactionConfigT
	// noDeeperTier reports that no table below this compaction's output tier can
	// contain the user key (by recorded key bounds). When it holds, a tombstone
	// or shadowed version can be reclaimed at an intermediate tier just as at the
	// bottom, because nothing below it could hold an older version to un-shadow.
	// nil means "unknown for every key" (never reclaim at an intermediate tier).
	noDeeperTier func(userKey []byte) bool
}

// writeMerged writes the merged stream to a new table, dropping older versions
// of each user key and, at the bottom tier, tombstones as well. Output rolls at
// user-key boundaries according to the configured per-depth file-size curve.
func (s *shardT) writeMerged(mw mergeWrite) ([]*tableMeta, error) {
	codecName := resolveLevelCodec(mw.cc.LevelCodecs, mw.depth, mw.cc.FreshCodecName, mw.cc.BottomCodecName, mw.bottomCodec)
	c, err := codecFromName(codecName)
	if err != nil {
		return nil, err
	}
	sink := compactionSink{
		shard:     s,
		depth:     mw.depth,
		codec:     c,
		bloomBits: mw.cc.BloomBits,
		blockSize: mw.cc.BlockSize,
		target:    mw.cc.targetFileSize(mw.depth),
	}
	var lastUser []byte
	haveLast := false
	var lastSeq uint64
	var lastKind ikeyKind
	var lastValue []byte
	droppedBelow := false // a version at or below retainSeq was already kept
	// reclaimHere is true for the current key when tombstones/dead versions can
	// be dropped: at the bottom tier, or at an intermediate tier when no deeper
	// tier can hold the key. Recomputed per new user key.
	reclaimHere := mw.dropTombstones
	now := mw.cc.ExpireBefore
	if now == 0 {
		now = time.Now().UnixNano()
	}
	for mw.merged.Next() {
		ik := mw.merged.internalKey()
		user := ikeyUserKey(ik)
		seq, kind := ikeySeqKind(ik)
		value := mw.merged.Value()

		sameKey := haveLast && bytes.Equal(user, lastUser)
		switch {
		case !sameKey:
			if err := sink.rollIfNeeded(); err != nil {
				return sink.fail(err)
			}
			haveLast = true
			lastUser = append(lastUser[:0], user...)
			droppedBelow = false
			reclaimHere = mw.dropTombstones ||
				(mw.noDeeperTier != nil && mw.noDeeperTier(user))
		case seq == lastSeq:
			if kind != lastKind {
				return sink.fail(fmt.Errorf("compaction: conflicting kinds at sequence %d", seq))
			}
			if !bytes.Equal(value, lastValue) {
				return sink.fail(fmt.Errorf("compaction: conflicting values at sequence %d", seq))
			}
			continue // identical internal key replayed into more than one table
		case seq < mw.retainSeq && droppedBelow:
			// Older version of a key: a snapshot needs versions with
			// seq >= retainSeq plus the single newest one strictly below
			// retainSeq. Once that below-watermark version is kept, drop the
			// rest for this key.
			continue
		}
		lastSeq, lastKind, lastValue = seq, kind, value
		writeKey, value, kind, err := resolveCompactionEntry(mw.cc, mergeEntry{
			ik:    ik,
			user:  user,
			value: value,
			seq:   seq,
			kind:  kind,
		}, now)
		if err != nil {
			return sink.fail(err)
		}
		if reclaimHere && kind == ikeyKindDelete && seq <= mw.retainSeq {
			// A tombstone that no live snapshot needs and that shadows nothing
			// below (bottom tier, or no deeper tier holds this key): drop it and
			// mark the key decided so its older versions are dropped too. (When
			// seq > retainSeq a snapshot may still need the pre-delete value, so
			// the tombstone and older versions are kept.)
			droppedBelow = true
			continue
		}
		if seq <= mw.retainSeq {
			droppedBelow = true
		}
		if err := sink.add(writeKey, value); err != nil {
			return sink.fail(err)
		}
	}
	if err := mw.merged.Error(); err != nil {
		return sink.fail(err)
	}
	if err := sink.finish(); err != nil {
		return sink.fail(err)
	}
	return sink.metas, nil
}

// mergeEntry is one entry the compaction merge is deciding on.
type mergeEntry struct {
	ik    []byte // full internal key
	user  []byte // user-key portion of ik
	value []byte
	seq   uint64
	kind  ikeyKind
}

// resolveCompactionEntry applies TTL expiry and the optional compaction filter
// to one entry, returning the key/value/kind to write. An expired TTL value or a
// value the filter rejects becomes a tombstone. now is the expiry cutoff.
func resolveCompactionEntry(cc compactionConfigT, e mergeEntry, now int64) (writeKey, outValue []byte, outKind ikeyKind, err error) {
	writeKey = e.ik
	filterValue := e.value
	var ttl time.Duration
	if e.kind == ikeyKindSetTTL {
		decoded, expiresAt, derr := decodeExpiringValue(e.value)
		if derr != nil {
			return nil, nil, 0, fmt.Errorf("compaction: %w", derr)
		}
		if expiresAt <= now {
			return ikeyEncode(nil, e.user, e.seq, ikeyKindDelete), nil, ikeyKindDelete, nil
		}
		filterValue = decoded
		ttl = time.Duration(expiresAt - now)
	}
	filterSafe := cc.FilterThrough == 0 || e.seq <= cc.FilterThrough
	if e.kind != ikeyKindDelete && cc.Filter != nil && filterSafe {
		keep, ferr := callCompactionFilter(cc.Filter, CompactionFilterEntry{
			Key:   bytes.Clone(e.user),
			Value: bytes.Clone(filterValue),
			TTL:   ttl,
		})
		if ferr != nil {
			return nil, nil, 0, ferr
		}
		if !keep {
			return ikeyEncode(nil, e.user, e.seq, ikeyKindDelete), nil, ikeyKindDelete, nil
		}
	}
	return writeKey, e.value, e.kind, nil
}

type compactionOutput struct {
	num    uint32
	path   string
	f      *os.File
	w      *tableWriter
	approx int64
}

type compactionSink struct {
	shard                *shardT
	depth                int
	codec                blockCodec
	bloomBits, blockSize int
	target               int64
	out                  *compactionOutput
	metas                []*tableMeta
}

func (s *compactionSink) start() error {
	num := s.shard.alloc.Next()
	if num == 0 {
		return ErrFileNumberExhausted
	}
	path, err := s.shard.tablePath(num)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	s.out = &compactionOutput{
		num:  num,
		path: path,
		f:    f,
		w:    newTableWriter(f, s.codec, s.bloomBits, s.blockSize),
	}
	return nil
}

func (s *compactionSink) add(key, value []byte) error {
	if s.out == nil {
		if err := s.start(); err != nil {
			return err
		}
	}
	if err := s.out.w.Add(key, value); err != nil {
		return err
	}
	s.out.approx += int64(len(key) + len(value) + 16)
	return nil
}

func (s *compactionSink) rollIfNeeded() error {
	if s.out != nil && s.out.approx >= s.target {
		return s.finish()
	}
	return nil
}

func (s *compactionSink) finish() error {
	if s.out == nil {
		return nil
	}
	out := s.out
	s.out = nil
	size, err := out.w.finish()
	if err == nil {
		err = out.f.Sync()
	}
	if closeErr := out.f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = removeFileDurable(out.path)
		return err
	}
	if err := syncDir(filepath.Dir(out.path)); err != nil {
		_ = removeFileDurable(out.path)
		return err
	}
	meta, err := s.shard.openTableMeta(tableSpec{
		num:        out.num,
		depth:      s.depth,
		path:       out.path,
		size:       size,
		minKey:     out.w.minUserKey(),
		maxKey:     out.w.maxUserKey(),
		entries:    out.w.entryCount(),
		tombstones: out.w.tombstoneCount(),
	})
	if err != nil {
		_ = removeFileDurable(out.path)
		return err
	}
	s.metas = append(s.metas, meta)
	return nil
}

func (s *compactionSink) fail(err error) ([]*tableMeta, error) {
	if s.out != nil {
		_ = s.out.f.Close()
		_ = removeFileDurable(s.out.path)
		s.out = nil
	}
	for _, meta := range s.metas {
		_ = meta.releaseOwner(true)
	}
	s.metas = nil
	return nil, err
}

func (cc compactionConfigT) targetFileSize(depth int) int64 {
	base := cc.FileSizeBase
	if base <= 0 {
		base = DefaultFileSizeBase
	}
	maxSize := cc.FileSizeMax
	if maxSize < base {
		maxSize = DefaultFileSizeMax
		if maxSize < base {
			maxSize = base
		}
	}
	mul := cc.FileSizeMultiplier
	if mul < 1 {
		mul = DefaultFileSizeMultiplier
	}
	target := base
	for range depth {
		if target >= maxSize || target > maxSize/int64(mul) {
			return maxSize
		}
		target *= int64(mul)
	}
	if target > maxSize {
		return maxSize
	}
	return target
}
