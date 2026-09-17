package levisdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// maxTierDepth caps the tier ladder (like LevelDB's fixed level count). Output
// never lands deeper than this; the deepest tier merges in place instead. It
// bounds compaction depth (so a highly-compressible workload cannot march the
// bottom tier downward forever), and with it read/recovery fan-out. Seven tiers
// at the default size curve (2 MiB base, x2, capped at 16 MiB) cover terabytes.
const maxTierDepth = 7

// CompactionConfig tunes the size-tiered picker and the codecs/file sizes used
// for merged output.
type compactionConfigT struct {
	TierRatio int
	// TierByteTrigger compacts a tier once its aggregate on-disk bytes reach this
	// value, even below TierRatio tables (density trigger). Zero disables it.
	TierByteTrigger    int64
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
	// engine users, which do not have a DB WAL.
	FilterThrough uint64
	// MaxCompactionBytes caps the total input bytes merged in one non-bottom
	// compaction so a single merge does not monopolize the spindle. Zero disables
	// the cap (merge the whole tier).
	MaxCompactionBytes int64
	// OverlapSelection narrows a non-bottom compaction to the largest group of
	// key-overlapping tables in the tier (see overlapSets) instead of merging the
	// whole tier, so a lookup touches at most one output table per non-overlapping
	// group. The bottom tier still merges wholly (tombstone GC needs it).
	OverlapSelection bool
}

// pickCompaction returns the tier depth to compact, or -1 if none is ready. A
// tier is ready when it holds at least TierRatio tables (count trigger), OR when
// it holds >= 2 tables and its aggregate on-disk bytes reach byteTrigger (density
// trigger: bounds read amplification when a few large tables never reach the
// count threshold), OR (when tombstoneRatio > 0) when it holds >= 2 tables and
// its tombstone fraction meets tombstoneRatio (reclaim delete-heavy tiers early).
// The shallowest ready tier is chosen so fresh data is merged first.
func (s *engineT) pickCompaction(ratio int, byteTrigger int64, tombstoneRatio float64) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	counts := map[int]int{}
	bytes := map[int]int64{}
	entries := map[int]int{}
	tombstones := map[int]int{}
	for _, t := range s.tables {
		counts[t.depth]++
		bytes[t.depth] += t.size
		entries[t.depth] += t.entries
		tombstones[t.depth] += t.tombstones
	}
	best := -1
	for depth, c := range counts {
		// The cap tier (maxTierDepth) is terminal: there is no deeper tier to push
		// to, and re-merging its distinct data by count or bytes reclaims nothing
		// and would loop forever. Only a tombstone/overwrite-heavy cap tier is worth
		// compacting there, because that genuinely shrinks it.
		ready := false
		if depth < maxTierDepth {
			ready = c >= ratio
			if !ready && byteTrigger > 0 && c >= 2 {
				ready = bytes[depth] >= byteTrigger
			}
		}
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
func (s *engineT) Compact(depth int, retainSeq uint64, cc compactionConfigT) error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	s.mu.RLock()
	tables := append([]*tableMeta(nil), s.tables...)
	maxDepth := 0
	for _, t := range s.tables {
		if t.depth > maxDepth {
			maxDepth = t.depth
		}
	}
	// Output normally lands one tier deeper, but is capped at maxTierDepth so the
	// tier ladder cannot grow without bound. At the cap the deepest tier merges IN
	// PLACE (outDepth == depth). Without the cap, a workload whose live data fits in
	// fewer than TierRatio tables per tier would relocate the bottom one tier deeper
	// every compaction cycle, marching the depth downward forever (an infinite
	// compaction loop that starves flushes). The cap bounds recovery/read fan-out
	// too. When the source is already at the cap, the merge collapses it in place.
	outDepth := depth + 1
	if outDepth > maxTierDepth {
		outDepth = maxTierDepth
	}
	inPlace := outDepth == depth
	var inputs []*tableMeta
	// Non-input tables at ANY depth can contain older versions: ingest puts
	// fresh sequences at the bottom. Include all their bounds in the GC check.
	var deeper [][2][]byte
	for _, t := range s.tables {
		if t.depth == depth {
			inputs = append(inputs, t)
			continue
		}
		deeper = append(deeper, [2][]byte{t.minKey, t.maxKey})
	}
	s.mu.RUnlock()

	if len(inputs) < 2 {
		return nil
	}
	// The output is the bottom tier when nothing lives deeper than it. An in-place
	// merge at the cap is the bottom (its tables are all inputs, nothing is below);
	// otherwise it is the bottom only when the output tier is at/below the deepest.
	bottomCodec := outDepth >= maxDepth
	dropTombstones := outDepth > maxDepth || (inPlace && depth == maxDepth)
	if dropTombstones {
		// Reclaiming a bottom tombstone also requires merging overlapping
		// shallower versions. Include each entire connected overlap group that
		// touches the selected tier, keeping disjoint tables untouched.
		inputs = nil
		for _, group := range overlapSets(tables) {
			for _, table := range group {
				if table.depth == depth {
					inputs = append(inputs, group...)
					break
				}
			}
		}
	}

	// Overlap-scoped selection: merge only the largest group of key-overlapping
	// tables rather than the whole tier, so unrelated key ranges are not rewritten
	// and a lookup touches at most one output table per non-overlapping group. The
	// bottom tier is exempt (dropTombstones needs the whole tier to GC correctly).
	// Same-tier tables left out of the group are treated as deeper so an
	// intermediate-tier tombstone in the group is never dropped over a shadowed
	// value they still hold.
	if cc.OverlapSelection && !dropTombstones {
		groups := overlapSets(inputs)
		if best := largestGroup(groups); len(best) >= 2 && len(best) < len(inputs) {
			chosen := make(map[*tableMeta]bool, len(best))
			for _, t := range best {
				chosen[t] = true
			}
			for _, t := range inputs {
				if !chosen[t] {
					deeper = append(deeper, [2][]byte{t.minKey, t.maxKey})
				}
			}
			inputs = best
		}
	}

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

	// Collect every range tombstone from the (final) input tables. The merge
	// applies them to drop covered point entries; writeMerged persists the subset
	// that must be kept (carried whole to keep shadowing deeper tables, and at the
	// bottom tier only those a live snapshot still needs).
	inputRangeDels, rterr := collectInputRangeDels(inputs)
	if rterr != nil {
		return rterr
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
		rangeDels:      inputRangeDels,
	})
	if err != nil {
		return err
	}

	return s.commitCompaction(inputs, metas)
}

// collectInputRangeDels gathers every range tombstone from the input tables. The
// merge applies them to drop covered point entries, and the sink persists the
// subset that must be kept (see rangeDelsToPersist).
func collectInputRangeDels(inputs []*tableMeta) ([]rangeTombstone, error) {
	var out []rangeTombstone
	for _, t := range inputs {
		trts, err := t.reader.rangeTombstones()
		if err != nil {
			return nil, err
		}
		out = append(out, trts...)
	}
	return out, nil
}

// rangeDelsToPersist returns the range tombstones a compaction output must keep.
// At the bottom tier (dropAtBottom) a tombstone whose sequence is at or below
// retainSeq is dropped: no snapshot needs it and nothing deeper survives to
// un-shadow, and the merge has already dropped the point entries it covered.
// Tombstones above retainSeq, and all tombstones at a non-bottom tier, are kept.
//
// Bottom merges include every overlapping table at every depth. Table bounds
// include range tombstones, so no covered older point survives outside the merge.
func rangeDelsToPersist(rts []rangeTombstone, dropAtBottom bool, retainSeq uint64) []rangeTombstone {
	if !dropAtBottom {
		return rts
	}
	var out []rangeTombstone
	for i := range rts {
		if rts[i].seq > retainSeq {
			out = append(out, rts[i])
		}
	}
	return out
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

// overlapSets partitions tables into maximal groups of key-overlapping tables,
// following the UCS transitive-overlap idea: tables whose key ranges connect
// (directly or through a chain) belong to one group; disjoint ranges form
// separate groups. Compacting one group resolves its overlaps without touching
// unrelated ranges, so a lookup then touches at most one output table per group.
//
// Algorithm (O(n log n)): sort by minKey, then sweep, extending the current group
// while the next table's minKey is <= the group's running maxKey (they overlap or
// abut), closing the group when a gap appears. A nil minKey (unknown low bound)
// sorts first and joins the first group; a nil maxKey (unknown high bound) makes
// the running range cover everything, so all remaining tables join that group -
// the conservative choice, matching rangesMayContain's nil handling.
func overlapSets(tables []*tableMeta) [][]*tableMeta {
	if len(tables) == 0 {
		return nil
	}
	sorted := make([]*tableMeta, len(tables))
	copy(sorted, tables)
	sort.SliceStable(sorted, func(i, j int) bool {
		return compareOptionalMin(sorted[i].minKey, sorted[j].minKey) < 0
	})

	var sets [][]*tableMeta
	cur := []*tableMeta{sorted[0]}
	runMax := sorted[0].maxKey
	openEnded := sorted[0].maxKey == nil // running range covers everything above
	for _, t := range sorted[1:] {
		// t overlaps the current group if the group is open-ended, or t's low bound
		// is unknown, or t.minKey <= runMax.
		overlaps := openEnded || t.minKey == nil || bytes.Compare(t.minKey, runMax) <= 0
		if overlaps {
			cur = append(cur, t)
			if !openEnded {
				if t.maxKey == nil {
					openEnded = true
				} else if bytes.Compare(t.maxKey, runMax) > 0 {
					runMax = t.maxKey
				}
			}
			continue
		}
		sets = append(sets, cur)
		cur = []*tableMeta{t}
		runMax = t.maxKey
		openEnded = t.maxKey == nil
	}
	sets = append(sets, cur)
	return sets
}

// largestGroup returns the group with the most tables (the highest-overlap
// bucket, matching UCS's highest-overlap-first selection), or nil if none.
func largestGroup(groups [][]*tableMeta) []*tableMeta {
	var best []*tableMeta
	for _, g := range groups {
		if len(g) > len(best) {
			best = g
		}
	}
	return best
}

// compareOptionalMin orders min-key bounds: nil (unknown low) sorts before any
// concrete key so an unbounded table joins the first group.
func compareOptionalMin(a, b []byte) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	default:
		return bytes.Compare(a, b)
	}
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
func (s *engineT) commitCompaction(inputs, metas []*tableMeta) error {
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

// CompactAll merges every table in the engine, regardless of tier, into
// bottom-tier tables. Because the output is the bottom, tombstones and dead
// versions (older than retainSeq) are reclaimed. Used by CompactRange for an
// operator-triggered full compaction.
func (s *engineT) CompactAll(retainSeq uint64, cc compactionConfigT) error {
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
	// CompactAll collapses every tier into the bottom; the merge applies range
	// tombstones to covered points and persists only those a snapshot still needs.
	inputRangeDels, rterr := collectInputRangeDels(inputs)
	if rterr != nil {
		return rterr
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
		rangeDels:      inputRangeDels,
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
	// rangeDels are the range tombstones carried from the input tables, to persist
	// in an output table. Empty when dropped at the bottom tier.
	rangeDels []rangeTombstone
	// noDeeperTier reports that no non-input table, including shallower tables,
	// can contain the user key. A tombstone can then be reclaimed without
	// revealing an older value left outside this merge.
	// nil means "unknown for every key" (never reclaim at an intermediate tier).
	noDeeperTier func(userKey []byte) bool
}

// writeMerged writes the merged stream to a new table, dropping older versions
// of each user key and, at the bottom tier, tombstones as well. Output rolls at
// user-key boundaries according to the configured per-depth file-size curve.
func (s *engineT) writeMerged(mw mergeWrite) ([]*tableMeta, error) {
	codecName := resolveLevelCodec(mw.cc.LevelCodecs, mw.depth, mw.cc.FreshCodecName, mw.cc.BottomCodecName, mw.bottomCodec)
	c, err := codecFromName(codecName)
	if err != nil {
		return nil, err
	}
	sink := compactionSink{
		eng:              s,
		depth:            mw.depth,
		codec:            c,
		bloomBits:        mw.cc.BloomBits,
		blockSize:        mw.cc.BlockSize,
		target:           mw.cc.targetFileSize(mw.depth),
		pendingRangeDels: rangeDelsToPersist(mw.rangeDels, mw.dropTombstones, mw.retainSeq),
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
				// TTL sorts before delete at the same sequence. Recovery may
				// replay the original TTL beside its compacted tombstone; accept
				// only that expired pair. Same-kind payload conflicts still fail.
				_, expiredValue, expiredKind, expiryErr := resolveCompactionEntry(compactionConfigT{}, mergeEntry{
					ik:    ik,
					user:  user,
					value: lastValue,
					seq:   seq,
					kind:  lastKind,
				}, now)
				if expiryErr == nil && kind == expiredKind && bytes.Equal(value, expiredValue) {
					continue
				}
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
		// lastValue must own its bytes: value aliases the merge iterator's reusable
		// curVal buffer, which the next Next() overwrites in place, so a borrowed
		// slice would make the same-seq conflict check above compare a buffer with
		// itself and silently miss a genuine value conflict. Copy like lastUser.
		lastSeq, lastKind = seq, kind
		lastValue = append(lastValue[:0], value...)
		// A carried range tombstone newer than this version deletes it. When the key
		// can be reclaimed here (bottom tier or no deeper tier holds it) and no
		// snapshot needs the pre-delete value (rt.seq <= retainSeq), drop the entry
		// and mark the key decided so its older versions drop too. Otherwise the
		// point is written and the range tombstone (persisted alongside) shadows it
		// on read, so no version is lost for a snapshot that still needs it.
		if reclaimHere && maxCoveringRangeDelSeqLE(mw.rangeDels, user, mw.retainSeq) > seq {
			droppedBelow = true
			continue
		}
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
	// Carried range tombstones must survive even when the merge produced no point
	// entries (so no output table was started). Force one output to hold them.
	if len(sink.pendingRangeDels) > 0 && sink.out == nil {
		if err := sink.start(); err != nil {
			return sink.fail(err)
		}
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
	num  uint32
	path string
	f    *os.File
	w    *tableWriter
}

type compactionSink struct {
	eng                  *engineT
	depth                int
	codec                blockCodec
	bloomBits, blockSize int
	target               int64
	out                  *compactionOutput
	metas                []*tableMeta
	// pendingRangeDels are range tombstones carried from the input tables that have
	// not yet been written to an output. They are attached to the first output
	// table produced (one carrier is enough: the read path scans every table's
	// range tombstones, so a tombstone in one output still shadows keys in another).
	pendingRangeDels []rangeTombstone
}

func (s *compactionSink) start() error {
	num := s.eng.alloc.Next()
	if num == 0 {
		return ErrFileNumberExhausted
	}
	path, err := s.eng.tablePath(num)
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
		w: newTableWriter(f, tableWriterConfig{
			codec:     s.codec,
			bloomBits: s.bloomBits,
			blockSize: s.blockSize,
		}),
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
	return nil
}

// rollIfNeeded closes the current output table once its COMPRESSED on-disk size
// reaches the target, so file sizes track real disk footprint and highly
// compressible data does not spray many tiny SSTs. bytesWritten counts only
// blocks already flushed to the file, so a table always holds at least one full
// block before it can roll (it never produces an almost-empty SST); the current
// in-memory block adds at most one blockSize of overshoot. Called at user-key
// boundaries, so a key's versions never split across tables.
func (s *compactionSink) rollIfNeeded() error {
	if s.out != nil && s.out.w.bytesWritten() >= s.target {
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
	// Attach any carried range tombstones to this (the first) output, then clear
	// them so later output tables in the same compaction do not duplicate them.
	rts := s.pendingRangeDels
	s.pendingRangeDels = nil
	out.w.setRangeTombstones(rts)
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
	minKey, maxKey := widenBoundsForRangeDels(out.w.minUserKey(), out.w.maxUserKey(), rts)
	meta, err := s.eng.openTableMeta(tableSpec{
		num:        out.num,
		depth:      s.depth,
		path:       out.path,
		size:       size,
		minKey:     minKey,
		maxKey:     maxKey,
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
