// Storage abstracts the on-disk layout of a levisdb database: the data
// directory, its single-process lock, and file naming.
//
// File numbers are a single monotonic uint32 counter hex-encoded to a fixed
// 8-character width so lexical order matches creation order during recovery.
// Tables, WAL segments, the manifest, LOCK, and CURRENT all live directly in
// the database directory; the file-name patterns are disjoint so listing one
// kind never picks up another.
//
// ponytail: uint32 file numbers cap at ~4.3B lifetime creations before wrap;
// widen to uint64 (%016x) if a deployment ever approaches that.

package levisdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	lockName     = "LOCK"
	currentName  = "CURRENT"
	manifestName = "MANIFEST"
	// tableSuffix is the on-disk extension for sorted-string tables. Its 4-byte
	// width keeps the "%08x" + suffix file name at a fixed 12 chars so lexical
	// sort still equals numeric sort.
	tableSuffix = ".sst"
)

// Storage owns a database directory and its exclusive lock.
type storageT struct {
	dir            string
	lock           *os.File
	readOnly       bool
	currentSyncDir func(string) error // test hook; nil uses syncDir
}

// currentInstallError means CURRENT was already renamed into place, but the
// directory sync that makes the rename crash-durable failed. Callers must keep
// the newly referenced manifest; deleting it would make CURRENT dangle.
type currentInstallError struct{ err error }

func (e *currentInstallError) Error() string { return e.err.Error() }
func (e *currentInstallError) Unwrap() error { return e.err }

func currentWasInstalled(err error) bool {
	var installErr *currentInstallError
	return errors.As(err, &installErr)
}

// Open creates dir if needed and acquires an exclusive lock on it. Close must
// be called to release the lock.
func openStorage(dir string) (*storageT, error) {
	return openStorageMode(dir, false)
}

func openStorageReadOnly(dir string) (*storageT, error) {
	return openStorageMode(dir, true)
}

func openStorageMode(dir string, readOnly bool) (*storageT, error) {
	if readOnly {
		if _, err := os.Stat(filepath.Join(dir, currentName)); err != nil {
			return nil, err
		}
		lock, err := acquireLockMode(filepath.Join(dir, lockName), true)
		if err != nil {
			return nil, err
		}
		return &storageT{dir: dir, lock: lock, readOnly: true}, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		return nil, err
	}
	lock, err := acquireLockMode(filepath.Join(dir, lockName), false)
	if err != nil {
		return nil, err
	}
	return &storageT{dir: dir, lock: lock}, nil
}

// Close releases the directory lock.
func (s *storageT) Close() error {
	if s.lock == nil {
		return nil
	}
	err := releaseLock(s.lock)
	s.lock = nil
	return err
}

// TablePath returns the path to a table file.
func (s *storageT) tablePath(num uint32) (string, error) {
	return filepath.Join(s.dir, fmt.Sprintf("%08x%s", num, tableSuffix)), nil
}

// listTables returns the canonical table file numbers present in the database
// directory. The strict %08x.sst name filter skips LOCK, CURRENT, MANIFEST-*,
// and WAL segments, so discovery and orphan cleanup stay layout-free.
func (s *storageT) listTables() ([]uint32, error) {
	entries, err := os.ReadDir(s.dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var nums []uint32
	for _, e := range entries {
		if e.IsDir() || len(e.Name()) != 12 || !strings.HasSuffix(e.Name(), tableSuffix) {
			continue
		}
		stem := strings.TrimSuffix(e.Name(), tableSuffix)
		v, perr := strconv.ParseUint(stem, 16, 32)
		if perr != nil || fmt.Sprintf("%08x", v) != stem {
			continue
		}
		nums = append(nums, uint32(v))
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	return nums, nil
}

// logPath returns the path to a WAL segment, colocated with the tables in the
// database directory. The file number comes from the global allocator, so
// segment numbers never collide with table numbers.
func (s *storageT) logPath(num uint32) (string, error) {
	return filepath.Join(s.dir, fmt.Sprintf("%08x.log", num)), nil
}

// listLogs returns the WAL segment numbers present, ascending. On a clean
// shutdown none remain; after a crash the leftover segments are replayed into
// the memtable.
func (s *storageT) listLogs() ([]uint32, error) {
	entries, err := os.ReadDir(s.dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var nums []uint32
	for _, e := range entries {
		if e.IsDir() || len(e.Name()) != 12 || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		stem := strings.TrimSuffix(e.Name(), ".log")
		v, perr := strconv.ParseUint(stem, 16, 32)
		if perr != nil || fmt.Sprintf("%08x", v) != stem {
			continue
		}
		nums = append(nums, uint32(v))
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	return nums, nil
}

// removeLog deletes a WAL segment after its entries have been recovered.
func (s *storageT) removeLog(num uint32) error {
	path, err := s.logPath(num)
	if err != nil {
		return err
	}
	return removeFileDurable(path)
}

// ManifestPath returns the path to a manifest file.
func (s *storageT) manifestPath(num uint32) string {
	return filepath.Join(s.dir, fmt.Sprintf("%s-%08x", manifestName, num))
}

// RemoveManifest deletes an obsolete manifest file after rotation.
func (s *storageT) removeManifest(num uint32) error {
	return removeFileDurable(s.manifestPath(num))
}

// ListManifests returns the file numbers of all manifest files present.
func (s *storageT) listManifests() ([]uint32, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	prefix := fmt.Sprintf("%s-", manifestName)
	var nums []uint32
	for _, e := range entries {
		if e.IsDir() || len(e.Name()) != len(prefix)+8 || !strings.HasPrefix(e.Name(), prefix) {
			continue
		}
		stem := strings.TrimPrefix(e.Name(), prefix)
		v, perr := strconv.ParseUint(stem, 16, 32)
		if perr != nil || fmt.Sprintf("%08x", v) != stem {
			continue
		}
		nums = append(nums, uint32(v))
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	return nums, nil
}

// ReadCurrent returns the manifest number recorded in CURRENT, or ok=false if
// CURRENT does not exist (a fresh database).
func (s *storageT) readCurrent() (num uint32, ok bool, err error) {
	data, err := os.ReadFile(filepath.Join(s.dir, currentName))
	if os.IsNotExist(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	name := strings.TrimSpace(string(data))
	prefix := fmt.Sprintf("%s-", manifestName)
	if len(name) != len(prefix)+8 || !strings.HasPrefix(name, prefix) {
		return 0, false, fmt.Errorf("storage: malformed CURRENT %q", name)
	}
	stem := strings.TrimPrefix(name, prefix)
	v, err := strconv.ParseUint(stem, 16, 32)
	if err != nil || fmt.Sprintf("%08x", v) != stem {
		return 0, false, fmt.Errorf("storage: malformed CURRENT %q: %w", name, err)
	}
	return uint32(v), true, nil
}

// SetCurrent atomically points CURRENT at the given manifest number.
func (s *storageT) setCurrent(num uint32) error {
	name := fmt.Sprintf("%s-%08x", manifestName, num)
	tmp := filepath.Join(s.dir, fmt.Sprintf("%s.tmp", currentName))
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := writeAll(f, []byte(fmt.Sprintf("%s\n", name))); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, currentName)); err != nil {
		return err
	}
	syncCurrentDir := s.currentSyncDir
	if syncCurrentDir == nil {
		syncCurrentDir = syncDir
	}
	if err := syncCurrentDir(s.dir); err != nil {
		return &currentInstallError{err: err}
	}
	return nil
}

func removeFileDurable(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	// Sync even when the name is already absent: a prior attempt may have
	// removed it but failed while making that directory update durable.
	return syncDir(filepath.Dir(path))
}
