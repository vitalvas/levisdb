package levisdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenStorageCreatesLayout(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := openStorage(dir)
	require.NoError(t, err)
	defer s.Close()

	assert.DirExists(t, dir)
	assert.FileExists(t, filepath.Join(dir, lockName))
}

func TestStoragePaths(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := openStorage(dir)
	require.NoError(t, err)
	defer s.Close()

	t.Run("tablePath", func(t *testing.T) {
		p, err := s.tablePath(255)
		require.NoError(t, err)
		assert.Equal(t, filepath.Join(dir, "000000ff.sst"), p)
	})

	t.Run("logPath", func(t *testing.T) {
		p, err := s.logPath(1)
		require.NoError(t, err)
		assert.Equal(t, filepath.Join(dir, "00000001.log"), p)
	})

	t.Run("manifestPath", func(t *testing.T) {
		assert.Equal(t, filepath.Join(dir, "MANIFEST-0000002a"), s.manifestPath(42))
	})
}

func TestStorageListAndRemoveLogs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := openStorage(dir)
	require.NoError(t, err)
	defer s.Close()

	data := s.dir
	for _, n := range []uint32{5, 1, 3} {
		p, perr := s.logPath(n)
		require.NoError(t, perr)
		require.NoError(t, os.WriteFile(p, []byte("x"), 0o644))
	}
	// A non-log file and a bad name must be ignored.
	require.NoError(t, os.WriteFile(filepath.Join(data, "notes.txt"), []byte("x"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(data, "zz.log"), []byte("x"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(data, "1.log"), []byte("x"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(data, "0000000A.log"), []byte("x"), 0o644))

	logs, err := s.listLogs()
	require.NoError(t, err)
	assert.Equal(t, []uint32{1, 3, 5}, logs)

	require.NoError(t, s.removeLog(3))
	logs, err = s.listLogs()
	require.NoError(t, err)
	assert.Equal(t, []uint32{1, 5}, logs)
}

func TestStorageListTablesCanonicalOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := openStorage(dir)
	require.NoError(t, err)
	defer s.Close()

	data := s.dir
	for _, name := range []string{
		"00000005.sst", "00000001.sst", "notes.txt", "1.sst",
		"0000000A.sst", "00000003.SST",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(data, name), []byte("x"), 0o644))
	}

	nums, err := s.listTables()
	require.NoError(t, err)
	assert.Equal(t, []uint32{1, 5}, nums)
}

func TestStorageCurrent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := openStorage(dir)
	require.NoError(t, err)
	defer s.Close()

	t.Run("missing", func(t *testing.T) {
		_, ok, err := s.readCurrent()
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("roundtrip", func(t *testing.T) {
		require.NoError(t, s.setCurrent(42))
		num, ok, err := s.readCurrent()
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, uint32(42), num)
	})

	t.Run("malformed", func(t *testing.T) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, currentName), []byte("garbage\n"), 0o644))
		_, _, err := s.readCurrent()
		assert.Error(t, err)
	})
}

func TestStorageCloseIdempotent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := openStorage(dir)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	assert.NoError(t, s.Close()) // second close is a no-op
}

func TestOpenStorageErrors(t *testing.T) {
	t.Parallel()

	t.Run("mkdir-parent-is-file", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "afile")
		require.NoError(t, os.WriteFile(file, []byte("x"), 0o644))
		// Parent of the target dir is a regular file, so MkdirAll fails.
		_, err := openStorage(filepath.Join(file, "sub"))
		assert.Error(t, err)
	})

	t.Run("already-locked", func(t *testing.T) {
		dir := t.TempDir()
		s, err := openStorage(dir)
		require.NoError(t, err)
		defer s.Close()
		_, err = openStorage(dir) // second open cannot acquire the lock
		assert.Error(t, err)
	})
}

func TestStorageReadCurrentBadSuffix(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := openStorage(dir)
	require.NoError(t, err)
	defer s.Close()

	// Valid MANIFEST- prefix but a non-hex suffix triggers the ParseUint path.
	require.NoError(t, os.WriteFile(filepath.Join(dir, currentName), []byte("MANIFEST-zzzz\n"), 0o644))
	_, _, err = s.readCurrent()
	assert.Error(t, err)
}

func TestStorageReadCurrentRejectsNonCanonicalNumber(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := openStorage(dir)
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, os.WriteFile(filepath.Join(dir, currentName), []byte("MANIFEST-1\n"), 0o644))
	_, _, err = s.readCurrent()
	assert.ErrorContains(t, err, "malformed CURRENT")
}

func TestStorageSetCurrentWriteError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := openStorage(dir)
	require.NoError(t, err)
	defer s.Close()

	// Make the dir read-only so writing the CURRENT.tmp file fails.
	require.NoError(t, os.Chmod(dir, 0o555))
	defer os.Chmod(dir, 0o755)

	if err := s.setCurrent(1); err == nil {
		// note: on some environments (e.g. running as root) a read-only dir is
		// still writable; skip rather than fail spuriously.
		t.Skip("directory perms not enforced in this environment")
	}
}
