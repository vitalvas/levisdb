//go:build linux || darwin

package levisdb

import (
	"fmt"
	"os"
	"syscall"
)

// acquireLock opens path and takes an exclusive, non-blocking advisory lock,
// so a second process opening the same database fails fast rather than
// corrupting it.
func acquireLock(path string) (*os.File, error) {
	return acquireLockMode(path, false)
}

func acquireLockMode(path string, readOnly bool) (*os.File, error) {
	flags := os.O_CREATE | os.O_RDWR
	lockMode := syscall.LOCK_EX | syscall.LOCK_NB
	if readOnly {
		flags = os.O_RDONLY
		lockMode = syscall.LOCK_SH | syscall.LOCK_NB
	}
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), lockMode); err != nil {
		f.Close()
		return nil, fmt.Errorf("storage: database already locked: %w", err)
	}
	return f, nil
}

// releaseLock unlocks and closes the lock file.
func releaseLock(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}
