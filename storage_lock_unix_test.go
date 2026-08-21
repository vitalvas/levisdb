//go:build linux || darwin

package levisdb

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcquireReleaseLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "LOCK")

	f, err := acquireLock(path)
	require.NoError(t, err)
	require.NotNil(t, f)

	// A second lock on the same path must fail while the first is held.
	f2, err := acquireLock(path)
	assert.Error(t, err)
	assert.Nil(t, f2)

	require.NoError(t, releaseLock(f))

	// After release the lock can be re-acquired.
	f3, err := acquireLock(path)
	require.NoError(t, err)
	require.NoError(t, releaseLock(f3))
}
