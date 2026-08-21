//go:build linux || darwin

package levisdb

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncDir(t *testing.T) {
	t.Parallel()

	t.Run("existing directory", func(t *testing.T) {
		require.NoError(t, syncDir(t.TempDir()))
	})

	t.Run("missing path errors", func(t *testing.T) {
		assert.Error(t, syncDir(filepath.Join(t.TempDir(), "does-not-exist")))
	})
}
