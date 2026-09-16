package levisdb

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultOptions(t *testing.T) {
	t.Parallel()
	o := DefaultOptions("/tmp/db")

	assert.Equal(t, "/tmp/db", o.Dir)
	assert.Equal(t, int64(DefaultMemtableSize), o.MemtableSize)
	assert.Equal(t, DefaultTierRatio, o.TierRatio)
	assert.Equal(t, int64(DefaultFileSizeBase), o.FileSizeBase)
	assert.Equal(t, DefaultFileSizeMultiplier, o.FileSizeMultiplier)
	assert.Equal(t, int64(DefaultFileSizeMax), o.FileSizeMax)
	assert.Equal(t, DefaultBloomBits, o.BloomBits)
	assert.Equal(t, DefaultBlockSize, o.BlockSize)
	assert.Equal(t, int64(DefaultBlockCacheSize), o.BlockCacheSize)
	assert.Equal(t, DefaultFreshCodec, o.FreshCodec)
	assert.Equal(t, DefaultBottomCodec, o.BottomCodec)

	require.NoError(t, o.validate())
}

func TestFillDefaultsPreservesSetValues(t *testing.T) {
	t.Parallel()
	o := Options{
		Dir:                "/data",
		MemtableSize:       1 << 20,
		TierRatio:          3,
		FileSizeBase:       1 << 20,
		FileSizeMultiplier: 3,
		FileSizeMax:        4 << 20,
		BloomBits:          16,
		BlockSize:          8 << 10,
		BlockCacheSize:     1 << 30,
		FreshCodec:         "none",
		BottomCodec:        "s2",
	}
	o.fillDefaults()

	assert.Equal(t, 3, o.TierRatio)
	assert.Equal(t, "none", o.FreshCodec)
	assert.Equal(t, "s2", o.BottomCodec)
}

func TestFillDefaultsTierByteTrigger(t *testing.T) {
	t.Parallel()

	t.Run("default derived from FileSizeMax", func(t *testing.T) {
		o := Options{Dir: "/d"}
		o.fillDefaults()
		assert.Equal(t, o.FileSizeMax*defaultTierByteTriggerFiles, o.TierByteTrigger)
		assert.Positive(t, o.TierByteTrigger)
	})

	t.Run("negative disables the trigger", func(t *testing.T) {
		o := Options{Dir: "/d", TierByteTrigger: -1}
		o.fillDefaults()
		assert.Zero(t, o.TierByteTrigger, "a negative value disables the density trigger")
	})

	t.Run("explicit value preserved", func(t *testing.T) {
		o := Options{Dir: "/d", TierByteTrigger: 5 << 20}
		o.fillDefaults()
		assert.Equal(t, int64(5<<20), o.TierByteTrigger)
	})
}

func TestValidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mutate  func(*Options)
		wantErr string
	}{
		{"valid", func(*Options) {}, ""},
		{"missing dir", func(o *Options) { o.Dir = "" }, "Dir is required"},
		{"non-positive memtable size", func(o *Options) { o.MemtableSize = -1 }, "MemtableSize must be positive"},
		{"memtable size over limit", func(o *Options) { o.MemtableSize = maxMemtableSize + 1 }, "MemtableSize must be <="},
		{"tier ratio too small", func(o *Options) { o.TierRatio = 1 }, "TierRatio must be >= 2"},
		{"NaN tombstone ratio", func(o *Options) { o.TombstoneCompactionRatio = math.NaN() }, "TombstoneCompactionRatio"},
		{"tombstone ratio above one", func(o *Options) { o.TombstoneCompactionRatio = 1.1 }, "TombstoneCompactionRatio"},
		{"stop below slowdown", func(o *Options) { o.L0SlowdownTables = 10; o.L0StopTables = 5 }, "L0StopTables"},
		{"non-positive file size base", func(o *Options) { o.FileSizeBase = -1 }, "FileSizeBase must be positive"},
		{"file size multiplier too small", func(o *Options) { o.FileSizeMultiplier = 0 }, "FileSizeMultiplier must be >= 1"},
		{"max below base", func(o *Options) { o.FileSizeMax = o.FileSizeBase - 1 }, "FileSizeMax"},
		{"negative bloom", func(o *Options) { o.BloomBits = -1 }, "BloomBits must be positive"},
		{"non-positive block size", func(o *Options) { o.BlockSize = 0 }, "BlockSize must be positive"},
		{"negative block cache", func(o *Options) { o.BlockCacheSize = -1 }, "BlockCacheSize must be non-negative"},
		{"unknown fresh codec", func(o *Options) { o.FreshCodec = "lz4" }, "unknown FreshCodec"},
		{"unknown bottom codec", func(o *Options) { o.BottomCodec = "lz4" }, "unknown BottomCodec"},
		{"unknown level codec", func(o *Options) { o.LevelCodecs = []string{CodecS2, "lz4"} }, "unknown LevelCodecs[1]"},
		{"level codecs with empty gap ok", func(o *Options) { o.LevelCodecs = []string{CodecZstd, "", CodecNone} }, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := DefaultOptions("/tmp/db")
			tt.mutate(&o)
			err := o.validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}
