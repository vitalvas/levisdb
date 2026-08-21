// Block codecs provide compression for SSTables. Each block records a
// one-byte codec id, so a single table may mix codecs across tiers: a fast
// codec (s2) for fresh tiers and a high-ratio codec (zstd) for the bottom.

package levisdb

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
)

// Codec names accepted by Options.FreshCodec and Options.BottomCodec. Use these
// constants instead of bare strings when configuring compression.
const (
	// CodecNone stores blocks uncompressed.
	CodecNone = "none"
	// CodecS2 is fast with a moderate ratio, a good fit for fresh tiers.
	CodecS2 = "s2"
	// CodecZstd is high-ratio, a good fit for the cold bottom tier.
	CodecZstd = "zstd"
)

// ID is the one-byte identifier stored with each block.
type codecID byte

const (
	// None stores blocks uncompressed.
	codecNone codecID = iota
	// S2 is fast with a moderate ratio, used for fresh tiers.
	codecS2
	// Zstd is high-ratio, used for the bottom tier.
	codecZstd
)

// Codec compresses and decompresses a block.
type blockCodec interface {
	id() codecID
	Name() string
	compress(dst, src []byte) []byte
	decompress(dst, src []byte) ([]byte, error)
}

// FromName returns the codec for a configured name (CodecNone, CodecS2,
// CodecZstd).
func codecFromName(name string) (blockCodec, error) {
	switch name {
	case CodecNone:
		return noneCodec{}, nil
	case CodecS2:
		return s2Codec{}, nil
	case CodecZstd:
		return zstdCodec{}, nil
	default:
		return nil, fmt.Errorf("codec: unknown codec %q", name)
	}
}

// resolveLevelCodec picks the codec name for a table written at depth. A
// non-empty LevelCodecs[depth] wins; otherwise it falls back to the two-way
// split (bottom for the deepest tier, fresh elsewhere). Options.validate has
// already checked every LevelCodecs entry, so the name is always valid here.
func resolveLevelCodec(levelCodecs []string, depth int, fresh, bottom string, bottomTier bool) string {
	if depth >= 0 && depth < len(levelCodecs) && levelCodecs[depth] != "" {
		return levelCodecs[depth]
	}
	if bottomTier {
		return bottom
	}
	return fresh
}

// FromID returns the codec that wrote a block, for the read path.
func codecFromID(id codecID) (blockCodec, error) {
	switch id {
	case codecNone:
		return noneCodec{}, nil
	case codecS2:
		return s2Codec{}, nil
	case codecZstd:
		return zstdCodec{}, nil
	default:
		return nil, fmt.Errorf("codec: unknown codec id %d", id)
	}
}

type noneCodec struct{}

func (noneCodec) id() codecID                     { return codecNone }
func (noneCodec) Name() string                    { return CodecNone }
func (noneCodec) compress(dst, src []byte) []byte { return append(dst, src...) }
func (noneCodec) decompress(dst, src []byte) ([]byte, error) {
	return append(dst, src...), nil
}

type s2Codec struct{}

func (s2Codec) id() codecID                     { return codecS2 }
func (s2Codec) Name() string                    { return CodecS2 }
func (s2Codec) compress(dst, src []byte) []byte { return s2.Encode(dst, src) }
func (s2Codec) decompress(dst, src []byte) ([]byte, error) {
	out, err := s2.Decode(dst, src)
	if err != nil {
		return nil, fmt.Errorf("codec: s2 decode: %w", err)
	}
	return out, nil
}

// Shared zstd encoder/decoder, created once on first use. Constructing them is
// expensive; they are safe for concurrent use. Lazy construction avoids an
// init() and the nil-options constructors never fail in practice.
var (
	zstdEncoder = sync.OnceValue(func() *zstd.Encoder {
		enc, err := zstd.NewWriter(nil)
		if err != nil {
			panic(fmt.Sprintf("codec: zstd encoder init: %v", err))
		}
		return enc
	})
	zstdDecoder = sync.OnceValue(func() *zstd.Decoder {
		dec, err := zstd.NewReader(nil)
		if err != nil {
			panic(fmt.Sprintf("codec: zstd decoder init: %v", err))
		}
		return dec
	})
)

type zstdCodec struct{}

func (zstdCodec) id() codecID                     { return codecZstd }
func (zstdCodec) Name() string                    { return CodecZstd }
func (zstdCodec) compress(dst, src []byte) []byte { return zstdEncoder().EncodeAll(src, dst) }
func (zstdCodec) decompress(dst, src []byte) ([]byte, error) {
	out, err := zstdDecoder().DecodeAll(src, dst)
	if err != nil {
		return nil, fmt.Errorf("codec: zstd decode: %w", err)
	}
	return out, nil
}
