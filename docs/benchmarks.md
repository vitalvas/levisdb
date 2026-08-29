# Benchmarks

Both engines run with NoSync so the comparison measures the storage engine. The
1M-element results are below; a 50M-element run is at the end.

## Write (1,000,000 elements)

| Value | levisdb | goleveldb |
| --- | --- | --- |
| 16 B | 435 ms | **357 ms** |
| 256 B | **938 ms** | 1.79 s |
| 1 KB | **1.85 s** | 6.80 s |

The write gap widens with value size: goleveldb's leveled write amplification grows
with the data moved per level, while levisdb's size-tiered path stays efficient.

## Random point read (1,000,000 elements)

| Value | levisdb | goleveldb |
| --- | --- | --- |
| 16 B | **553 ns/op** (121 B, 4 allocs) | 2,646 ns/op (1,472 B, 17 allocs) |
| 256 B | **966 ns/op** (567 B, 4 allocs) | 2,922 ns/op (1,185 B, 17 allocs) |

## Full scan (1,000,000 elements)

| Value | levisdb | goleveldb |
| --- | --- | --- |
| 16 B | **40 ms** (0.5 MB, 48 allocs) | 54 ms (14.3 MB, 53.9k allocs) |
| 256 B | **61 ms** (1.1 MB, 60 allocs) | 161 ms (45.4 MB, 441k allocs) |

Scan allocations are near-constant: the iterator reuses its key-reconstruction and
decompression buffers across blocks, so a full scan does not scale allocations
with the row count.

## Large scale (50,000,000 elements)

Bulk load and random point read at 50M elements.

### Write

| Value | levisdb | goleveldb |
| --- | --- | --- |
| 16 B | **12.76 s** | 21.44 s |
| 256 B | **78.65 s** | 138.19 s |

### Random point read

| Value | levisdb | goleveldb |
| --- | --- | --- |
| 16 B | **2.67 µs/op** | 11.27 µs/op |
| 256 B | **3.22 µs/op** | 31.01 µs/op |

At 50M elements levisdb stays at a few microseconds per read while goleveldb slows
as it traverses a deeper leveled tree with more cold-cache misses; the gap widens
with value size. The 50M read figures are steady-state timings of random gets taken
after the load has settled.

## Overlap-scoped compaction (tier of 6 disjoint key ranges)

| Overlap selection | Time/op | Bytes merged/op |
| --- | --- | --- |
| on (default) | **17.7 ms** | **74 KB** |
| off | 82.6 ms | 444 KB |

## Block codecs

Ratio and per-block compress/decompress time for the four codecs on a 4 KiB
block, across three data shapes: highly repetitive, moderately compressible
structured data (mixed), and incompressible (random). All codecs are zero-alloc
at steady state.

### Moderately compressible (mixed structured data)

| Codec | Ratio | Compress | Decompress |
| --- | --- | --- | --- |
| none | 1.00x | 0.18 µs | 0.18 µs |
| s2 | 1.86x | 4.5 µs | 1.1 µs |
| flate | **3.11x** | 13.1 µs | 6.7 µs |
| zstd | 3.08x | 13.9 µs | 6.0 µs |

### Highly repetitive

| Codec | Ratio | Compress | Decompress |
| --- | --- | --- | --- |
| s2 | **89x** | 1.3 µs | 1.4 µs |
| zstd | 72x | 2.2 µs | 1.4 µs |
| flate | 64x | 2.1 µs | 1.9 µs |

### Incompressible (random)

All codecs fall back to storing the block raw (~1.00x); the size-check fallback
never ships a block larger than its payload.

Takeaways: s2 is fastest and wins on highly repetitive data; zstd and flate reach
a much higher ratio on moderately compressible structured data, where flate edges
zstd on ratio at comparable speed. flate fills the gap between s2's speed and
zstd's ratio for that mid-compressibility case; it is not the right choice for
highly repetitive data (s2 wins) or incompressible data (all tie at raw). Both
`FreshCodec` and `BottomCodec` default to s2; select flate or zstd per tier via
`BottomCodec` or `LevelCodecs` when the ratio is worth the extra CPU.
