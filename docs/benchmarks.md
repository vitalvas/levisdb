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
