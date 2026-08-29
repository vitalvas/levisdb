# Benchmarks

levisdb runs single-shard here so the comparison measures the storage engine, not
levisdb's sharding layer (goleveldb is not sharded). NoSync. The 1M-element results
are below; a 50M-element run is at the end.

## Write (1,000,000 elements)

| Value | levisdb | goleveldb |
| --- | --- | --- |
| 16 B | 503 ms | **341 ms** |
| 256 B | **931 ms** | 1.86 s |
| 1 KB | **1.88 s** | 6.64 s |

The write gap widens with value size: goleveldb's leveled write amplification grows
with the data moved per level, while levisdb's size-tiered path stays efficient.

## Random point read (1,000,000 elements)

| Value | levisdb | goleveldb |
| --- | --- | --- |
| 16 B | **560 ns/op** (137 B, 5 allocs) | 2,642 ns/op (1,473 B, 17 allocs) |
| 256 B | **1,003 ns/op** (583 B, 5 allocs) | 2,920 ns/op (1,186 B, 17 allocs) |

## Full scan (1,000,000 elements)

| Value | levisdb | goleveldb |
| --- | --- | --- |
| 16 B | 134 ms (9.2 MB, 21.9k allocs) | **54 ms** (14.3 MB, 53.8k allocs) |
| 256 B | **84 ms** (9.3 MB, 188k allocs) | 172 ms (45.6 MB, 441k allocs) |

## Large scale (50,000,000 elements)

Bulk load and random point read at 50M elements.

### Write

| Value | levisdb | goleveldb |
| --- | --- | --- |
| 16 B | **14.97 s** | 21.68 s |
| 256 B | **86.9 s** | 139.7 s |

### Random point read

| Value | levisdb | goleveldb |
| --- | --- | --- |
| 16 B | **2.44 µs/op** | 79.9 µs/op |
| 256 B | **2.93 µs/op** | 91.0 µs/op |

The read gap widens to ~30x at 50M: goleveldb traverses a deep leveled tree with
cold-cache misses, while levisdb stays at a few microseconds. The 50M read figures
are steady-state timings of random gets taken after the load has settled.

## Overlap-scoped compaction (tier of 6 disjoint key ranges)

| Overlap selection | Time/op | Bytes merged/op |
| --- | --- | --- |
| on (default) | **20.7 ms** | **74 KB** |
| off | 81.1 ms | 444 KB |
