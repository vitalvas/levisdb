# Benchmarks

## Write (1,000,000 elements)

| Value | levisdb (single) | levisdb (8 writers) | goleveldb |
| --- | --- | --- | --- |
| 16 B | 686 ms | **275 ms** | 412 ms |
| 256 B | 2.55 s | **1.75 s** | 1.81 s |

## Random point read (1,000,000 elements)

| Value | levisdb | goleveldb |
| --- | --- | --- |
| 16 B | **625 ns/op** (138 B, 5 allocs) | 2,922 ns/op (1,485 B, 17 allocs) |
| 256 B | **1,074 ns/op** (581 B, 5 allocs) | 3,124 ns/op (1,200 B, 17 allocs) |
