# d4m

In-memory data collections, typically with eviction to external memory such as SSD/NVMe, or similar storage.

## Overview

d4m provides low-latency data structures designed for workloads where data volume exceeds available heap memory. Hot data lives on the Java heap; cold data is transparently evicted to memory-mapped files, keeping the working set bounded while preserving access to the full dataset.

## Modules

| Module                                     | Description                                                                                                                                                                       |
|--------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `d4m-common`                               | Shared primitives: `AtomicBuffer`, `UnsafeBuffer`, bit/byte utilities                                                                                                             |
| [`d4m-sequence`](d4m-sequence/README.md)   | Ordered append-optimised sequence with COW inserts and cursor-based iteration                                                                                                     |
| [`d4m-kv`](d4m-kv/README.md)               | Tiered key-value store (`KeyValueStorage`) plus an append-only multi-value-per-key collection (`KeyListStorage`) built on top of it; hash-sharded segments and cascading eviction |
| [`d4m-benchmark`](d4m-benchmark/README.md) | JMH benchmarks                                                                                                                                                                    |
| `d4m-example`                              | Usage examples for both modules                                                                                                                                                   |

## Building

```bash
./gradlew build
```

JVM flags required for `Unsafe` access:

```
--add-opens java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens java.base/java.nio=ALL-UNNAMED
```

## Benchmarks (indicative)

Headline numbers from a moderate JMH run
(`-wi 3 -w 5 -i 3 -r 10 -f 1`) on an Apple M1 Pro (32 GB,
macOS 26.4.1, Amazon Corretto 21.0.3). These are **indicative only** --
three warmup + three measurement iterations per parametrised
benchmark, meant to give a rough sense of shape, not to be cited as a
published result.

For the full breakdown -- both eviction profiles side by side, per-op
explanations, the cache-vs-mmap analysis, and how to reproduce -- see
[**`d4m-benchmark/README.md`**](d4m-benchmark/README.md).

### `d4m-kv` -- eviction profiles

Both profiles share the same **32 MB hot tier per segment**. The
"30" in `evict30` is the **spill percentage** -- evict30 writes
`1 / 0.70 ~ 1.43 x` more entries per JMH iteration than noEviction
so that 30 % of the working set overflows to mmap. noEviction's
working set fits 1 : 1 in the hot tier (0 % mmap). Single-thread
benchmarks run in `Mode.SingleShotTime` so the op count per
iteration is fixed; concurrent benchmarks pre-populate the same
ranges and cycle in-place updates.

`noEviction` table (ops/sec, short JMH config):

| Operation                                     | `KeyValues` | `KeyLists` |
|-----------------------------------------------|------------:|-----------:|
| write (single-thread `put` / `append`)        |       5.14M |      3.52M |
| read (single-thread `get` / `list`+`forEach`) |       7.61M |     609K ¹ |
| concurrent `rw1` (1 W + 1 R, 16 segments)     |       9.20M |       528K |
| concurrent `rw10` (1 W + 10 R, 16 segments)   |      15.42M |       271K |

¹ Each `KeyListsReadBenchmark.list` op delivers 10 entries -- entry-level throughput is \~6.09 M entries/sec.

The write rows above use `mode = insert` (fresh-key inserts into an
empty store); `mode = update` (cyclic in-place updates of a
pre-populated store) and the `evict30` side-by-side comparison live
in [`d4m-benchmark/README.md`](d4m-benchmark/README.md).

### `d4m-sequence` (ops/sec, `writeProfile=APPEND_100`, forward cursor)

Chunks come from a two-tier allocator (heap pool + mmap overflow). The
65 K column has the pre-populated set spilling to mmap (~44 % on mmap);
the 131 K and 524 K columns fit in the heap pool. See
[`d4m-benchmark/README.md`](d4m-benchmark/README.md) for the fit table
and per-benchmark hot-vs-mmap interpretation.

| Benchmark                                        | Sequences | `eviction, 65 K` | `hot, 131 K` | `hot, 524 K` |
|--------------------------------------------------|-----------|-----------------:|-------------:|-------------:|
| `SequenceWriteBenchmark.write`                   | 1         |            6.11M |        3.00M |        2.89M |
| `SequenceWriteBenchmark.write`                   | 1024      |            5.92M |        5.27M |        2.26M |
| `SequenceHistoricalReadBenchmark.read`           | 1024      |          112.66M |      166.75M |      144.72M |
| `SequenceRealtimeBroadcastBenchmark.broadcast`   | 1         |          172.48M |      285.71M |      252.98M |
| `SequenceScaledBroadcastBenchmark.twoReaders`    | 1024      |          152.19M |      147.10M |      132.84M |

## License

MIT License. See [LICENSE](LICENSE) for details.

