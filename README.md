# d4m

In-memory data collections, typically with eviction to external memory such as SSD/NVMe, or similar storage.

## Overview

d4m provides low-latency data structures designed for workloads where data volume exceeds available heap memory. Hot data lives on the Java heap; cold data is transparently evicted to memory-mapped files, keeping the working set bounded while preserving access to the full dataset.

## Modules

| Module | Description |
|--------|-------------|
| `d4m-common` | Shared primitives: `AtomicBuffer`, `UnsafeBuffer`, bit/byte utilities |
| [`d4m-sequence`](d4m-sequence/README.md) | Ordered append-optimised sequence with COW inserts and cursor-based iteration |
| [`d4m-kv`](d4m-kv/README.md) | Tiered key-value store (`KeyValueStorage`) plus an append-only multi-value-per-key collection (`KeyListStorage`) built on top of it; hash-sharded segments and cascading eviction |
| [`d4m-benchmark`](d4m-benchmark/README.md) | JMH benchmarks |
| `d4m-example` | Usage examples for both modules |

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

Headline numbers from a short JMH run on an Apple M1 Pro (32 GB,
macOS 26.4.1, Amazon Corretto 21.0.3). These are **indicative only** --
short single-iteration measurements meant to give a rough sense of
shape, not to be cited as a published result.

For the full breakdown -- both eviction profiles side by side, per-op
explanations, the cache-vs-mmap analysis, and how to reproduce -- see
[**`d4m-benchmark/README.md`**](d4m-benchmark/README.md).

### `d4m-kv` -- `noEviction` (ops/sec)

The `noEviction` profile keeps the entire working set in heap. The
sub-project README also reports an `evict30` profile (32 MB hot tier per
segment, sized larger than the M1 Pro 24 MB SLC, with ~30 % of data
spilled to mmap) -- a realistic eviction workload that takes both real
CPU-cache misses and mmap accesses.

| Operation | `KeyValues` | `KeyLists` |
|---|---:|---:|
| write (single-thread `put` / `append`) | 12.45 M | 412 K |
| read (single-thread `get` / `list`+`forEach`) | 13.04 M | 777 K¹ |
| concurrent `rw1` (1 W + 1 R, 16 segments) | 15.38 M | 1.21 M |
| concurrent `rw10` (1 W + 10 R, 16 segments) | 22.68 M | 856 K |

¹ Each `KeyListsReadBenchmark.list` op delivers 10 entries -- entry-level throughput is ~7.8 M/sec.

### `d4m-sequence` (ops/sec, `writeProfile=APPEND_100`, forward cursor)

| Benchmark | Sequences | `chunkSize=65 536` | `chunkSize=131 072` | `chunkSize=524 288` |
|---|---|---:|---:|---:|
| `WriteBenchmark.write` | 1 | 21.83 M | 22.55 M | 22.81 M |
| `WriteBenchmark.write` | 1024 | 10.87 M | 10.55 M | 10.68 M |
| `HistoricalReadBenchmark.read` | 1024 | 112.91 M | 166.78 M | 145.01 M |
| `RealtimeBroadcastBenchmark.broadcast` | 1 | 111.85 M | 128.91 M | 114.90 M |
| `ScaledBroadcastBenchmark.twoReaders` | 1024 | 160.17 M | 149.65 M | 128.17 M |

## License

MIT License. See [LICENSE](LICENSE) for details.

