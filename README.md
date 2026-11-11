# d4m

In-memory data collections, typically with eviction to external memory such as SSD/NVMe, or similar storage.

## Overview

d4m provides low-latency data structures designed for workloads where data volume exceeds available heap memory. Hot data lives in memory, on the Java heap or off-heap; cold data is transparently evicted to memory-mapped or plain files, keeping the working set bounded while preserving access to the full dataset.

## Modules

| Module                                     | Description                                                                                                                                                                       |
|--------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `d4m-common`                               | Shared primitives: `AtomicBuffer`, `UnsafeBuffer`, bit/byte utilities                                                                                                             |
| [`d4m-sequence`](d4m-sequence/README.md)   | Ordered append-optimised sequence with COW inserts and cursor-based iteration                                                                                                     |
| [`d4m-kv`](d4m-kv/README.md)               | Tiered key-value store (`KeyValueStorage`) plus an append-only multi-value-per-key collection (`KeyListStorage`) built on top of it; hash-sharded segments and cascading eviction |
| [`d4m-queue`](d4m-queue/README.md)         | Single-producer/single-consumer binary queue of off-heap segments with a disk journal or overflow; the managing thread owns all files                                             |
| [`d4m-benchmark`](d4m-benchmark/README.md) | JMH benchmarks                                                                                                                                                                    |
| `d4m-example`                              | Usage examples for the modules                                                                                                                                                    |

## Binary Data Layout

All collections keep binary data in buffers accessed through `AtomicBuffer` and place every stored item 8-byte aligned:

| Collection     | Starts 8-byte aligned | Padding                                       |
|----------------|-----------------------|-----------------------------------------------|
| `d4m-sequence` | entry payload         | after the payload, up to the next entry       |
| `d4m-kv`       | entry, key and value  | the key is padded to 8 bytes before the value |
| `d4m-queue`    | message payload       | after the payload, up to the next frame       |

This lets an application encode and decode its data in place with direct primitive access: `getLong`/`putLong` and the like at fixed offsets of its own schema, or a flyweight codec (e.g. SBE over an Agrona buffer wrapping the same memory: the native address `AtomicBuffer.addressOffset() + offset` for a direct buffer, the backing array of `AtomicBuffer.byteBuffer()` at its array offset plus `offset` for a heap one), without copying. Since an item starts aligned, a field is aligned exactly when its offset within the item is, and multi-byte reads and writes avoid the cost of misaligned access. All multi-byte accessors use native byte order.

- Padding bytes have undefined content: read only within the item's length.
- The alignment holds for the absolute address in direct, memory-mapped and heap memory alike, as long as the buffer a collection is built on starts 8-byte aligned. The collections allocate their own buffers that way; a buffer passed in by the application (e.g. to `KeyValueBuffer`) is its responsibility. Heap memory is allocated with `UnsafeBuffer.allocateHeapAligned`, which aligns it whatever offset the JVM gives to the data of a `byte[]` (e.g. with `-XX:+UseCompactObjectHeaders`). Buffers that are used for volatile, ordered or CAS access are checked with `AtomicBuffer.verifyAlignment()` when they are created, since misaligned atomic access can tear values or crash the JVM on some platforms.
- `d4m-kv` reads copy the key and the value into a buffer supplied by the consumer, at the offsets it chooses (`KeyValueConsuming.KeyValue.valueOffset()`), so the alignment there is up to the consumer; the stored key and value are accessed in place by an `EvictionListener`.

## Binaries

Binaries for Maven, Ivy, Gradle, and others can be found at
[https://central.sonatype.com/artifact/io.github.green4j/d4m-all](https://central.sonatype.com/artifact/io.github.green4j/d4m-all).

Example for Maven:

```xml
<dependency>
    <groupId>io.github.green4j</groupId>
    <artifactId>d4m-all</artifactId>
    <version>${d4m.version}</version>
</dependency>
```

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
columns are chunk sizes. Only `SequenceHistoricalReadBenchmark.read` has
a fixed hot-vs-mmap split (its pre-populated 65 K set spills ~44 % to
mmap; the 131 K and 524 K sets fit the heap pool); the other rows start
from an empty pool and allocate monotonically. See
[`d4m-benchmark/README.md`](d4m-benchmark/README.md) for the fit table
and per-benchmark interpretation.

| Benchmark                                        | Sequences | `65 K` | `131 K` | `524 K` |
|--------------------------------------------------|-----------|-------:|--------:|--------:|
| `SequenceWriteBenchmark.write`                   | 1         |   6.11M |    3.00M |    2.89M |
| `SequenceWriteBenchmark.write`                   | 1024      |   5.92M |    5.27M |    2.26M |
| `SequenceHistoricalReadBenchmark.read`           | 1024      | 112.66M |  166.75M |  144.72M |
| `SequenceRealtimeBroadcastBenchmark.broadcast`   | 1         | 172.48M |  285.71M |  252.98M |
| `SequenceScaledBroadcastBenchmark.twoReaders`    | 1024      | 152.19M |  147.10M |  132.84M |

### `d4m-queue` (messages/sec, 200-byte payload, `offer`)

`transfer`: one writer, one reader and the manager on separate threads, the writer at most 1 M messages ahead of the reader. `writeRead`: all three roles on one thread. See
[`d4m-benchmark/README.md`](d4m-benchmark/README.md) for both write paths and how to read the numbers.

| Benchmark                                  | `OVERFLOW` | `JOURNAL` |
|--------------------------------------------|-----------:|----------:|
| `QueueWriteReadBenchmark.writeRead` ¹      |     24.57M |     8.98M |
| `QueueTransferBenchmark.transfer:messages` |     31.61M |    16.31M |

¹ Writer, reader and manager on the same thread, no cross-core transfer.

Latency (`QueueLatencyBenchmark`, `Mode.SampleTime`): a round trip writer -> reader -> writer takes **125 ns at p50 and 583 ns at p99** while the reader keeps up. When the reader is slower than the writer, a burst builds a backlog: writing 1 M messages until a reader spending \~125 ns each has consumed the last one takes 163 ms at p50, with the backlog overflowing a 64 MB memory pool to disk and back.

## License

MIT License. See [LICENSE](LICENSE) for details.

