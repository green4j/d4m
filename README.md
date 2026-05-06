# d4m

In-memory data collections, typically with eviction to external memory such as SSD/NVMe, or similar storage.

## Overview

d4m provides low-latency data structures designed for single-writer/multi-reader workloads where data volume exceeds available heap memory. Hot data lives on the Java heap; cold data is transparently evicted to memory-mapped files, keeping the working set bounded while preserving access to the full dataset.

## Modules

| Module | Description |
|--------|-------------|
| `d4m-common` | Shared primitives: `AtomicBuffer`, `UnsafeBuffer`, bit/byte utilities |
| `d4m-sequence` | Ordered append-optimised sequence with COW inserts and cursor-based iteration |
| `d4m-kv` | Tiered key-value store with hash-sharded segments and cascading eviction |
| `d4m-benchmark` | JMH benchmarks |
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

## License

MIT License. See [LICENSE](LICENSE) for details.
