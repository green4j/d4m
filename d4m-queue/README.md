# d4m-queue

Single-producer/single-consumer binary queue made of fixed-size off-heap segments. The segment being filled always lives in memory, so the reader follows the writer with the latency of a cache-line transfer; the disk either keeps everything (journal) or takes only what does not fit into the memory limit (overflow), which is loaded back ahead of the reader.

## Architecture

```
  Writer thread                    Reader thread
  offer() --+                      +-- poll()
            |     segment chain    |
            V                      V
   [seg]->[seg]->[seg]->[seg]->[active seg]     memory: resident segments
     |      |      ^
     |      |      | load ahead of reader
     V      V      |
   <dir>/00000000000000000000.seg ...           disk: journal or overflow segments
            ^
            |
  SegmentManager.doWork()  -- manager thread (an event loop of the application)
    supplies empty segments to the writer, persists, evicts, loads, deletes
```

- **`BinaryQueue`**, **`QueueWriter`**, **`QueueReader`**, **`MessageHandler`** -- the interfaces shared by queue implementations: binary messages, sequence numbers, non-blocking `offer`/`poll`.
- **`SegmentedQueue`** -- the `BinaryQueue` made of segments, the data plane: a writer, a reader and a chain of segments. It never performs I/O and never allocates on the hot path.
- **`SegmentManager`** -- the control plane: owns the memory pool, allocated in full when the manager is built, and all files of the queues it opened. Single-threaded: the application calls `doWork()` from its own loop. One manager can serve many queues; each queue is served by exactly one manager and lives in its own directory.

### Threading model

| Thread | Calls | Never does |
|---|---|---|
| writer (exactly one) | `QueueWriter.offer`, `claim`, `claimBuffer`, `commit`, `abort`, `close` | I/O, allocation, locks, waiting |
| reader (exactly one) | `QueueReader.poll`, `seek`, `position` | I/O, allocation, locks, waiting |
| manager (exactly one per manager) | `SegmentManager.open`, `doWork`, `segments`, `close`; `SegmentedQueue.close`; all `SegmentManager.Listener` callbacks | -- |
| any | `writerSeq`, `readerSeq`, `rejectedOffers`, `SegmentedQueue.persistedSeq`, `SegmentManager.maxMemoryBytes`, `usedMemoryBytes` | -- |

`offer`, `claim` and `poll` never block: `offer` and `claim` return `QueueWriter.REJECTED` when no prepared segment is available, `poll` returns `0` when there is nothing to deliver. Waiting is up to the caller.

## API

```java
SegmentManager manager = SegmentManager.builder()
        .withSegmentSize(32 * 1024 * 1024)        // bytes per segment
        .withMaxMemoryBytes(1024L * 1024 * 1024)  // memory pool of all queues, allocated by build()
        .withDiskMode(DiskMode.OVERFLOW)          // or JOURNAL
        .withFsync(false)
        .withDeleteConsumed(true)
        .withRetainSegments(0)                    // segments kept right behind the reader
        .withSpareSegments(2)                     // empty segments prepared for the writer
        .build();

SegmentedQueue queue = manager.open(Path.of("/data/queues/feed"), ReadMode.TAILING);

// manager thread (event loop)
while (running) {
    if (manager.doWork() == 0) {
        // idle as the application prefers
    }
}

// writer thread: copy a message ...
long seq = queue.writer().offer(buffer, offset, length); // seq or QueueWriter.REJECTED

// ... or encode it in place, without a copy
QueueWriter writer = queue.writer();
int at = writer.claim(maxLength);                        // or (int) QueueWriter.REJECTED
if (at != (int) QueueWriter.REJECTED) {
    int length = encode(writer.claimBuffer(), at);       // up to maxLength bytes
    long claimedSeq = writer.commit(length);             // or writer.abort()
}

// reader thread
int delivered = queue.reader().poll((seq, buffer, offset, length) -> {
    // the buffer region is valid during the call only
    return true; // false: not consumed, delivered again by the next poll
}, 256);

// stop the writer and the reader first, then, on the manager thread or after joining it:
queue.close();
manager.close(); // closes the queues still open, then the manager
```

Every message gets a sequence number: `0, 1, 2, ...`, continuing after a restart. `reader.seek(seq)` skips messages below `seq` (forward only), so a reader that remembers the last delivered sequence number resumes with `seek(saved + 1)` without duplicates.

### Disk modes and read modes

| | `ReadMode.TAILING` | `ReadMode.PERSISTED_ONLY` |
|---|---|---|
| `DiskMode.OVERFLOW` | the disk takes only what does not fit into memory | not allowed |
| `DiskMode.JOURNAL` | every message is written to disk shortly after `offer` or `commit` | the reader sees a message only after it is on disk |

- **`OVERFLOW`** -- when the memory limit is reached, the newest sealed segment ahead of the reader is written to disk and evicted. Evicted segments are loaded back ahead of the reader, evicting segments further ahead if needed. While the reader keeps up, no file is created until `queue.close()` writes the unconsumed segments and the newest one.
- **`JOURNAL`** -- the manager writes the committed prefix of every segment, including the one being filled, on every `doWork()` and publishes `persistedSeq`. `PERSISTED_ONLY` readers read from memory but stop at `persistedSeq`.

Segments the reader has passed are freed; their files are deleted when `deleteConsumed` is set. `retainSegments(n)` keeps `n` segments right behind the reader.

## Observability / Listeners

`SegmentedQueue` exposes counters readable from any thread: `writerSeq()`, `readerSeq()`, `persistedSeq()` and `rejectedOffers()`; the lag is `writerSeq() - readerSeq()`. `SegmentManager.segments(queue, consumer)` lists the live segments on the manager thread.

Structural events go to an optional `SegmentManager.Listener` (`withListener(...)`, `null` disables it). As in the other modules, every callback receives the raising manager as its first `notifier` argument plus primitives, allocates nothing and never runs on the hot path.

| Callback | When |
|---|---|
| `onSegmentRecovered` | `open()` found a segment file; only the newest file is read, so the `lastSeq` of the others comes from the next file name |
| `onSegmentTruncated` | a segment file had bytes after its last valid message (e.g. a torn write); they are ignored |
| `onSegmentPersisted` | a sealed segment, or the newest one on `close()`, has been completely written to disk |
| `onSegmentEvicted` | a segment ahead of the reader has been removed from memory |
| `onSegmentLoaded` | an evicted segment has been loaded back |
| `onSegmentDeleted` | the file of a consumed segment has been deleted |
| `onOffersRejected` | the writer had offers or claims rejected since the previous `doWork()` |

All callbacks run on the manager thread, inside `open()`, `doWork()` or `close()`; the writer and the reader only update counters. Implementations must be fast and must not call back into the manager.

## Format

Frame (8-byte aligned, so the payload is 8-byte aligned too):

| Bytes | Field | Notes |
|---|---|---|
| 0-3 | `length` | header + payload; written last with release semantics, so a non-zero length commits the frame |
| 4-7 | `flags` | `END` closes the segment |
| 8-15 | `seq` | sequence number |
| 16- | payload | |

Segment: a 64-byte header (`magic`, `version`, `segmentSize`, `baseSeq`) followed by frames. The writer links the next segment before it commits the `END` frame, so a reader that sees `END` always finds the next segment. The same layout is used in memory and in the file `<baseSeq, 20 digits>.seg`.

Segment buffers are reused without being cleared. Instead, the writer keeps the length word at the first uncommitted position zero: it zeroes the first frame's length when it starts a segment and the next frame's length before it commits each frame. Nobody reads past a zero length, so stale bytes further on do no harm.

Copies: `offer` copies the message once into the segment, `claim`/`commit` lets the writer encode in place, and the reader gets the message in the segment itself. The manager moves data only to and from disk.

### Recovery

`open()` lists the segment files of the directory: their names give the chain. Only the newest file is scanned to find where the writer continues. A frame is valid while its length fits the segment and its `seq` is exactly the expected one; everything after the first invalid frame is ignored, and the newest file is cut back to its last valid message. Older files are validated the same way when they are loaded for the reader. No CRC is computed: if the payload needs one, the application puts it into the message.

After `queue.close()` nothing is lost: segments the reader has passed are freed, the rest of the unconsumed segments is written (`JOURNAL` writes everything), and the newest segment is always written, so sequence numbers continue after reopening. If a write fails, `queue.close()` throws and leaves the queue open, so it can be retried; closing a closed queue does nothing. `SegmentedQueue` is `AutoCloseable`, so it fits try-with-resources on the manager thread (or after the manager thread has been joined), and `SegmentManager.close()` closes every queue still open the same way. After a crash the reader replays what is on disk from the oldest file: with `JOURNAL` everything up to the last write, with `OVERFLOW` only the segments that had overflowed to disk.

### Sequence numbers after a crash

The writer continues after the newest message found on disk. Messages that were offered but never reached the disk are lost in a crash, and their sequence numbers are given to new messages after the restart. A reader that has already seen such a lost message and resumes with `seek(saved + 1)` would skip new messages. So `seek(saved + 1)` is exact after a clean `close()`, and after a crash only with `DiskMode.JOURNAL` and `ReadMode.PERSISTED_ONLY`, where the reader never sees a message before it is on disk. In the other combinations, after a crash the application should resume from the oldest recovered message and deduplicate by its own means.

"On disk" means written to the segment file. Without `withFsync(true)` the data may still be in the OS page cache: it survives a crash of the process, but not a crash of the OS or a power loss, so the guarantee above holds against those only with `fsync`. With `fsync` every write is forced to the storage device before `persistedSeq` moves, and the queue directory is forced after a segment file is created or deleted, so the file itself survives too.

### Limitations

- One process per queue directory: the manager does not lock the directory, opening it from two processes at once corrupts the queue.
- Segment files are written in the native byte order of the machine; a file written on a machine of the other byte order is refused as foreign.
- Every `doWork()` walks all live segments of every queue of the manager. This is cheap for hundreds of segments; with thousands of live segments per queue, larger segments keep the step short.

## Memory Consumption

Segment memory is off-heap. `SegmentManager.Builder.build()` allocates the whole pool at once, `floor(maxMemoryBytes / segmentSize)` buffers of `segmentSize` bytes each (`ByteBuffer.allocateDirect`), and the manager never allocates segment memory afterwards. A shortage therefore shows up when the manager is built rather than in the middle of a burst, and the resident memory of the process does not grow while it runs. When the pool is used up, the manager evicts segments to disk (from any queue it serves), or the writer gets `QueueWriter.REJECTED`; it never allocates beyond the pool.

- **Direct memory limit**: `-XX:MaxDirectMemorySize` must cover `maxMemoryBytes` of all managers in the process, plus whatever else the application allocates directly. Otherwise `build()` throws `OutOfMemoryError`. Without the option the limit equals the maximum heap size.
- **Container limit**: the pool counts fully towards the process's resident memory from the start, on top of the heap. Files written by the manager also go through the OS page cache, which is charged to the container's memory cgroup. The kernel can reclaim it, but heavy writes may still throttle the process close to the limit.
- **Heap**: segment descriptors are pooled like the buffers. The pool grows to the largest number of live segments seen, about 250 bytes each, and is reused afterwards, so the writer and the reader allocate nothing, and neither does the manager while the queues stay in memory. Writing and loading segment files allocates a few small temporary objects per file operation. The number of live segments is bounded by the disk: a backlog of 1 TB in 32 MB segments is about 32,000 segments, some 8 MB of heap.
- **Disk**: not limited by the manager. With `OVERFLOW`, a reader that stays behind lets the queue grow until the disk is full; `doWork()` then throws `IOException`.

`SegmentManager.maxMemoryBytes()` returns the pool size and `usedMemoryBytes()` the part held by the queues (resident segments and segments prepared for the writers). Both are safe to read from any thread. The pool is released when the manager becomes unreachable, like any direct buffer.

A queue needs at least `spareSegments + 3 + retainSegments` segments: the active one, the spares, the reader's one, the retained ones and one to load or evict. `open()` refuses a queue whose minimum, together with the minimums of the queues already open, does not fit into the pool. This keeps every queue able to get a segment by evicting segments of the others.

A message of payload `p` takes `align8(16 + p)` bytes, so the per-message overhead is 16-23 bytes. A segment holds `floor((segmentSize - 64 - 16) / align8(16 + p))` messages: 155,344 messages of 200 bytes in a 32 MB segment.

## Performance (indicative)

Apple M1 Pro, macOS, Corretto 21, 200-byte messages, writer, reader and manager on separate threads, the reader busy-spinning. Measured with the examples below while other processes were running; numbers vary between runs.

| Measurement | Result |
|---|---|
| writer-to-reader latency, 1 message per 10 us (`QueueRealtimeLatency`) | min below the timer resolution, p50 ~85 ns, p90 ~290 ns, p99 ~1-1.5 us |
| throughput, `OVERFLOW`, 512 MB memory, the reader keeps up (`QueueThroughput`) | ~50 M messages/s with `offer`, ~46-48 M with `claim`/`commit` |
| throughput, `OVERFLOW`, the reader falls behind and the queue overflows to disk | ~9-10 M messages/s, bound by the disk (~2 GB/s) |
| throughput, `JOURNAL` (~3.4 GB/s to disk) | ~16 M messages/s |

The latency is bounded by the cross-core cache-line transfer. `System.nanoTime()` has a resolution of 41.67 ns on Apple silicon, so a minimum of 0 means the message arrived within one timer tick. For 200-byte messages, copying into the queue costs about as much as encoding in place; `claim`/`commit` mainly saves the application its own staging buffer.

JMH benchmarks (`QueueWriteReadBenchmark`, `QueueTransferBenchmark`, and `QueueLatencyBenchmark` for the latency when the reader keeps up and when a backlog builds and overflows to disk) and their results are in [`d4m-benchmark`](../d4m-benchmark/README.md#d4m-queue).

```bash
./gradlew :d4m-example:runExample -Pmain=io.github.green4j.d4m.example.queue.QueueRealtimeLatency
./gradlew :d4m-example:runExample -Pmain=io.github.green4j.d4m.example.queue.QueueThroughput -Dd4m.queue.diskMode=JOURNAL
./gradlew :d4m-example:runExample -Pmain=io.github.green4j.d4m.example.queue.QueueThroughput -Dd4m.queue.claim=true
```
