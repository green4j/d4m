/*
 * MIT License
 *
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package io.github.green4j.d4m.kv;

import io.github.green4j.d4m.common.AtomicBuffer;

import java.io.File;
import java.nio.file.Paths;

/**
 * A {@link KeyValues} backed by an {@link AbstractKeyValueRing} of
 * {@link KeyValueSegment}s with tiered main memory and memory-mapped file storage.
 * Use {@link #builder()} to configure and construct instances.
 *
 * <p>{@link Builder#build()} returns a thread-safe instance backed by
 * {@link KeyValueRing}. {@link Builder#buildSingleThreaded()} returns a
 * non-thread-safe instance backed by {@link SingleThreadedKeyValueRing}; all
 * {@code put} and {@code get} calls on it must be made by a single thread.
 */
public final class KeyValueStorage implements KeyValues {

    /**
     * Creates a new {@link Builder} for configuring and constructing a {@link KeyValueStorage}.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }


    /**
     * Fluent builder for configuring a {@link KeyValueStorage} instance,
     * including main memory size, memory-mapped file settings, and ring parameters.
     */
    public static final class Builder {
        private long totalMainMemory = 128 * 1024 * 1024; // 128 MB
        private boolean useOffHeapMainMemory = false;
        private int mmapFileSize = 1024 * 1024 * 1024; // 1GB
        private int ringSize = 8;
        private int ringShuffleMultiplier = 32;
        private boolean prepareMmapFilesOnStart = true;
        private File mmapFilesFolder =
                Paths.get(System.getProperty("java.io.tmpdir")).toFile();
        private MmapTierFactory.Listener mmapTierListener =
                new MmapTierFactory.Listener() {
                    @Override
                    public void onMemoryMappedFileFolderCleanup(final MmapTierFactory notifier,
                                                                final File mmapFileFolder,
                                                                final File mmapFile) {
                        System.out.println("Memory Mapped File Folder Cleanup: "
                                + mmapFile.getAbsolutePath());
                    }

                    @Override
                    public void onMemoryTierCreated(final MmapTierFactory notifier,
                                                    final int size,
                                                    final boolean isOffHeap) {
                        System.out.println("Memory Tier Created: "
                                + size
                                + " bytes "
                                + (isOffHeap ? "[offh]" : "[heap]"));
                    }

                    @Override
                    public void onMemoryMappedFileTierCreated(final MmapTierFactory notifier,
                                                              final File mmapFile) {
                        System.out.println("Memory Mapped File Tier Created: "
                                + mmapFile.getAbsolutePath()
                                + ", size "
                                + mmapFile.length()
                                + " bytes");
                    }
                };

        private Builder() {
        }

        /**
         * Sets the total amount of main memory in bytes for all hot (Tier 0) regions combined.
         *
         * @param totalMainMemory the total main memory in bytes
         * @return this builder
         */
        public Builder withTotalMainMemory(final long totalMainMemory) {
            this.totalMainMemory = totalMainMemory;
            return this;
        }

        /**
         * Sets whether to allocate the hot tier using off-heap memory.
         *
         * @param useOffHeapMainMemory {@code true} to use off-heap, {@code false} for heap
         * @return this builder
         */
        public Builder withUseOffHeapMainMemory(final boolean useOffHeapMainMemory) {
            this.useOffHeapMainMemory = useOffHeapMainMemory;
            return this;
        }

        /**
         * Configures the hot tier to use off-heap memory.
         *
         * @return this builder
         */
        public Builder withOffHeapMainMemory() {
            this.useOffHeapMainMemory = true;
            return this;
        }

        /**
         * Configures the hot tier to use heap memory.
         *
         * @return this builder
         */
        public Builder withHeapMainMemory() {
            this.useOffHeapMainMemory = false;
            return this;
        }

        /**
         * Sets the size in bytes for each memory-mapped file tier.
         *
         * @param mmapFileSize the file size in bytes
         * @return this builder
         */
        public Builder withMmapFileSize(final int mmapFileSize) {
            this.mmapFileSize = mmapFileSize;
            return this;
        }

        /**
         * Sets the number of segments in the ring (rounded up to a power of two).
         *
         * @param ringSize the desired number of segments
         * @return this builder
         */
        public Builder withRingSize(final int ringSize) {
            this.ringSize = ringSize;
            return this;
        }

        /**
         * Sets the shuffle multiplier that controls internal segment array sizing
         * to distribute hash collisions more evenly.
         *
         * @param ringShuffleMultiplier the shuffle multiplier (rounded up to a power of two)
         * @return this builder
         */
        public Builder withRingShuffleMultiplier(final int ringShuffleMultiplier) {
            this.ringShuffleMultiplier = ringShuffleMultiplier;
            return this;
        }

        /**
         * Sets whether to eagerly create memory-mapped file tiers during construction.
         *
         * @param prepareMmapFilesOnStart {@code true} to create mmap tiers eagerly
         * @return this builder
         */
        public Builder withPrepareMmapFilesOnStart(final boolean prepareMmapFilesOnStart) {
            this.prepareMmapFilesOnStart = prepareMmapFilesOnStart;
            return this;
        }

        /**
         * Enables eager creation of memory-mapped file tiers during construction.
         *
         * @return this builder
         */
        public Builder withPrepareMmapFilesOnStart() {
            this.prepareMmapFilesOnStart = true;
            return this;
        }

        /**
         * Sets the directory where memory-mapped files will be created.
         *
         * @param mmapFilesFolder the target directory
         * @return this builder
         */
        public Builder withMemoryMappedFilesFolder(final File mmapFilesFolder) {
            this.mmapFilesFolder = mmapFilesFolder;
            return this;
        }

        /**
         * Sets the directory where memory-mapped files will be created, specified as a path string.
         *
         * @param folderPath the path to the target directory
         * @return this builder
         */
        public Builder withMemoryMappedFilesFolder(final String folderPath) {
            this.mmapFilesFolder = new File(folderPath);
            return this;
        }

        /**
         * Sets the Mmap Tier Factory Listener. If not set, a default implementation which logs
         * activity to {@code System.out} used.
         *
         * @param mmapTierListener the listener
         * @return this builder
         */
        public Builder withMmapTierListener(final MmapTierFactory.Listener mmapTierListener) {
            this.mmapTierListener = mmapTierListener;
            return this;
        }

        /**
         * Returns the configured total main memory in bytes.
         *
         * @return the total main memory
         */
        public long totalMainMemory() {
            return totalMainMemory;
        }

        /**
         * Returns whether off-heap memory is configured for the hot tier.
         *
         * @return {@code true} if off-heap
         */
        public boolean useOffHeapMainMemory() {
            return useOffHeapMainMemory;
        }

        /**
         * Returns the configured memory-mapped file size in bytes.
         *
         * @return the file size
         */
        public int mmapFileSize() {
            return mmapFileSize;
        }

        /**
         * Returns the configured ring size.
         *
         * @return the ring size
         */
        public int ringSize() {
            return ringSize;
        }

        /**
         * Returns the configured ring shuffle multiplier.
         *
         * @return the shuffle multiplier
         */
        public int ringShuffleMultiplier() {
            return ringShuffleMultiplier;
        }

        /**
         * Returns whether memory-mapped files are prepared on start.
         *
         * @return {@code true} if files are prepared on start
         */
        public boolean prepareMmapFilesOnStart() {
            return prepareMmapFilesOnStart;
        }

        /**
         * Returns the configured folder for memory-mapped files.
         *
         * @return the folder
         */
        public File mmapFilesFolder() {
            return mmapFilesFolder;
        }

        /**
         * Returns Mmap Tier Factory Listener.
         *
         * @return the listener
         */
        public MmapTierFactory.Listener mmapTierListener() {
            return mmapTierListener;
        }

        /**
         * Constructs a new thread-safe {@link KeyValueStorage} backed by a
         * {@link KeyValueRing}.
         *
         * @return a new storage instance
         */
        public KeyValueStorage build() {
            return new KeyValueStorage(this, false);
        }

        /**
         * Constructs a new non-thread-safe {@link KeyValueStorage} backed by a
         * {@link SingleThreadedKeyValueRing}. All {@code put} and {@code get} calls
         * must be made by a single thread.
         *
         * @return a new single-threaded storage instance
         */
        public KeyValueStorage buildSingleThreaded() {
            return new KeyValueStorage(this, true);
        }
    }

    private final AbstractKeyValueRing ring;

    private KeyValueStorage(final Builder parameters, final boolean singleThreaded) {
        final int memoryTierSize = (int) (parameters.totalMainMemory / parameters.ringSize);

        final int memoryTierInitialCapacity = 65536;
        final int mmapFileTierInitialCapacity = 65536;

        final SegmentFactory segmentFactory = index ->
                new KeyValueSegment(
                        parameters.prepareMmapFilesOnStart ? 2 : 1,
                        new MmapTierFactory(
                                index,
                                memoryTierSize,
                                parameters.useOffHeapMainMemory,
                                memoryTierInitialCapacity,
                                parameters.mmapFileSize,
                                mmapFileTierInitialCapacity,
                                parameters.mmapFilesFolder,
                                Integer.MAX_VALUE,
                                parameters.mmapTierListener
                        ),
                        null // no eviction with unlimited
                );

        if (singleThreaded) {
            ring = new SingleThreadedKeyValueRing(
                    parameters.ringSize,
                    parameters.ringShuffleMultiplier,
                    segmentFactory
            );
        } else {
            ring = new KeyValueRing(
                    parameters.ringSize,
                    parameters.ringShuffleMultiplier,
                    segmentFactory
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(final AtomicBuffer key,
                    final int keyOffset,
                    final int keySize,
                    final AtomicBuffer value,
                    final int valueOffset,
                    final int valueSize) {
        ring.put(
                key,
                keyOffset,
                keySize,
                value,
                valueOffset,
                valueSize
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean get(final AtomicBuffer key,
                       final int keyOffset,
                       final int keySize,
                       final KeyValueConsuming.ValueConsumer<KeyValueConsuming.Value> consumer) {
        return ring.get(
                key,
                keyOffset,
                keySize,
                consumer
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void compute(final AtomicBuffer key,
                        final int keyOffset,
                        final int keySize,
                        final ComputeAction action) {
        ring.compute(key, keyOffset, keySize, action);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void compute(final AtomicBuffer key1,
                        final int key1Offset,
                        final int key1Size,
                        final AtomicBuffer key2,
                        final int key2Offset,
                        final int key2Size,
                        final TwoKeyComputeAction action) {
        ring.compute(
                key1, key1Offset, key1Size,
                key2, key2Offset, key2Size,
                action
        );
    }

    /**
     * Returns {@link KeyValueSegments} used by this storage.
     * Concrete type is {@link KeyValueRing} for {@link Builder#build()} and
     * {@link SingleThreadedKeyValueRing} for {@link Builder#buildSingleThreaded()}.
     *
     * @return the ring
     */
    public KeyValueSegments ring() {
        return ring;
    }
}
