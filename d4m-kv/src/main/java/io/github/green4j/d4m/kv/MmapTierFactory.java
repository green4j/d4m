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
import io.github.green4j.d4m.common.UnsafeBuffer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * A {@link TierFactory} that creates the first tier backed by heap or off-heap memory
 * and subsequent tiers backed by memory-mapped files. On construction, any stale
 * memory-mapped files from a previous run are cleaned up.
 */
public final class MmapTierFactory implements TierFactory, Closeable {

    /**
     * Listener for structural lifecycle events raised by {@link MmapTierFactory},
     * intended for logging and metrics.
     *
     * <p>Threading: {@link #onMemoryMappedFileFolderCleanup} is invoked during
     * factory construction on the constructing thread, with no lock held.
     * {@link #onMemoryTierCreated} and {@link #onMemoryMappedFileTierCreated}
     * fire when a tier is created on demand -- on a {@link KeyValueRing} this
     * happens inline on the caller's {@code put} / {@code compute} thread while
     * that segment's exclusive write lock is held (they may also fire at build
     * time when the store eagerly prepares mmap files). Implementations must be
     * fast and non-blocking and must not call back into the store.</p>
     */
    public interface Listener {
        /**
         * Called when a stale memory-mapped file is deleted during startup cleanup.
         *
         * @param notifier       the factory instance
         * @param mmapFileFolder the folder being cleaned
         * @param mmapFile       the file that was deleted
         */
        void onMemoryMappedFileFolderCleanup(MmapTierFactory notifier,
                                             File mmapFileFolder,
                                             File mmapFile);

        /**
         * Called when a new in-memory (heap or off-heap) tier buffer is created.
         *
         * @param notifier  the factory instance
         * @param size      the buffer size in bytes
         * @param isOffHeap {@code true} if off-heap memory was allocated
         */
        void onMemoryTierCreated(
                MmapTierFactory notifier,
                int size,
                boolean isOffHeap
        );

        /**
         * Called when a new memory-mapped file tier is created.
         *
         * @param notifier the factory instance
         * @param mmapFile the file that backs the new tier
         */
        void onMemoryMappedFileTierCreated(
                MmapTierFactory notifier,
                File mmapFile
        );
    }

    public static final String MMAP_FILE_PREFIX = "mmap-kv-";
    public static final String MMAP_FILE_EXTENSION = ".tmp";

    private final int id;
    private final int memoryTierSize;
    private final boolean memoryTierOffHeap;
    private final int memoryTierInitialCapacity;
    private final int mmapFileTierSize;
    private final int mmapFileTierInitialCapacity;
    private final File mmapFileFolder;
    private final int maxNumberOfTiers;
    private final Listener listener;

    /**
     * Creates a new factory that produces tiers for the given segment id.
     *
     * @param id                          the segment identifier used to namespace memory-mapped files
     * @param memoryTierSize              the byte size of the first (in-memory) tier buffer
     * @param memoryTierOffHeap           {@code true} to allocate the first tier off-heap
     * @param memoryTierInitialCapacity   the initial metadata capacity for the memory tier
     * @param mmapFileTierSize            the byte size of each memory-mapped file tier
     * @param mmapFileTierInitialCapacity the initial metadata capacity for mmap tiers
     * @param mmapFileFolder              the directory for memory-mapped files
     * @param maxNumberOfTiers            the maximum number of tiers this factory will create
     * @param listener                    optional listener for lifecycle events, or {@code null}
     */
    public MmapTierFactory(final int id,
                           final int memoryTierSize,
                           final boolean memoryTierOffHeap,
                           final int memoryTierInitialCapacity,
                           final int mmapFileTierSize,
                           final int mmapFileTierInitialCapacity,
                           final File mmapFileFolder,
                           final int maxNumberOfTiers,
                           final Listener listener) {
        this.id = id;
        this.memoryTierSize = memoryTierSize;
        this.memoryTierOffHeap = memoryTierOffHeap;
        this.memoryTierInitialCapacity = memoryTierInitialCapacity;
        this.mmapFileTierSize = mmapFileTierSize;
        this.mmapFileTierInitialCapacity = mmapFileTierInitialCapacity;
        this.mmapFileFolder = mmapFileFolder;
        this.maxNumberOfTiers = maxNumberOfTiers;
        this.listener = listener;

        cleanup();
    }

    /**
     * Returns the segment identifier associated with this factory.
     *
     * @return the id
     */
    public int id() {
        return id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Tier next(final Tier[] currentTiers,
                     final int currentSize,
                     final EvictionListener evictionListener) throws Exception {
        if (currentSize == maxNumberOfTiers) {
            return null;
        }

        final AtomicBuffer buffer;
        final int initialCapacity;

        switch (currentSize) {
            case 0: {
                buffer = createMemoryBuffer();
                initialCapacity = memoryTierInitialCapacity;
                break;
            }
            default: {
                buffer = createMemoryMappedBuffer(currentSize);
                initialCapacity = mmapFileTierInitialCapacity;
                break;
            }
        }

        return new Tier(
                initialCapacity,
                buffer,
                evictionListener
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
    }

    private void cleanup() {
        if (maxNumberOfTiers < 1) { // no plans to use memory mapped files
            return;
        }

        if (mmapFileFolder.exists()) {
            if (!mmapFileFolder.isDirectory()) {
                throw new IllegalArgumentException(mmapFileFolder + " is not a folder");
            }
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(
                    mmapFileFolder.toPath(),
                    MMAP_FILE_PREFIX + id + "-*" + MMAP_FILE_EXTENSION
            )) {
                for (final Path filePath : stream) {
                    if (Files.isRegularFile(filePath)) {
                        final File file = filePath.toFile();

                        if (!file.delete()) {
                            throw new RuntimeException();
                        }

                        if (listener != null) {
                            listener.onMemoryMappedFileFolderCleanup(
                                    this,
                                    mmapFileFolder,
                                    file
                            );
                        }
                    }
                }
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            mmapFileFolder.mkdirs();
        }
    }

    private AtomicBuffer createMemoryBuffer() {
        final AtomicBuffer result = new UnsafeBuffer(
                memoryTierOffHeap
                        ? ByteBuffer.allocateDirect(memoryTierSize)
                        : ByteBuffer.allocate(memoryTierSize)
        );

        if (listener != null) {
            listener.onMemoryTierCreated(
                    this,
                    memoryTierSize,
                    memoryTierOffHeap
            );
        }

        return result;
    }

    private AtomicBuffer createMemoryMappedBuffer(final int tierIndex) throws IOException {
        final File file = new File(
                mmapFileFolder,
                "mmap-kv-" + id + "-" + tierIndex + MMAP_FILE_EXTENSION
        );
        file.createNewFile();
        file.deleteOnExit();

        final AtomicBuffer result;

        try (FileChannel fileChannel = FileChannel.open(file.toPath(),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {

            fileChannel.write(
                    ByteBuffer.allocate(1),
                    mmapFileTierSize - 1
            );

            final MappedByteBuffer mappedBuffer = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE, 0, mmapFileTierSize
            );

            result = new UnsafeBuffer(
                    mappedBuffer
            );
        }

        if (listener != null) {
            listener.onMemoryMappedFileTierCreated(
                    this,
                    file
            );
        }

        return result;
    }
}
