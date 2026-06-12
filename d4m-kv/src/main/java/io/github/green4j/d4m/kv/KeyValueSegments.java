package io.github.green4j.d4m.kv;

public interface KeyValueSegments {

    /**
     * Returns the number of distinct segments in this ring.
     *
     * @return the number of segments
     */
    int numberOfSegments();

    /**
     * Returns the segment at the given index in the internal shuffled array.
     *
     * @param index the index into the shuffled segment array
     * @return the segment at the given index
     */
    KeyValueSegment getSegment(int index);

    /**
     * Returns the internal shuffled segment array.
     *
     * @return the segment array
     */
    KeyValueSegment[] segments();

    /**
     * Returns the total size of the internal shuffled segment array,
     * which equals {@code numberOfSegments * shuffleMultiplier}.
     *
     * @return the total array size
     */
    int size();
}
