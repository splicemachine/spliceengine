package com.splicemachine.stats.frequency;

import com.splicemachine.stats.ByteUpdateable;

/**
 * Marker interface to denote a byte-specific frequency counter.
 *
 * @author Scott Fines
 * Date: 3/27/14
 */
public interface ByteFrequencyCounter extends ByteUpdateable, FrequencyCounter<Byte> {

    /**
     * Get all elements which have occurred more than {@code support*numVisited} times,
     * where {@code numVisitied} is the total number of elements that the counter has seen
     * in total. For example, to find all elements which occupy more than 10% of the table, we
     * would call {@code heavyHitters(0.1)}
     *
     * @param support the percentage of the table which is occupied by the defined "heavy hitter"
     * @return All elements (within the accuracy guaranteed by the algorithm) which have a frequency
     * of at least {@code support*numVisited}
     */
    ByteFrequentElements heavyHitters(float support);

    /**
     * Find the {@code k} most frequently seen elements. These ae
     *
     * <p>Note that the implementation is allowed to return fewer than {@code k} elements. This
     * is because some implementations cannot guarantee {@code k} elements in all cases (depending
     * on the configuration and the guarantees provided by that algorithm. No algorithm will ever
     * return <em>more</em> than {@code k} elements, however.
     *
     * @param k the maximum number of elements to return
     * @return up to {@code k} elements whose frequency exceeds all others.
     */
    ByteFrequentElements frequentElements(int k);
}
