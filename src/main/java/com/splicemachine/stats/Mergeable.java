package com.splicemachine.stats;

/**
 * @author Scott Fines
 *         Date: 10/27/14
 */
public interface Mergeable<T,M extends Mergeable<T,M>> {

    /**
     * Merge this entity with another copy.
     *
     * @param other the element to be merged
     */
    M merge(M other);
}
