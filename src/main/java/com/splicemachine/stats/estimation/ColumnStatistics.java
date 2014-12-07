package com.splicemachine.stats.estimation;

import com.splicemachine.stats.frequency.FrequentElements;

/**
 * Represents information about a specific column.
 *
 * @author Scott Fines
 *         Date: 10/27/14
 */
public interface ColumnStatistics<T> {

    /**
     * @return the number of {@code null} entries in the column.
     */
    long nullCount();

    /**
     * @return the number of entries which are <em>not</em> {@code null}
     */
    long nonNullCount();

    /**
     * @return the most frequently occurring values for the specified column. If there
     * are no such elements (e.g. because all elements have the same count), then this will
     * return an empty set. This will never return {@code null}.
     */
    FrequentElements<T> mostFrequentElements();

    /**
     * @return the <em>cardinality</em> of the column. The <em>cardinality</em>
     * of a column is the number of distinct elements in the column.
     */
    long cardinality();

    /**
     * @return the ratio of {@link #cardinality()}/{@link #nonNullCount()}.
     */
    float duplicateFactor();

    /**
     * @return the average size(in bytes) of the column value.
     */
    int averageSize();


}
