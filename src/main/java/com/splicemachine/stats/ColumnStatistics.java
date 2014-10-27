package com.splicemachine.stats;

import com.splicemachine.stats.frequency.FrequencyEstimate;

import java.util.Set;

/**
 * Represents information about a specific column.
 *
 * @author Scott Fines
 *         Date: 10/27/14
 */
public interface ColumnStatistics<T extends Comparable<T>> {

    /**
     * @return the number of {@code null} entries in the column.
     */
    long nullCount();

    /**
     * @return the number of entries which are <em>not</em> {@code null}
     */
    long nonNullCount();

    /**
     * @return the minimum value for the column
     */
    T min();

    /**
     * @return the maximum value for the column
     */
    T max();

    /**
     * @return the most frequently occurring values for the specified column.
     */
    Set<FrequencyEstimate<T>> mostFrequentElements();

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
     * @return the average size(in number of bytes) of column values.
     */
    int averageSize();

    /**
     * Estimate the number of rows which have a column value between the two
     * specified values {@code start} and {@code stop}
     *
     * To match equals-type clauses, make {@code start.equals(stop)==true}.
     *
     * @param start the start value of the scan. To scan from the beginning of the table,
     *              specify {@code null}.
     * @param stop the stop value of the scan. To scan to the end of the table,
     *             specified {@code null}
     * @param includeMin if {@code true}, then scan >= {@code start}
     * @param includeMax if {@code true}, then scan <= {@code stop}
     * @return an estimate of the number of rows which match the given query
     */
    long estimateSize(T start,T stop, boolean includeMin,boolean includeMax);

}
