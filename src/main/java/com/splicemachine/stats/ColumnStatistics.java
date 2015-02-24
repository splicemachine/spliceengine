package com.splicemachine.stats;

import com.splicemachine.stats.frequency.FrequentElements;

/**
 * Representation of Column-level statistics.
 *
 * Column statistics represent logical information about specific columns. Technically,
 * this is typed, but it is expected that there is a subinterface for primitive data types
 * in order to avoid auto-boxing where needed.
 *
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface ColumnStatistics<T> extends Mergeable<ColumnStatistics<T>> {

    /**
     * @return the cardinality of the Column values (e.g. the number of distinct
     *          elements) in the data set.
     */
    long cardinality();

    /**
     * @return the fraction of elements which are null relative to the count of records.
     */
    float nullFraction();

    /**
     * @return the number of rows which had a null value for this column
     */
    long nullCount();

    /**
     * @return the top {@code k} most frequently occurring values in this data set.
     *          {@code k}  is determined by configuration and collection.
     */
    FrequentElements<T> topK();

    /**
     * @return the minimum value for this column
     */
    T minValue();

    /**
     * @return the maximum value for this column
     */
    T maxValue();

    /**
     * @return the width of the column (in bytes)
     */
    long avgColumnWidth();

}
