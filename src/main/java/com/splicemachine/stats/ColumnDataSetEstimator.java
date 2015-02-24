package com.splicemachine.stats;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface ColumnDataSetEstimator<E> {

    /**
     * Estimate the number of entries which match the given value.
     *
     * @param value the value to check, or {@code null}. If {@code value ==null}, then
     *              this is equivalent to {@link #nullCount()}.
     * @return the number of entries which match the given value
     */
    long equals(E value);

    /**
     * Estimate the number of null entries in the Data Set.
     *
     * @return the number of null entries contains in the data set (estimated).
     */
    long nullCount();

    /**
     * Estimate the number of entries contains in the specified range, excluding null values
     *
     * @param start the start of the range. If {@code start ==null}, then this will find all
     *              elements which occur at (if {@code includeStop==true}) or before {@code stop}. If
     *              {@code stop} is also null, then this will return the number of entries in the data set.
     * @param stop the end of the range. if {@code stop==null}, then this will find all elements
     *             which occur at (if {@code includeStart==true}) or after {@code start}. If {@code start}
     *             is also null, then this will return the number of entries in the data set.
     * @param includeStart {@code true } if the start should be included (i.e. if the range is [start,stop
     * @param includeStop {@code true} if the stop should be included (i.e. if the range is start,stop])
     * @return the estimated number of entries in the specified range.
     */
    long count(E start, E stop, boolean includeStart, boolean includeStop);

    /**
     * Estimate the cardinality of the specified range, not including null entries.
     *
     * The <em>cardinality</em> is the number of distinct values in the data set (in this case,
     * in the range of data).
     *
     * @param start the start of the range. If {@code start ==null}, then this will find all
     *              elements which occur at (if {@code includeStop==true}) or before {@code stop}. If
     *              {@code stop} is also null, then this will return the cardinality of the data set.
     * @param stop the end of the range. if {@code stop==null}, then this will find all elements
     *             which occur at (if {@code includeStart==true}) or after {@code start}. If {@code start}
     *             is also null, then this will return the cardinality of the data set.
     * @param includeStart {@code true } if the start should be included (i.e. if the range is [start,stop
     * @param includeStop {@code true} if the stop should be included (i.e. if the range is start,stop])
     * @return the cardinality of the specified range
     */
    long cardinality(E start, E stop, boolean includeStart, boolean includeStop);


}
