package com.splicemachine.stats.estimation;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public interface OrderedColumnStatistics<T extends Comparable<T>> extends ColumnStatistics<T> {
    /**
     * @return the minimum value for the column. This value excludes {@code null}, and is
     * therefore never null.
     */
    T minValue();

    /**
     * @return the number of times {@link #minValue()} occurred.
     */
    long minCount();

    /**
     * @return the maximum value for the column. This value excludes {@code null}, and is
     * therefore never null.
     */
    T maxValue();

    /**
     * @return the number of times {@link #maxValue()} occurred.
     */
    long maxCount();

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
    long between(T start,T stop, boolean includeMin,boolean includeMax);

    /**
     * Estimate the number of rows which have a column value equal to the
     * specified value. If {@code value==null}, then returns the number of rows
     * with a null entry for this column.
     *
     * @param value the value to estimate equality for
     * @return the (estimated) number of rows which are equal to the specified non-null
     * value
     */
    long equals(T value);
}
