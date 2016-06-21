package com.splicemachine.stats.histogram;

/**
 * @author Scott Fines
 *         Date: 4/30/14
 */
public interface RangeQuerySolver<T extends Comparable<T>> {
		/**
		 * Get the (estimated) number of elements which are contained
		 * within the specified bounds. {@code null} is treated as "from the very beginning.
		 *
		 *
		 * @param start the start of the range of interest, or {@code null} if the estimate
		 *              should be from the min element of the histogram.
		 * @param end the end of the range of interest, or {@code null} if the estimate should be
		 *            to the maximum element of the histogram.
		 * @param inclusiveStart whether or not to include an estimate of equality on the start key (e.g. if
		 *                       set to true, will do >= start. If false, will do > start)
		 * @param inclusiveEnd whether or not to include an estimate of equality on the end key (e.g. if set to true,
		 *                     will do <= end. If false, will do < end).
		 * @return the (estimated) number of elements in the specified range.
		 * @throws java.lang.IllegalArgumentException if {@code start.equals(end)}, and the underlying structure is
		 * 																						unable to estimate that quantity
		 */
		long getNumElements(T start, T end,boolean inclusiveStart,boolean inclusiveEnd);

		/**
		 * @return the minimum element in the histogram.
		 */
		T getMin();

		/**
		 * @return the maximum element in the histogram.
		 */
		T getMax();
}
