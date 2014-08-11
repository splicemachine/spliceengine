package com.splicemachine.stats.histogram;

/**
 * @author Scott Fines
 *         Date: 4/30/14
 */
public interface IntRangeQuerySolver extends RangeQuerySolver<Integer> {
		int min();

		int max();

		long after(int value, boolean equals);

		long before(int value, boolean equals);

		long equal(int value);

		long between(int startValue, int endValue,boolean inclusiveStart,boolean inclusiveEnd);
}
