package com.splicemachine.stats.order;

import com.splicemachine.stats.Updateable;

/**
 *
 * @author Scott Fines
 *         Date: 6/5/14
 */
public interface MinMaxCollector<T extends Comparable<T>> extends Updateable<T> {

		T minimum();

		T maximum();

		/**
		 * @return the number of times the minimum value was seen
		 */
		long minCount();

		/**
		 * @return the number of times the maximum was seen
		 */
		long maxCount();
}
