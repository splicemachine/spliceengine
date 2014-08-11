package com.splicemachine.logicalstats.order;

import com.splicemachine.logicalstats.Updateable;

/**
 * @author Scott Fines
 *         Date: 6/5/14
 */
public interface MinMaxCollector<T extends Comparable<T>> extends Updateable<T> {

		T minimum();

		T maximum();
}
