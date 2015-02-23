package com.splicemachine.stats.cardinality;

import com.splicemachine.stats.Mergeable;
import com.splicemachine.stats.Updateable;

/**
 * @author Scott Fines
 *         Date: 6/5/14
 */
public interface CardinalityEstimator<T> extends Updateable<T>,Mergeable<CardinalityEstimator<T>> {
		/**
		 * @return an estimate of the number of distinct items seen by this Estimate
		 */
		long getEstimate();
}
