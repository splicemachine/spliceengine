package com.splicemachine.logicalstats.cardinality;

import com.splicemachine.logicalstats.Updateable;

/**
 * @author Scott Fines
 *         Date: 6/5/14
 */
public interface CardinalityEstimator<T> extends Updateable<T> {
		/**
		 * @return an estimate of the number of distinct items seen by this Estimate
		 */
		long getEstimate();
}
