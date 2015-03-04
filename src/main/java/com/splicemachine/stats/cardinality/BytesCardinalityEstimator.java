package com.splicemachine.stats.cardinality;

import com.splicemachine.stats.BytesUpdateable;

/**
 * @author Scott Fines
 *         Date: 6/5/14
 */
public interface BytesCardinalityEstimator extends CardinalityEstimator<byte[]>,BytesUpdateable{

    BytesCardinalityEstimator merge(BytesCardinalityEstimator other);

    BytesCardinalityEstimator getClone();
}
