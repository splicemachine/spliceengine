package com.splicemachine.stats.cardinality;

import com.splicemachine.stats.BytesUpdateable;

import java.nio.ByteBuffer;

/**
 * @author Scott Fines
 *         Date: 6/5/14
 */
public interface BytesCardinalityEstimator extends CardinalityEstimator<ByteBuffer>,BytesUpdateable{

    BytesCardinalityEstimator merge(BytesCardinalityEstimator other);

    BytesCardinalityEstimator getClone();
}
