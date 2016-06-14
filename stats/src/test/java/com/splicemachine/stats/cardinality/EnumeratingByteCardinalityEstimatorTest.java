package com.splicemachine.stats.cardinality;

import org.junit.Test;

/**
 * @author Scott Fines
 *         Date: 10/7/14
 */
public class EnumeratingByteCardinalityEstimatorTest {

    @Test
    public void testSizeBitSet() throws Exception {
        for(int i=0;i<100000;i++){
            new EnumeratingByteCardinalityEstimator();
        }
    }
}
