package com.splicemachine.stats.frequency;

import org.junit.Test;

/**
 * Tests that run against a large volume of data (generally taken from real-world scenarios)
 *
 * @author Scott Fines
 *         Date: 4/2/15
 */
public class LongFrequencyCounterScaleTest{

    @Test
    public void testSequentialOrdering() throws Exception{
        long size =1<<20;
//        long size =Long.MAX_VALUE;
        LongFrequencyCounter longFrequencyCounter=FrequencyCounters.longCounter(16,32);
        for(long i=0;i<size;i++){
            longFrequencyCounter.update(i,1l);
        }
    }
}
