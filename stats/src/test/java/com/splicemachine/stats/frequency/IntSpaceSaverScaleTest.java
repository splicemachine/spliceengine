package com.splicemachine.stats.frequency;

import org.junit.Test;

/**
 * @author Scott Fines
 *         Date: 4/2/15
 */
public class IntSpaceSaverScaleTest{
    @Test
    public void testSequentialOrdering() throws Exception{
        int size = 1<<16;
        IntFrequencyCounter longFrequencyCounter=FrequencyCounters.intCounter(16);
        for(int i=0;i<size;i++){
            longFrequencyCounter.update(i,1l);
        }
    }
}
