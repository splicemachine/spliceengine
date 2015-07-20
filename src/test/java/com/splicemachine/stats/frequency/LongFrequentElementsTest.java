package com.splicemachine.stats.frequency;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 7/20/15
 */
public class LongFrequentElementsTest{

    @Test
    public void testCanFetchAllElementsEmptyCollection() throws Exception{
        LongFrequentElements dfe = LongFrequentElements.topK(10,0l,Collections.<LongFrequencyEstimate>emptyList());
        Set<? extends FrequencyEstimate<Long>> frequencyEstimates=dfe.allFrequentElements();
        Assert.assertEquals("Incorrect size!",0,frequencyEstimates.size());
    }
}
