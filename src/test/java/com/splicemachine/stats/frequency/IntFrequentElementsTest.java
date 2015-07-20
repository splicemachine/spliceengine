package com.splicemachine.stats.frequency;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 7/20/15
 */
public class IntFrequentElementsTest{

    @Test
    public void testCanFetchAllElementsEmptyCollection() throws Exception{
        IntFrequentElements dfe = IntFrequentElements.topK(10,0l,Collections.<IntFrequencyEstimate>emptyList());
        Set<? extends FrequencyEstimate<Integer>> frequencyEstimates=dfe.allFrequentElements();
        Assert.assertEquals("Incorrect size!",0,frequencyEstimates.size());
    }
}
