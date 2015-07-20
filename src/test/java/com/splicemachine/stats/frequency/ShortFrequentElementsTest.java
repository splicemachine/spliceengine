package com.splicemachine.stats.frequency;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 7/20/15
 */
public class ShortFrequentElementsTest{

    @Test
    public void testCanFetchAllElementsEmptyCollection() throws Exception{
        ShortFrequentElements dfe = ShortFrequentElements.topK(10,0l,Collections.<ShortFrequencyEstimate>emptyList());
        Set<? extends FrequencyEstimate<Short>> frequencyEstimates=dfe.allFrequentElements();
        Assert.assertEquals("Incorrect size!",0,frequencyEstimates.size());
    }
}
