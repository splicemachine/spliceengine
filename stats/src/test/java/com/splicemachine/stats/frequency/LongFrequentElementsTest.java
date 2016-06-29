package com.splicemachine.stats.frequency;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
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

    @Test
    public void testMerge() throws Exception {
        List<LongFrequencyEstimate> frequencyEstimates1 = new LinkedList<>();
        frequencyEstimates1.add(new LongValueEstimate(10, 200, 1));
        frequencyEstimates1.add(new LongValueEstimate(30, 100, 1));
        frequencyEstimates1.add(new LongValueEstimate(40, 150, 1));
        LongFrequentElements merged = LongFrequentElements.topK(10,3l,frequencyEstimates1);

        List<LongFrequencyEstimate> frequencyEstimates2 = new LinkedList<>();
        frequencyEstimates2.add(new LongValueEstimate(50, 200, 1));
        frequencyEstimates2.add(new LongValueEstimate(60, 100, 1));
        frequencyEstimates2.add(new LongValueEstimate(70, 150, 1));
        LongFrequentElements other = LongFrequentElements.topK(10,3l,frequencyEstimates2);

        merged.merge(other);
        Set<? extends FrequencyEstimate<Long>> elements = merged.allFrequentElements();
        Assert.assertEquals("incorrect size", frequencyEstimates1.size()+frequencyEstimates2.size(), elements.size());
        //Make sure no elements from other is in merge
        for (FrequencyEstimate<Long> estimate:elements) {
            for (LongFrequencyEstimate o : frequencyEstimates2) {
                Assert.assertTrue(estimate != o);
            }
        }
    }
}
