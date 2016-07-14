/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
public class DoubleFrequentElementsTest{

    @Test
    public void testCanFetchAllElementsEmptyCollection() throws Exception{
        DoubleFrequentElements dfe = DoubleFrequentElements.topK(10,0l,Collections.<DoubleFrequencyEstimate>emptyList());
        Set<? extends FrequencyEstimate<Double>> frequencyEstimates=dfe.allFrequentElements();
        Assert.assertEquals("Incorrect size!",0,frequencyEstimates.size());
    }

    @Test
    public void testMerge() throws Exception {
        List<DoubleFrequencyEstimate> frequencyEstimates1 = new LinkedList<>();
        frequencyEstimates1.add(new DoubleValueEstimate(10.0d, 200, 1));
        frequencyEstimates1.add(new DoubleValueEstimate(30.0d, 100, 1));
        frequencyEstimates1.add(new DoubleValueEstimate(40.0d, 150, 1));
        DoubleFrequentElements merged = DoubleFrequentElements.topK(10,3l,frequencyEstimates1);

        List<DoubleFrequencyEstimate> frequencyEstimates2 = new LinkedList<>();
        frequencyEstimates2.add(new DoubleValueEstimate(50.0d, 200, 1));
        frequencyEstimates2.add(new DoubleValueEstimate(60.0d, 100, 1));
        frequencyEstimates2.add(new DoubleValueEstimate(70.0d, 150, 1));
        DoubleFrequentElements other = DoubleFrequentElements.topK(10,3l,frequencyEstimates2);

        merged.merge(other);
        Set<? extends FrequencyEstimate<Double>> elements = merged.allFrequentElements();
        Assert.assertEquals("incorrect size", frequencyEstimates1.size()+frequencyEstimates2.size(), elements.size());
        //Make sure no elements from other is in merge
        for (FrequencyEstimate<Double> estimate:elements) {
            for (DoubleFrequencyEstimate o : frequencyEstimates2) {
                Assert.assertTrue(estimate != o);
            }
        }
    }
}
