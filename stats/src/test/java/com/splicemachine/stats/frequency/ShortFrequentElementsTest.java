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
public class ShortFrequentElementsTest{

    @Test
    public void testCanFetchAllElementsEmptyCollection() throws Exception{
        ShortFrequentElements dfe = ShortFrequentElements.topK(10,0l,Collections.<ShortFrequencyEstimate>emptyList());
        Set<? extends FrequencyEstimate<Short>> frequencyEstimates=dfe.allFrequentElements();
        Assert.assertEquals("Incorrect size!",0,frequencyEstimates.size());
    }

    @Test
    public void testMerge() throws Exception {
        List<ShortFrequencyEstimate> frequencyEstimates1 = new LinkedList<>();
        frequencyEstimates1.add(new ShortValueEstimate((short)10, 200, 1));
        frequencyEstimates1.add(new ShortValueEstimate((short)30, 100, 1));
        frequencyEstimates1.add(new ShortValueEstimate((short)40, 150, 1));
        ShortFrequentElements merged = ShortFrequentElements.topK(10,3l,frequencyEstimates1);

        List<ShortFrequencyEstimate> frequencyEstimates2 = new LinkedList<>();
        frequencyEstimates2.add(new ShortValueEstimate((short)50, 200, 1));
        frequencyEstimates2.add(new ShortValueEstimate((short)60, 100, 1));
        frequencyEstimates2.add(new ShortValueEstimate((short)70, 150, 1));
        ShortFrequentElements other = ShortFrequentElements.topK(10,3l,frequencyEstimates2);

        merged.merge(other);
        Set<? extends FrequencyEstimate<Short>> elements = merged.allFrequentElements();
        Assert.assertEquals("incorrect size", frequencyEstimates1.size()+frequencyEstimates2.size(), elements.size());
        //Make sure no elements from other is in merge
        for (FrequencyEstimate<Short> estimate:elements) {
            for (ShortFrequencyEstimate o : frequencyEstimates2) {
                Assert.assertTrue(estimate != o);
            }
        }
    }
}
