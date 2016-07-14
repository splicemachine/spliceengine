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

import com.splicemachine.utils.ComparableComparator;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 7/20/15
 */
public class ObjectFrequentElementsTest{

    @Test
    public void testCanFetchAllElementsEmptyCollection() throws Exception{
        FrequentElements<BigDecimal> dfe = ObjectFrequentElements.topK(10,0l,
                Collections.<FrequencyEstimate<BigDecimal>>emptyList(),
                ComparableComparator.<BigDecimal>newComparator());
        Set<? extends FrequencyEstimate<BigDecimal>> frequencyEstimates=dfe.allFrequentElements();
        Assert.assertEquals("Incorrect size!",0,frequencyEstimates.size());
    }
}
