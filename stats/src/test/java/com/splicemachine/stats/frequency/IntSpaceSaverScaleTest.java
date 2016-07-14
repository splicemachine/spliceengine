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
