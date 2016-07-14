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

package com.splicemachine.si.impl.hlc;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by jleach on 4/21/16.
 */
public class HLCTest {

    @Test
    public void alwaysIncreasingLocalEvent() {
        HLC hlc = new HLC();
        long value = 0;
        for (int i = 0; i< 1000000; i++) {
            long comparison = hlc.sendOrLocalEvent();
            Assert.assertTrue("went backwards, catastophic",value<comparison);
            value = comparison;
        }
    }
}
