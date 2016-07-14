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

package com.splicemachine.pipeline.traffic;

import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(ArchitectureIndependent.class)
public class SpliceWriteControlTest {

    @Test
    public void performIndependentWrite() {
        SpliceWriteControl writeControl = new AtomicSpliceWriteControl(3, 3, 200, 200);

        writeControl.performIndependentWrite(25);
        assertEquals("{ dependentWriteThreads=0, independentWriteThreads=1, dependentWriteCount=0, independentWriteCount=25 }", writeControl.getWriteStatus().toString());

        writeControl.performIndependentWrite(25);
        assertEquals("{ dependentWriteThreads=0, independentWriteThreads=2, dependentWriteCount=0, independentWriteCount=50 }", writeControl.getWriteStatus().toString());

        writeControl.finishIndependentWrite(25);
        assertEquals("{ dependentWriteThreads=0, independentWriteThreads=1, dependentWriteCount=0, independentWriteCount=25 }", writeControl.getWriteStatus().toString());
    }

}