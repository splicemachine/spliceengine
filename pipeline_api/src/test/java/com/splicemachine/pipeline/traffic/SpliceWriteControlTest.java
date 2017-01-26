/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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