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

package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(ArchitectureIndependent.class)
public class CostUtilsTest {

//    @Test
//    public void isThisBaseTable() {
//        SpliceLevel2OptimizerImpl optimizer = new SpliceLevel2OptimizerImpl();
//        optimizer.outermostCostEstimate = new SpliceCostEstimateImpl();
//        optimizer.outermostCostEstimate.setEstimatedCost(0.00000000000001);
//        optimizer.outermostCostEstimate.setEstimatedRowCount(1);
//        optimizer.joinPosition = 0;
//
//        assertTrue(CostUtils.isThisBaseTable(optimizer));
//    }

    @Test
    public void add() {
        long result = CostUtils.add(20L, 20L);
        assertEquals(40L, result);


        result = CostUtils.add(Long.MAX_VALUE, 200L);
        assertEquals(Long.MAX_VALUE, result);
    }

}