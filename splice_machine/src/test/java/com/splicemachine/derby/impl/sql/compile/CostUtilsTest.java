/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
