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

package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.store.access.SortCostController;

/**
 * CostController for a TEMP-table based algorithm for computing Grouped Aggregates.
 *
 * The TEMP base algorithm is separated into a <em>parallel</em> and <em>sequential</em>
 * phase. The parallel phase always occurs, and contributes its cost
 * to the local portion of the sequential phase.
 *
 * -----
 * <h2>Costing the Parallel Phase</h2>
 * For each partition in the underlying scan, we submit a parallel task which reads
 * all data, then pushes that data into temp sorted according to key columns. We
 * assume that data is uniformly distributed across the known partitions (which
 * is a poor assumption in many cases, but for v1.0 will work acceptably), so
 * we know that each task will read 1/numPartitions worth of the rows. As a result,
 * the parallel cost is
 *
 * parallelCost = (baseCost.localCost+baseCost.remoteCost)/numPartitions
 *
 * The output partition count is 1 (until we modify the Sort operation to
 * use multiple buffers, which we can't do unless we know it's the top operation).
 *
 * -----
 * <h2>Costing the Sequential Phase</h2>
 * The sequential phase merely reads the total data set out over the network,
 * so it inherits the remote cost of the previous operation.
 *
 * finalLocalCost = parallelCost + baseCost.remoteCost;
 *
 * @author Scott Fines
 *         Date: 3/26/15
 */
public class TempSortController implements SortCostController{
    public TempSortController() {
    }

    @Override public void close(){  }

    @Override
    public void estimateSortCost(CostEstimate baseCost, CostEstimate sortCost) throws StandardException{
        if(baseCost.isUninitialized()) return; //don't do anything, we aren't real yet
        double parallelCost = (baseCost.localCost()+baseCost.remoteCost())/baseCost.partitionCount();
//        baseCost.setBase(baseCost.cloneMe());
        baseCost.setLocalCost(baseCost.localCost()+parallelCost);
        baseCost.setLocalCostPerPartition(baseCost.localCost());

        // set sortCost
        if (sortCost != null) {
            sortCost.setLocalCost(parallelCost);
            sortCost.setLocalCostPerPartition(parallelCost);
        }
    }
}
