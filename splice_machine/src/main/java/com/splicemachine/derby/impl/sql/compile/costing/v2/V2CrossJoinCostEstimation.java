/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.compile.costing.v2;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.compile.costing.SelectivityEstimator;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.impl.sql.compile.SelectivityUtil;
import com.splicemachine.derby.impl.sql.compile.costing.StrategyJoinCostEstimation;

public class V2CrossJoinCostEstimation implements StrategyJoinCostEstimation {
    @Override
    public void estimateCost(Optimizable innerTable,
                             OptimizablePredicateList predList,
                             ConglomerateDescriptor cd,
                             CostEstimate outerCost,
                             CostEstimate innerCost,
                             Optimizer optimizer,
                             SelectivityEstimator selectivityEstimator) throws StandardException {

        if(outerCost.isUninitialized() ||(outerCost.localCost()==0d && outerCost.getEstimatedRowCount()==1.0)){
            /*
             * Derby calls this method at the end of each table scan, even if it's not a join (or if it's
             * the left side of the join). When this happens, the outer cost is still unitialized, so there's
             * nothing to do in this method;
             */
            RowOrdering ro = outerCost.getRowOrdering();
            if(ro!=null)
                outerCost.setRowOrdering(ro); //force a cloning
            return;
        }

        // Only use cross join when it is inner join and run on Spark
        innerCost.setBase(innerCost.cloneMe());
        double joinSelectivity =
                SelectivityUtil.estimateJoinSelectivity(selectivityEstimator, innerTable, cd, predList,
                                                        (long) innerCost.rowCount(), (long) outerCost.rowCount(),
                                                        outerCost, SelectivityEstimator.JoinPredicateType.ALL);
        double totalOutputRows = SelectivityUtil.getTotalRows(joinSelectivity, outerCost.rowCount(), innerCost.rowCount());
        double totalJoinedRows = outerCost.rowCount() * innerCost.rowCount();
        double joinCost = crossJoinStrategyLocalCost(innerCost, outerCost, totalJoinedRows);
        innerCost.setLocalCost(joinCost);
        innerCost.setLocalCostPerParallelTask(joinCost);
        double remoteCostPerPartition = SelectivityUtil.getTotalPerPartitionRemoteCost(innerCost, outerCost, optimizer);
        innerCost.setRemoteCost(remoteCostPerPartition);
        innerCost.setRemoteCostPerParallelTask(remoteCostPerPartition);
        innerCost.setEstimatedHeapSize((long) SelectivityUtil.getTotalHeapSize(innerCost, outerCost, totalOutputRows));
        innerCost.setRowCount(totalOutputRows);
        innerCost.setRawRowCount(totalOutputRows);
        innerCost.setParallelism(outerCost.getParallelism());
        innerCost.setRowOrdering(null);
    }

    /**
     *
     * Cross Join Local Cost Computation
     *
     * Total Cost = (Left Side Cost)/Left Side Partition Count) + (Right Side Transfer Cost) +
     * (Right Side Cost) + (Joining Row Cost)
     *
     * @param innerCost
     * @param outerCost
     * @return
     */

    public static double crossJoinStrategyLocalCost(CostEstimate innerCost, CostEstimate outerCost,
                                                    double numOfJoinedRows) {
        SConfiguration config = EngineDriver.driver().getConfiguration();
        double localLatency = config.getFallbackLocalLatency();
        double joiningRowCost = numOfJoinedRows * localLatency;
        assert outerCost.getLocalCostPerParallelTask() != 0d || outerCost.localCost() == 0d;
        assert innerCost.getLocalCostPerParallelTask() != 0d || innerCost.localCost() == 0d;
        assert innerCost.getRemoteCostPerParallelTask() != 0d || innerCost.remoteCost() == 0d;
        assert outerCost.getRemoteCostPerParallelTask() != 0d || outerCost.remoteCost() == 0d;
        double innerLocalCost = innerCost.getLocalCostPerParallelTask()*innerCost.getParallelism();
        double innerRemoteCost = innerCost.getRemoteCostPerParallelTask()*innerCost.getParallelism();
        return outerCost.getLocalCostPerParallelTask() +
                innerRemoteCost + innerLocalCost +
                joiningRowCost;
    }
}
