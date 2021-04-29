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
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.impl.sql.compile.FromBaseTable;
import com.splicemachine.db.impl.sql.compile.HashableJoinStrategy;
import com.splicemachine.db.impl.sql.compile.JoinNode;
import com.splicemachine.db.impl.sql.compile.SelectivityUtil;
import com.splicemachine.derby.impl.sql.compile.costing.StrategyJoinCostEstimation;

public class V2BroadcastJoinCostEstimation implements StrategyJoinCostEstimation {
    static final double LEFT_ONE_ROW_HASH_PROBE_COST     = 0.3;   // 0.3 microseconds
    static final double RIGHT_ONE_ROW_HASH_TRANSMIT_COST = 1.2;   // 1.2 microseconds

    @Override
    public void estimateCost(Optimizable innerTable,
                             OptimizablePredicateList predList,
                             ConglomerateDescriptor cd,
                             CostEstimate outerCost,
                             Optimizer optimizer,
                             CostEstimate innerCost) throws StandardException {
        /*
         * Broadcast Joins are relatively straightforward. Before scanning a single outer row,
         * it first reads all inner table rows into a local hashtable; then, as each outer row
         * is read, the inner hashtable is probed for any matching rows.
         *
         * The big effect here is that the cost to read the inner table is constant, regardless of
         * the join predicates (because the join predicate are applied after the inner table is read).
         *
         * totalCost.localCost = outerCost.localCost + innerCost.localCost+innerCost.remoteCost
         *
         * But the output metrics are different, based on the join predicates. Thus, we compute
         * the output joinSelectivity of all join predicates, and adjust the cost as
         *
         * totalCost.remoteCost = joinSelectivity*(outerCost.remoteCost+innerCost.remoteCost) --> revisit
         * totalCost.outputRows = joinSelectivity*outerCost.outputRows
         * totalCost.heapSize = joinSelectivity*(outerCost.heapSize + innerCost.heapSize)
         *
         * Note that we count the innerCost.remoteCost twice. This accounts for the fact that we
         * have to read the inner table's data twice--once to build the hashtable, and once
         * to account for the final scan of data to the control node.
         */
        if (outerCost.isUninitialized() || (outerCost.localCost() == 0d && outerCost.getEstimatedRowCount() == 1.0d)) {
            RowOrdering ro = outerCost.getRowOrdering();
            if (ro != null)
                outerCost.setRowOrdering(ro); //force a cloning
            return; //actually a scan, don't do anything
        }
        innerCost.setBase(innerCost.cloneMe());
        double estimatedMemoryMB = innerCost.getEstimatedHeapSize() / 1024d / 1024d;
        double estimatedRowCount = innerCost.getEstimatedRowCount();
        SConfiguration configuration = EngineDriver.driver().getConfiguration();
        long regionThreshold = configuration.getBroadcastRegionMbThreshold();
        long rowCountThreshold = configuration.getBroadcastRegionRowThreshold();
        AccessPath currentAccessPath = innerTable.getCurrentAccessPath();
        boolean isHinted = currentAccessPath.isHintedJoinStrategy();

        boolean costIt = isHinted ||
                estimatedMemoryMB < regionThreshold &&
                        estimatedRowCount < rowCountThreshold &&
                        (!currentAccessPath.isMissingHashKeyOK() ||
                                // allow full outer join without equality join condition
                                outerCost.getJoinType() == JoinNode.FULLOUTERJOIN ||
                                // allow left or inner join with non-equality broadcast join only if using spark
                                (innerTable instanceof FromBaseTable && optimizer.isForSpark()));

        if (costIt) {
            double joinSelectivity = SelectivityUtil.estimateJoinSelectivity(innerTable, cd, predList, (long) innerCost.rowCount(), (long) outerCost.rowCount(), outerCost, SelectivityUtil.JoinPredicateType.ALL);
            double totalOutputRows = SelectivityUtil.getTotalRows(joinSelectivity, outerCost.rowCount(), innerCost.rowCount());
            double joinSelectivityWithSearchConditionsOnly = SelectivityUtil.estimateJoinSelectivity(innerTable, cd, predList, (long) innerCost.rowCount(), (long) outerCost.rowCount(), outerCost, SelectivityUtil.JoinPredicateType.HASH_SEARCH);
            double totalJoinedRows = SelectivityUtil.getTotalRows(joinSelectivityWithSearchConditionsOnly, outerCost.rowCount(), innerCost.rowCount());
            int[] hashKeyColumns = HashableJoinStrategy.findHashKeyColumns(innerTable, cd, predList, optimizer.getAssignedTableMap());
            int innerHashKeyColCount = hashKeyColumns == null ? 0 : hashKeyColumns.length;
            double joinCost = broadcastJoinStrategyLocalCost(innerCost, outerCost, totalJoinedRows, innerHashKeyColCount);
            innerCost.setLocalCost(joinCost);
            innerCost.setLocalCostPerParallelTask(joinCost);
            innerCost.setParallelism(outerCost.getParallelism());
            double remoteCostPerPartition = SelectivityUtil.getTotalPerPartitionRemoteCost(innerCost, outerCost, optimizer);
            innerCost.setRemoteCost(remoteCostPerPartition);
            innerCost.setRemoteCostPerParallelTask(remoteCostPerPartition);
            innerCost.setRowOrdering(outerCost.getRowOrdering());
            innerCost.setEstimatedHeapSize((long) SelectivityUtil.getTotalHeapSize(innerCost, outerCost, totalOutputRows));
            innerCost.setRowCount(totalOutputRows);
        } else {
            // Set cost to max to rule out broadcast join
            innerCost.setLocalCost(Double.MAX_VALUE);
            innerCost.setRemoteCost(Double.MAX_VALUE);
            innerCost.setLocalCostPerParallelTask(Double.MAX_VALUE);
            innerCost.setRemoteCostPerParallelTask(Double.MAX_VALUE);
        }
    }

    /**
     * Broadcast Join Local Cost Computation
     *
     * @param innerCost the RHS cost
     * @param outerCost the LHS cost
     * @return the broadcast join cost
     */
    private static double broadcastJoinStrategyLocalCost(CostEstimate innerCost,
                                                         CostEstimate outerCost,
                                                         double numOfJoinedRows,
                                                         int innerHashKeyColCount) {
        SConfiguration config = EngineDriver.driver().getConfiguration();
        double localLatency = config.getFallbackLocalLatency(); // set to 1-microsecond normally, involves cost of opening iterator, calling next, ... etc

        double result = broadCastJoinLocalCostHelper(innerCost, outerCost, numOfJoinedRows, innerHashKeyColCount, localLatency);

        // For full outer join, we need to broadcast the left side also to compute the non-matching rows
        // from the right, so add cost to reflect that.
        if (outerCost.getJoinType() == JoinNode.FULLOUTERJOIN) {
            result += broadCastJoinLocalCostHelper(outerCost, innerCost, numOfJoinedRows, 0, localLatency);
        }
        return result;
    }

    private static double broadCastJoinLocalCostHelper(CostEstimate innerCost,
                                                       CostEstimate outerCost,
                                                       double numOfJoinedRows,
                                                       int innerHashKeyColCount,
                                                       double localLatency) {
        assert innerCost.getLocalCostPerParallelTask() != 0d || innerCost.localCost() == 0d;
        assert innerCost.getRemoteCostPerParallelTask() != 0d || innerCost.remoteCost() == 0d;


        //// estimate the size of the hash table
        double result = 0.0d;
        if(innerHashKeyColCount > 0) {                                                      // actual broadcast join
            double joiningRowCost = outerCost.rowCount() * LEFT_ONE_ROW_HASH_PROBE_COST;    // cost of probing each LHS row's key in the RHS hash table
            result =  innerCost.getLocalCost()                                              // cost of scanning RHS
                    + innerCost.rowCount() * RIGHT_ONE_ROW_HASH_TRANSMIT_COST               // cost of hashing the RHS (on OlapServer, in case of OLAP)
                    + outerCost.getLocalCostPerParallelTask()                               // cost of scanning the LHS (partitioned in OLAP, entirety in OLTP)
                    + joiningRowCost / outerCost.getParallelism();                          // cost of joining rows from LHS and RHS including the hash probing
        } else {                                                                            // nested loop join with RHS broadcast to executors (in case of OLAP)
            double joiningRowCost = numOfJoinedRows * localLatency;
            result =  innerCost.getLocalCost()                                              // cost of scanning RHS
                    + innerCost.remoteCost()                                                // cost of sending the hash table over the network
                    + outerCost.getLocalCostPerParallelTask()                               // cost of scanning the LHS (partitioned in OLAP, entirety in OLTP)
                    + joiningRowCost / outerCost.getParallelism();                          // cost of joining rows from LHS and RHS including the hash probing
        }
        return result;
    }
}
