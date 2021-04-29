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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.impl.sql.compile.JoinNode;
import com.splicemachine.db.impl.sql.compile.SelectivityUtil;
import com.splicemachine.derby.impl.sql.compile.costing.StrategyJoinCostEstimation;

public class V2MergeSortJoinCostEstimation implements StrategyJoinCostEstimation {
    static final double OLTP_LEFT_ONE_ROW_HASH_PROBE_COST     = 0.6;   // 0.6 microseconds
    static final double OLTP_RIGHT_ONE_ROW_HASH_TRANSMIT_COST = 1.4;   // 1.4 microseconds

    private static final double OLAP_LOCAL_LATENCY       = 0.3;        // 0.3 microseconds
    private static final double OLAP_SHUFFLE_OVERHEAD = 160000;        // 160 milliseconds

    @Override
    public void estimateCost(Optimizable innerTable,
                             OptimizablePredicateList predList,
                             ConglomerateDescriptor cd,
                             CostEstimate outerCost,
                             Optimizer optimizer,
                             CostEstimate innerCost) throws StandardException {
        if(outerCost.isUninitialized() ||(outerCost.localCost()==0d && outerCost.getEstimatedRowCount()==1.0d)){
            RowOrdering ro=outerCost.getRowOrdering();
            if(ro!=null)
                outerCost.setRowOrdering(ro); //force a cloning
            return; //actually a scan, don't change the cost
        }
        //set the base costing so that we don't lose the underlying table costs
        innerCost.setBase(innerCost.cloneMe());
        double joinSelectivity = SelectivityUtil.estimateJoinSelectivity(innerTable, cd, predList, (long) innerCost.rowCount(), (long) outerCost.rowCount(), outerCost, SelectivityUtil.JoinPredicateType.ALL);
        double totalOutputRows = SelectivityUtil.getTotalRows(joinSelectivity, outerCost.rowCount(), innerCost.rowCount());
        double joinSelectivityWithSearchConditionsOnly = SelectivityUtil.estimateJoinSelectivity(innerTable, cd, predList, (long) innerCost.rowCount(), (long) outerCost.rowCount(), outerCost, SelectivityUtil.JoinPredicateType.HASH_SEARCH);
        double totalJoinedRows = SelectivityUtil.getTotalRows(joinSelectivityWithSearchConditionsOnly, outerCost.rowCount(), innerCost.rowCount());
        double joinCost = mergeSortJoinStrategyLocalCost(innerCost, outerCost, totalJoinedRows, optimizer.isForSpark());
        innerCost.setLocalCost(joinCost);
        innerCost.setLocalCostPerParallelTask(joinCost);
        double remoteCostPerPartition = SelectivityUtil.getTotalPerPartitionRemoteCost(innerCost,outerCost, optimizer);
        innerCost.setRemoteCost(remoteCostPerPartition);
        innerCost.setRemoteCostPerParallelTask(remoteCostPerPartition);
        innerCost.setParallelism(outerCost.getParallelism());
        innerCost.setEstimatedHeapSize((long)SelectivityUtil.getTotalHeapSize(innerCost,outerCost,totalOutputRows));
        innerCost.setRowCount(totalOutputRows);
        innerCost.setParallelism(outerCost.getParallelism());
        innerCost.setRowOrdering(null);
    }

    static long log(int x, int base)
    {
        return (long) (Math.log(x) / Math.log(base));
    }


    // We haven't modelled the details of Tungsten sort, so we can't accurately
    // cost it as an external sort algorithm.
    // For now, instead of using zero sort costs,
    // let's just use a naive sort cost formula which assumes
    // the sort is done in memory with a simple nlog(n) bounding.
    // TODO: Find the actual formula for estimating Tungsten sort costs.
    static double getSortCost(int rowsPerPartition, double costPerComparison) {
        double sortCost =
                (costPerComparison *
                        (rowsPerPartition * log(rowsPerPartition, 2)));
        return sortCost;
    }
    /**
     *
     * Merge Sort Join Local Cost Computation
     *
     * Total Cost = Max( (Left Side Cost+ReplicationFactor*Left Transfer Cost)/Left Number of Partitions),
     *              (Right Side Cost+ReplicationFactor*Right Transfer Cost)/Right Number of Partitions)
     *
     * Replication Factor Based
     *
     * @param innerCost
     * @param outerCost
     * @return
     */
    private static double mergeSortJoinStrategyLocalCost(CostEstimate innerCost,
                                                         CostEstimate outerCost,
                                                         double numOfJoinedRows,
                                                         boolean isOlap) {
        double localCost = 0.0;
        if (isOlap) {
            double joiningRowCost = numOfJoinedRows * OLAP_LOCAL_LATENCY;

            long outerTableNumTasks = outerCost.getParallelism();
            double innerRowCount = innerCost.rowCount() > 1? innerCost.rowCount():1;
            int innerRowCountPerPartition =
                    (innerRowCount / outerTableNumTasks) > Integer.MAX_VALUE ?
                            Integer.MAX_VALUE : (int)(innerRowCount / outerTableNumTasks);

            double innerSortCost =
                    getSortCost(innerRowCountPerPartition, OLAP_LOCAL_LATENCY);

            double outerRowCount = outerCost.rowCount() > 1? outerCost.rowCount():1;
            int outerRowCountPerPartition =
                    (outerRowCount / outerTableNumTasks) > Integer.MAX_VALUE ?
                            Integer.MAX_VALUE : (int)(outerRowCount / outerTableNumTasks);

            double outerSortCost =
                    getSortCost(outerRowCountPerPartition, OLAP_LOCAL_LATENCY);

            return outerCost.getLocalCostPerParallelTask() + innerCost.getLocalCostPerParallelTask()
                    + OLAP_SHUFFLE_OVERHEAD * 2
                    + outerSortCost + innerSortCost
                    + joiningRowCost / outerCost.getParallelism();
        } else {
            // On OLTP, there is no sort-merge join happening
            if (outerCost.getJoinType() <= JoinNode.INNERJOIN) {     // during cost estimation, getJoinType() could be 0, but it's still inner join
                /* For inner join, build a MultiMap for RHS table and for each <K, V> from LHS, probe it in
                 * the MultiMap. For each matching row, put it in a List<<K, <K, V>>>. This is hashJoin()
                 * implementation in OLTP.
                 */
                double joiningRowCost = outerCost.rowCount() * OLTP_LEFT_ONE_ROW_HASH_PROBE_COST;  // cost of probing each LHS row's key in the RHS hash table
                localCost = innerCost.getLocalCost()                                               // cost of scanning RHS
                        + innerCost.rowCount() * OLTP_RIGHT_ONE_ROW_HASH_TRANSMIT_COST             // cost of hashing the RHS (on OlapServer, in case of OLAP)
                        + outerCost.getLocalCostPerParallelTask()                                  // cost of scanning the LHS (partitioned in OLAP, entirety in OLTP)
                        + joiningRowCost / outerCost.getParallelism();                             // cost of joining rows from LHS and RHS including the hash probing
            } else {
                /* For other join types, build a MultiMap for LHS and also another MultiMap for RHS. Then
                 * iterate over the union of LHS key set and RHS key set. For each key that exists in both
                 * LHS and RHS, add a matching row in a List<<K, <K, V>>>. This is cogroup() implementation
                 * in OLTP.
                 */
                double joiningRowCost = numOfJoinedRows * OLTP_LEFT_ONE_ROW_HASH_PROBE_COST * 2;   // cost of probing each LHS row's key in the RHS hash table
                localCost = innerCost.getLocalCost()                                               // cost of scanning RHS
                        + innerCost.rowCount() * OLTP_RIGHT_ONE_ROW_HASH_TRANSMIT_COST             // cost of hashing the RHS (on OlapServer, in case of OLAP)
                        + outerCost.getLocalCostPerParallelTask()                                  // cost of scanning the LHS (partitioned in OLAP, entirety in OLTP)
                        + outerCost.rowCount() * OLTP_LEFT_ONE_ROW_HASH_PROBE_COST                 // cost of hashing the LHS
                        + joiningRowCost / outerCost.getParallelism();                             // cost of joining rows from LHS and RHS including the hash probing
            }
            return localCost;
        }
    }
}
