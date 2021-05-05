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

package com.splicemachine.derby.impl.sql.compile.costing.v1;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.compile.costing.SelectivityEstimator;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.SessionProperties;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.impl.sql.compile.FromBaseTable;
import com.splicemachine.db.impl.sql.compile.Predicate;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import com.splicemachine.db.impl.sql.compile.SelectivityUtil;
import com.splicemachine.derby.impl.sql.compile.costing.StrategyJoinCostEstimation;

import static com.splicemachine.db.impl.sql.compile.JoinNode.INNERJOIN;

public class V1NestedLoopJoinCostEstimation implements StrategyJoinCostEstimation {

    private static final double NLJ_ON_SPARK_PENALTY = 1e15;  // msirek-temp

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

        //set the base costs for the join
        innerCost.setBase(innerCost.cloneMe());
        double totalRowCount = outerCost.rowCount()*innerCost.rowCount();

        double nljOnSparkPenalty = getNljOnSparkPenalty(innerTable, predList, innerCost, outerCost, optimizer);
        innerCost.setRowOrdering(outerCost.getRowOrdering());
        innerCost.setEstimatedHeapSize((long) SelectivityUtil.getTotalHeapSize(innerCost, outerCost, totalRowCount));
        innerCost.setParallelism(outerCost.getParallelism());
        innerCost.setRowCount(totalRowCount);
        double joinCost = nestedLoopJoinStrategyLocalCost(innerCost, outerCost, totalRowCount, optimizer.isForSpark());
        joinCost += nljOnSparkPenalty;
        innerCost.setLocalCost(joinCost);
        innerCost.setLocalCostPerParallelTask(joinCost);
        innerCost.setSingleScanRowCount(innerCost.getEstimatedRowCount());
        double remoteCostPerPartition = SelectivityUtil.getTotalPerPartitionRemoteCost(innerCost, outerCost, optimizer);
        innerCost.setRemoteCost(remoteCostPerPartition);
        innerCost.setRemoteCostPerParallelTask(remoteCostPerPartition);
    }

    // Nested loop join is most useful if it can be used to
    // derive an index point-lookup predicate, otherwise it can be
    // very slow on Spark.
    // NOTE: The following description of the behavior is
    //       enabled if session property olapAlwaysPenalizeNLJ
    //       is false, or not set:
    // Detect when no join predicates are present that have
    // both a start key and a stop key.  If none are present,
    // return a large cost penalty so we'll avoid such joins.
    private double getNljOnSparkPenalty(Optimizable table,
                                        OptimizablePredicateList predList,
                                        CostEstimate innerCost,
                                        CostEstimate outerCost,
                                        Optimizer optimizer) {
        double retval = 0.0d;
        if (!optimizer.isForSpark() || optimizer.isMemPlatform())
            return retval;
        if (table.getCurrentAccessPath().isHintedJoinStrategy())
            return retval;
        if (isSingleTableScan(optimizer))
            return retval;
        double multiplier = innerCost.getFromBaseTableRows();
        if (multiplier < 1d)
            multiplier = 1d;
        QueryTreeNode queryTreeNode = (QueryTreeNode) table;
        LanguageConnectionContext lcc = queryTreeNode.getLanguageConnectionContext();
        Boolean olapAlwaysPenalizeNLJ = (Boolean)
                lcc.getSessionProperties().getProperty(SessionProperties.PROPERTYNAME.OLAPALWAYSPENALIZENLJ);

        if (olapAlwaysPenalizeNLJ == null || !olapAlwaysPenalizeNLJ.booleanValue()) {
            if (!isBaseTable(table))
                return NLJ_ON_SPARK_PENALTY * multiplier;
            if (hasJoinPredicateWithIndexKeyLookup(predList))
                return retval;
        }
        return NLJ_ON_SPARK_PENALTY * multiplier;
    }

    private boolean isSingleTableScan(Optimizer optimizer) {
        return optimizer.getJoinPosition() == 0   &&
                optimizer.getJoinType() < INNERJOIN;
    }

    private boolean isBaseTable(Optimizable table) {
        return table instanceof FromBaseTable;
    }

    private boolean hasJoinPredicateWithIndexKeyLookup(OptimizablePredicateList predList) {
        if (predList != null) {
            for (int i = 0; i < predList.size(); i++) {
                Predicate p = (Predicate) predList.getOptPredicate(i);
                if (p.getReferencedSet().cardinality() > 1 &&
                        p.isStartKey() && p.isStopKey())
                    return true;
            }
        }
        return false;
    }

    /**
     *
     * Nested Loop Join Local Cost Computation
     *
     * Total Cost = (Left Side Cost)/Left Side Partition Count) + (Left Side Row Count/Left Side Partition Count)*(Right Side Cost + Right Side Transfer Cost)
     *
     * @param innerCost
     * @param outerCost
     * @return
     */

    public static double nestedLoopJoinStrategyLocalCost(CostEstimate innerCost, CostEstimate outerCost,
                                                         double numOfJoinedRows, boolean useSparkCostFormula) {
        SConfiguration config = EngineDriver.driver().getConfiguration();
        double localLatency = config.getFallbackLocalLatency();
        double joiningRowCost = numOfJoinedRows * localLatency;

        // Using nested loop join on spark is bad in general because we may incur thousands
        // or millions of RPC calls to HBase, depending on the number of rows accessed
        // in the outer table, which may saturate the network.

        // If we divide inner table probe costs by outerCost.getParallelism(), as the number
        // of partitions goes up, the cost of the join, according to the cost formula,
        // goes down, making nested loop join appear cheap on spark.
        // But is it really that cheap?
        // We have multiple spark tasks simultaneously sending RPC requests
        // in parallel (not just between tasks, but also in multiple threads within a task).
        // Saying that as partition count goes up, the costs go down implies that we have
        // infinite network bandwidth, which is not the case.
        // We therefore adopt a cost model which assumes all RPC requests go through the
        // same network pipeline, and remove the division of the inner table row lookup cost by the
        // number of partitions.

        // This change only applies to the spark path (for now) to avoid any possible
        // performance regression in OLTP query plans.
        // Perhaps this can be made the new formula for both spark and control
        // after more testing to validate it.

        // A possible better join strategy for OLAP queries, which still makes use of
        // the primary key or index on the inner table, could be to sort the outer
        // table on the join key and then perform a merge join with the inner table.

        /* DB-11662 note
         * The explanation above makes sense, but it seems that not taking innerLocalCost per
         * parallel task but the whole inner cost is too unfair for NLJ compared to other join
         * strategies, especially when the inner table scan cost is very high.
         */
        double innerRemoteCost = innerCost.getRemoteCostPerParallelTask()*innerCost.getParallelism();
        if (useSparkCostFormula)
            return outerCost.getLocalCostPerParallelTask() +
                    (Math.max(outerCost.rowCount() / outerCost.getParallelism(), 1)
                            * innerCost.getLocalCostPerParallelTask()) +
                    (outerCost.rowCount() * (innerRemoteCost + outerCost.getRemoteCost())) +  // this is not correct, but to avoid regression on OLAP
                    joiningRowCost / outerCost.getParallelism();
        else
            return outerCost.getLocalCostPerParallelTask() +
                    Math.max(outerCost.rowCount() / outerCost.getParallelism(), 1)
                            * (innerCost.getLocalCostPerParallelTask()+innerCost.getRemoteCost()) +
                    joiningRowCost / outerCost.getParallelism();
    }
}
