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

public class V2NestedLoopJoinCostEstimation implements StrategyJoinCostEstimation {

    private static final double NLJ_ON_SPARK_PENALTY = 1e15;          // msirek-temp
    private static final double RHS_SEQUENTIAL_SCAN_PORTION_SMALL_RHS = 0.1;
    private static final double RHS_SEQUENTIAL_SCAN_PORTION_BIG_RHS   = 0.9;
    private static final double OLTP_JOINING_ONE_ROW_COST = 35;       // 35  microseconds
    private static final double OLAP_JOINING_ONE_ROW_COST = 152;      // 152 microseconds

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

        /* In v1, we calculate total row count as the following:
         * totalRowCount = outerCost.rowCount() * innerCost.rowCount().
         * CostEstimate.rowCount() returns at least 1.0. Join output row count is then over estimated when
         * selectivity on one side is very low, leading to a row count smaller than 1.0. In extreme cases
         * where one side produces 0 rows, using 1 instead would lead to 1 * n = n rows instead of
         * 0 * n = 0 rows. When n is big, this difference is not negligible.
         */
        double totalRowCount = Math.max(1.0d, outerCost.getRawRowCount() * innerCost.getRawRowCount());

        /* Compare the output row count estimated by NLJ to the one estimated by estimateJoinSelectivity().
         * Take the smaller estimate.
         */
        if (outerCost.getJoinSelectionCardinality() >= 0) {
            totalRowCount = Math.min(outerCost.getJoinSelectionCardinality(), totalRowCount);
        }
        boolean isIndexKeyAccessRhs = hasJoinPredicateWithIndexKeyLookup(predList);

        double nljOnSparkPenalty = getNljOnSparkPenalty(innerTable, innerCost, optimizer, isIndexKeyAccessRhs);
        double joinCost = nestedLoopJoinStrategyLocalCost(innerCost, outerCost, isIndexKeyAccessRhs, optimizer.isForSpark());
        joinCost += nljOnSparkPenalty;
        innerCost.setLocalCost(joinCost);
        innerCost.setLocalCostPerParallelTask(joinCost);
        innerCost.setSingleScanRowCount(innerCost.getEstimatedRowCount());
        double remoteCostPerPartition = SelectivityUtil.getTotalPerPartitionRemoteCost(innerCost, outerCost, optimizer);
        innerCost.setRemoteCost(remoteCostPerPartition);
        innerCost.setRemoteCostPerParallelTask(remoteCostPerPartition);
        innerCost.setRowOrdering(outerCost.getRowOrdering());
        innerCost.setEstimatedHeapSize((long) SelectivityUtil.getTotalHeapSize(innerCost, outerCost, totalRowCount));
        innerCost.setParallelism(outerCost.getParallelism());
        innerCost.setRowCount(totalRowCount);
        innerCost.setRawRowCount(totalRowCount);
        if (totalRowCount < outerCost.getJoinSelectionCardinality()) {
            outerCost.setJoinSelectionCardinality(totalRowCount);
        }
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
                                        CostEstimate innerCost,
                                        Optimizer optimizer,
                                        boolean isIndexKeyAccessRhs) {
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
            if (isIndexKeyAccessRhs)
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
     * Nested Loop Join Local Cost Computation
     *
     * @param innerCost RHS cost estimate
     * @param outerCost LHS cost estimate
     * @return local cost of NLJ on LHS and RHS
     */

    private static double nestedLoopJoinStrategyLocalCost(CostEstimate innerCost,
                                                          CostEstimate outerCost,
                                                          boolean isIndexKeyAccessRhs,
                                                          boolean isOlap) {
        /* NLJ cost explanation
         *
         * ### Thread-level parallelism
         * In executing nested loop joins, RHS table has to be scanned for each LHS row. At the beginning of
         * execution, we launch at maximum 25 asynchronous tasks to scan the RHS table for the first 25 LHS
         * rows. Then the main thread waits for the first task to return.
         * The asynchronous task returns an iterator on the RHS table pointing to the first matching row.
         * Once the first task has returned, main thread takes this RHS iterator and merges the RHS row with
         * the LHS row pointed by an LHS iterator. The main thread works sequentially. It calls next() of the
         * RHS iterator until all rows are exhausted, then moves to the RHS iterator returned from the next
         * asynchronous task.
         * Also, once a RHS iterator is exhausted, the main thread will launch a new asynchronous task for
         * the next LHS row.
         * The asynchronous task returns immediately once it finds a matching row. If there are more matching
         * rows to be joined later, it's done by the main thread because by then, the iterator is taken over
         * by the main thread.
         * In a nutshell, the tasks of scanning RHS table are done in parallel before meeting the first
         * matching row. The rest of the scan are done by the main thread sequentially. It is important to
         * model this in cost estimation to reflect actual execution.
         *
         * ### Index key access on RHS
         * There are three important assumptions we make when we have an index key access on RHS:
         * 1. Finding the first matching row on RHS is very fast because scan range is very limited
         * 2. In case of a PK access, there are no more matching rows after the first matching row on RHS.
         *    So the main thread doesn't scan RHS, effectively.
         * 3. In case of an index access, there may be more matching rows, but they are stored together. The
         *    main thread doesn't spend much time on finding the next matching row.
         *
         * In a word, we assume for index key access on RHS, main thread starts to output rows once the first
         * asynchronous task is done. The main thread doesn't wait on producing the next output row.
         *
         * ### Non-key access on RHS
         * In this case, we cannot assume main thread doesn't wait on producing the next output rows.
         * Actually, when RHS is big, most of the time would be spent on main thread scanning RHS for the
         * next matching row.
         * We need to assume, however, how much portion of the RHS scanning tasks can be done in parallel,
         * i.e., where the first matching row in RHS is. This cannot be generally answered.
         * We currently assume that the first matching row in RHS is in the middle of the table. So half of
         * the scan is done in parallel, half is done in the main thread in sequential. Assuming more to be
         * scanned in sequential would lead to better estimation for big tables but over estimates for
         * smaller tables. Assuming less to be scanned sequential would lead to better estimation for small
         * tables but under estimates for big tables.
         * Based on join microbenchmark result, 1000 rows on RHS table seems to be a good turning point.
         */
        double numOfJoiningRowsPerTask = Math.max(outerCost.rowCount() / outerCost.getParallelism(), 1) * innerCost.rowCount();
        double joiningRowCostPerTask = numOfJoiningRowsPerTask * (isOlap ? OLAP_JOINING_ONE_ROW_COST : OLTP_JOINING_ONE_ROW_COST);
        double rhsSeqScanPortion = innerCost.rowCount() <= 1000 ? RHS_SEQUENTIAL_SCAN_PORTION_SMALL_RHS : RHS_SEQUENTIAL_SCAN_PORTION_BIG_RHS;
        double localCost = 0.0;
        if (isIndexKeyAccessRhs) {
            localCost = innerCost.getLocalCost()                   // the first async task needs to finish first
                    + outerCost.getLocalCostPerParallelTask()      // main thread scans LHS
                    + joiningRowCostPerTask;                       // joining LHS rows and matching RHS rows
        } else {
            localCost = innerCost.getLocalCost() * (1 - rhsSeqScanPortion)            // the first async task returns after finding the first matching row on RHS
                    + outerCost.getLocalCostPerParallelTask()                         // main thread scans LHS
                    + joiningRowCostPerTask                                           // joining LHS rows and matching RHS rows
                    + Math.max(outerCost.rowCount() / outerCost.getParallelism(), 1) * innerCost.getLocalCost() * rhsSeqScanPortion;  // main thread scans RHS sequentially for the rest of matching rows
        }
        return localCost;
    }
}
