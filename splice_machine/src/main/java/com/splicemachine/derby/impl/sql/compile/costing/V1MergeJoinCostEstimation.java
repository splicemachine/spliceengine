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

package com.splicemachine.derby.impl.sql.compile.costing;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.impl.sql.compile.BinaryRelationalOperatorNode;
import com.splicemachine.db.impl.sql.compile.ColumnReference;
import com.splicemachine.db.impl.sql.compile.Predicate;
import com.splicemachine.db.impl.sql.compile.SelectivityUtil;

public class V1MergeJoinCostEstimation implements StrategyJoinCostEstimation {

    private boolean isOuterTableEmpty(Optimizable innerTable, OptimizablePredicateList predList) throws StandardException {
        for (int i = 0; i < predList.size(); i++) {
            Predicate p = (Predicate) predList.getOptPredicate(i);
            if (!p.isHashableJoinPredicate()) continue;
            BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode) p.getAndNode().getLeftOperand();
            ColumnReference outerColumn = null;
            if (bron.getLeftOperand() instanceof ColumnReference) {
                ColumnReference cr = (ColumnReference) bron.getLeftOperand();
                if (cr.getTableNumber() != innerTable.getTableNumber()) {
                    outerColumn = cr;
                }
            }
            if (bron.getRightOperand() instanceof ColumnReference) {
                ColumnReference cr = (ColumnReference) bron.getRightOperand();
                if (cr.getTableNumber() != innerTable.getTableNumber()) {
                    outerColumn = cr;
                }
            }
            if (outerColumn != null) {
                StoreCostController outerTableCostController = outerColumn.getStoreCostController();
                if (outerTableCostController != null) {
                    long rc = (long) outerTableCostController.baseRowCount();
                    if (rc == 0)
                        return true;
                }
            }
        }

        return false;
    }

    public void estimateCost(Optimizable innerTable,
                             OptimizablePredicateList predList,
                             ConglomerateDescriptor cd,
                             CostEstimate outerCost,
                             Optimizer optimizer,
                             CostEstimate innerCost) throws StandardException {
        if (outerCost.localCost() == 0d && outerCost.getEstimatedRowCount() == 1.0) {
            /*
             * Derby calls this method at the end of each table scan, even if it's not a join (or if it's
             * the left side of the join). When this happens, the outer cost is still unitialized, so there's
             * nothing to do in this method;
             */
            RowOrdering ro = outerCost.getRowOrdering();
            if (ro != null)
                outerCost.setRowOrdering(ro); //force a cloning
            return;
        }
        //preserve the underlying CostEstimate for the inner table
        CostEstimate baseInnerCost = innerCost.cloneMe();
        innerCost.setBase(baseInnerCost);
        double joinSelectivityWithSearchConditionsOnly = SelectivityUtil.estimateJoinSelectivity(innerTable, cd, predList, (long) innerCost.rowCount(), (long) outerCost.rowCount(), outerCost, SelectivityUtil.JoinPredicateType.MERGE_SEARCH);
        double joinSelectivity = SelectivityUtil.estimateJoinSelectivity(innerTable, cd, predList, (long) innerCost.rowCount(), (long) outerCost.rowCount(), outerCost, SelectivityUtil.JoinPredicateType.ALL);
        double scanSelectivity = SelectivityUtil.estimateScanSelectivity(innerTable, predList, SelectivityUtil.JoinPredicateType.MERGE_SEARCH);
        double totalOutputRows = SelectivityUtil.getTotalRows(joinSelectivity * scanSelectivity, outerCost.rowCount(), innerCost.rowCount());
        innerCost.setParallelism(outerCost.getParallelism());
        boolean empty = isOuterTableEmpty(innerTable, predList);
        /* totalJoinedRows is different from totalOutputRows
         * totalJoinedRows: the number of joined rows constructed just based on the merge join search conditions, that is, the equality join conditions on the leading index columns.
         * totalOutputRows: the number of final output rows, this is the result after applying any restrictive conditions, e.g., the inequality join conditions, conditions not on index columns.
         * totalJoinedRows is always equal or larger than totalOutputRows */
        double totalJoinedRows = SelectivityUtil.getTotalRows(joinSelectivityWithSearchConditionsOnly * scanSelectivity, outerCost.rowCount(), innerCost.rowCount());

        double innerTableScaleFactor = Math.min(1.0, joinSelectivity * outerCost.rowCount());
        innerTableScaleFactor = Math.min(innerTableScaleFactor, scanSelectivity);

        // Adjust the scanned rows so we can possibly avoid spark execution.
        double rightRowCount = innerTableScaleFactor * innerCost.rowCount();
        baseInnerCost.setScannedBaseTableRows(rightRowCount);
        //innerCost.setRowCount(rightRowCount);

        double joinCost = mergeJoinStrategyLocalCost(innerCost, outerCost, empty, totalJoinedRows, innerTableScaleFactor);
        innerCost.setLocalCost(joinCost);
        innerCost.setLocalCostPerParallelTask(joinCost);
        double remoteCostPerPartition =
                SelectivityUtil.getTotalPerPartitionRemoteCost(innerCost, outerCost, optimizer, innerTableScaleFactor);
        innerCost.setRemoteCost(remoteCostPerPartition);
        innerCost.setRemoteCostPerParallelTask(remoteCostPerPartition);
        innerCost.setRowOrdering(outerCost.getRowOrdering());
        innerCost.setEstimatedHeapSize((long) SelectivityUtil.getTotalHeapSize(innerCost, outerCost, totalOutputRows));
        innerCost.setRowCount(totalOutputRows);
    }

    /**
     * Merge Join Local Cost Computation
     * Total Cost = (Left Side Cost + Right Side Cost + Right Side Remote Cost)/Left Side Partition Count) + Open Cost + Close Cost
     */
    public static double mergeJoinStrategyLocalCost(CostEstimate innerCost, CostEstimate outerCost, boolean outerTableEmpty, double numOfJoinedRows, double innerTableScaleFactor) {
        SConfiguration config = EngineDriver.driver().getConfiguration();
        double localLatency = config.getFallbackLocalLatency();
        double joiningRowCost = numOfJoinedRows * localLatency;

        assert innerCost.getRemoteCostPerParallelTask() != 0d || innerCost.remoteCost() == 0d;
        double innerRemoteCost = innerCost.getRemoteCostPerParallelTask() * innerTableScaleFactor *
                innerCost.getParallelism();
        if (outerTableEmpty) {
            return (outerCost.getLocalCostPerParallelTask()) + innerCost.getOpenCost() + innerCost.getCloseCost();
        } else
            return outerCost.getLocalCostPerParallelTask() + innerCost.getLocalCostPerParallelTask() * innerTableScaleFactor +
                    innerRemoteCost / outerCost.getParallelism() +
                    innerCost.getOpenCost() + innerCost.getCloseCost()
                    + joiningRowCost / outerCost.getParallelism();
    }

}
