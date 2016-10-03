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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.impl.sql.compile.*;

import java.util.Arrays;
import java.util.BitSet;

public class HalfMergeSortJoinStrategy extends HashableJoinStrategy {

    public HalfMergeSortJoinStrategy() {
    }

    @Override
    public boolean feasible(Optimizable innerTable,
                            OptimizablePredicateList predList,
                            Optimizer optimizer,
                            CostEstimate outerCost, boolean wasHinted) throws StandardException {
        if (!super.feasible(innerTable, predList, optimizer, outerCost, wasHinted))
            return false;

        /*
         * HalfMergeSortJoin is only feasible if the inner table is
         * sorted along the join columns.
         */
        ConglomerateDescriptor currentCd = innerTable.getCurrentAccessPath().getConglomerateDescriptor();
        if (currentCd == null) return false; //TODO -sf- this happens when over a non table scan, we should fix that

        // Take into account predicates from both inner and outer tables
        OptimizablePredicateList allPredicateList = new PredicateList();
        if (predList != null) {
            predList.copyPredicatesToOtherList(allPredicateList);
        }
        OptimizablePredicateList outerTablePredicateList = outerCost.getPredicateList();
        if (outerTablePredicateList != null) {
            outerTablePredicateList.copyPredicatesToOtherList(allPredicateList);
        }
        IndexRowGenerator innerRowGen = currentCd.getIndexDescriptor();
        return innerRowGen != null
                && innerRowGen.getIndexDescriptor() != null
                && mergeable(innerRowGen, allPredicateList, innerTable);
    }

    @Override
    public String getName() {
        return "HALFSORTMERGE";
    }

    @Override
    public String toString() {
        return "HalfMergeSortJoin";
    }

    /**
     * @see JoinStrategy#multiplyBaseCostByOuterRows
     */
    public boolean multiplyBaseCostByOuterRows() {
        return true;
    }

    /**
     * @see JoinStrategy#joinResultSetMethodName
     */
    public String joinResultSetMethodName() {
        return "getHalfMergeSortJoinResultSet";
    }

    /**
     * @see JoinStrategy#halfOuterJoinResultSetMethodName
     */
    public String halfOuterJoinResultSetMethodName() {
        return "getHalfMergeSortLeftOuterJoinResultSet";
    }

    /**
     * Right Side Cost + LeftSideRows*WriteCost
     */
    @Override
    public void estimateCost(Optimizable innerTable,
                             OptimizablePredicateList predList,
                             ConglomerateDescriptor cd,
                             CostEstimate outerCost,
                             Optimizer optimizer,
                             CostEstimate innerCost) throws StandardException {
        if (outerCost.isUninitialized() || (outerCost.localCost() == 0d && outerCost.getEstimatedRowCount() == 1.0d)) {
            RowOrdering ro = outerCost.getRowOrdering();
            if (ro != null)
                outerCost.setRowOrdering(ro); //force a cloning
            return; //actually a scan, don't change the cost
        }
        //set the base costing so that we don't lose the underlying table costs
        innerCost.setBase(innerCost.cloneMe());
        double joinSelectivity = SelectivityUtil.estimateJoinSelectivity(innerTable, cd, predList, (long) innerCost.rowCount(), (long) outerCost.rowCount(), outerCost);
        double totalOutputRows = SelectivityUtil.getTotalRows(joinSelectivity, outerCost.rowCount(), innerCost.rowCount());
        // Half sort merge join cost is 90% the cost of doing a merge sort join
        double joinCost = 0.9D * SelectivityUtil.mergeSortJoinStrategyLocalCost(innerCost, outerCost, 3);
        innerCost.setLocalCost(joinCost);
        innerCost.setLocalCostPerPartition(joinCost);
        innerCost.setRemoteCost(SelectivityUtil.getTotalRemoteCost(innerCost, outerCost, totalOutputRows));
        innerCost.setRowCount(totalOutputRows);
        innerCost.setEstimatedHeapSize((long) SelectivityUtil.getTotalHeapSize(innerCost, outerCost, totalOutputRows));
    }

    @Override
    public JoinStrategyType getJoinStrategyType() {
        return JoinStrategyType.HALF_MERGE_SORT;
    }

    // DB-3460: For an outer left join query, sort merge join was ruled out because it did not qualify memory
    // requirement for hash joins. Sort merge join requires substantially less memory than other hash joins, so
    // maxCapacity() is override to return a very large integer to bypass memory check.
    @Override
    public int maxCapacity(int userSpecifiedCapacity, int maxMemoryPerTable, double perRowUsage) {
        return Integer.MAX_VALUE;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private boolean mergeable(IndexRowGenerator innerRowGenerator,
                              OptimizablePredicateList predList,
                              Optimizable innerTable) throws StandardException {
        int[] keyColumnPositionMap = innerRowGenerator.baseColumnPositions();

        BitSet innerColumns = new BitSet(keyColumnPositionMap.length);
        for(int p=0;p<predList.size();p++) {
            Predicate pred = (Predicate) predList.getOptPredicate(p);
            RelationalOperator relop = pred.getRelop();
            if (pred.isJoinPredicate()) {
                assert relop instanceof BinaryRelationalOperatorNode :
                        "Programmer error: RelationalOperator of type " + relop.getClass() + " detected";
                BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode) relop;
                ColumnReference innerColumn = relop.getColumnOperand(innerTable);
                if (innerColumn == null) continue;
                int innerColumnNumber = innerColumn.getColumnNumber();
                for (int i = 0; i < keyColumnPositionMap.length; ++i) {
                    if (innerColumnNumber == keyColumnPositionMap[i]) {
                        innerColumns.set(i);
                    }
                }
            } else {
                if(!(relop instanceof BinaryRelationalOperatorNode)) continue;
                if(relop.getOperator()!=RelationalOperator.EQUALS_RELOP) continue;

                int innerEquals = pred.hasEqualOnColumnList(keyColumnPositionMap, innerTable);
                if (innerEquals >= 0) innerColumns.set(innerEquals);
            }
        }
        if(innerColumns.cardinality()<=0) return false; // we have no matching join predicates, so we can't work
        if(innerColumns.nextClearBit(0)<innerColumns.cardinality()) return false; // there's a gap, an unsorted column
        return true;
    }
}