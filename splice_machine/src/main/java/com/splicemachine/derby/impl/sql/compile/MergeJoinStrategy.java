/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.impl.sql.compile.*;
import java.util.Arrays;
import java.util.BitSet;

public class MergeJoinStrategy extends HashableJoinStrategy{

    public MergeJoinStrategy(){
    }

    @Override
    public String getName(){
        return "MERGE";
    }

    @Override
    public String toString(){
        return "MergeJoin";
    }

    @Override
    public String joinResultSetMethodName(){
        return "getMergeJoinResultSet";
    }

    @Override
    public boolean multiplyBaseCostByOuterRows(){
        return true;
    }

    @Override
    public String halfOuterJoinResultSetMethodName(){
        return "getMergeLeftOuterJoinResultSet";
    }

    @Override
    public boolean feasible(Optimizable innerTable,
                            OptimizablePredicateList predList,
                            Optimizer optimizer,
                            CostEstimate outerCost,
                            boolean wasHinted) throws StandardException{
        //we can't work if the outer table isn't sorted, regardless of what else happens
        if(outerCost==null) return false;
        RowOrdering outerRowOrdering=outerCost.getRowOrdering();
        if(outerRowOrdering==null) return false;

        /* Currently MergeJoin does not work with a right side IndexRowToBaseRowOperation */
        if(JoinStrategyUtil.isNonCoveringIndex(innerTable)){
            return false;
        }
        boolean hashFeasible=super.feasible(innerTable, predList, optimizer, outerCost, wasHinted);
        if(!hashFeasible) return false;

        /*
         * MergeJoin is only feasible if the inner and outer tables are both
         * sorted along the join columns *in the same order*.
         */
        ConglomerateDescriptor currentCd=innerTable.getCurrentAccessPath().getConglomerateDescriptor();
        if(currentCd==null) return false; //TODO -sf- this happens when over a non table scan, we should fix that

        // Take into account predicates from both inner and outer tables
        OptimizablePredicateList allPredicateList = new PredicateList();
        if (predList != null) {
            predList.copyPredicatesToOtherList(allPredicateList);
        }
        OptimizablePredicateList outerTablePredicateList = outerCost.getPredicateList();
        if (outerTablePredicateList != null) {
            outerTablePredicateList.copyPredicatesToOtherList(allPredicateList);
        }
        IndexRowGenerator innerRowGen=currentCd.getIndexDescriptor();
        return innerRowGen!=null
                && innerRowGen.getIndexDescriptor()!=null
                && mergeable(outerRowOrdering,innerRowGen,allPredicateList,innerTable);
    }


    /**
     * Right Side Cost + NetworkCost
     */
    @Override
    public void estimateCost(Optimizable innerTable,
                             OptimizablePredicateList predList,
                             ConglomerateDescriptor cd,
                             CostEstimate outerCost,
                             Optimizer optimizer,
                             CostEstimate innerCost) throws StandardException{
        if(outerCost.localCost()==0d && outerCost.getEstimatedRowCount()==1.0){
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
        //preserve the underlying CostEstimate for the inner table
        innerCost.setBase(innerCost.cloneMe());
        double joinSelectivity = SelectivityUtil.estimateJoinSelectivity(innerTable, cd, predList, (long) innerCost.rowCount(), (long) outerCost.rowCount(), outerCost);
        double scanSelectivity = SelectivityUtil.estimateScanSelectivity(innerTable, predList);
        double totalOutputRows = SelectivityUtil.getTotalRows(joinSelectivity*scanSelectivity, outerCost.rowCount(), innerCost.rowCount());
        innerCost.setNumPartitions(outerCost.partitionCount());
        boolean empty = isOuterTableEmpty(innerTable, predList);
        double joinCost = SelectivityUtil.mergeJoinStrategyLocalCost(innerCost, outerCost, empty);
        innerCost.setLocalCost(joinCost);
        innerCost.setLocalCostPerPartition(joinCost);innerCost.setRowOrdering(outerCost.getRowOrdering());
        innerCost.setRowCount(totalOutputRows);
        innerCost.setEstimatedHeapSize((long)SelectivityUtil.getTotalHeapSize(innerCost,outerCost,totalOutputRows));


    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private boolean isOuterTableEmpty(Optimizable innerTable, OptimizablePredicateList predList) throws StandardException{
        for (int i = 0; i < predList.size(); i++) {
            Predicate p = (Predicate) predList.getOptPredicate(i);
            if (!p.isJoinPredicate()) continue;
            BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode)p.getAndNode().getLeftOperand();
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
                if(outerTableCostController != null) {
                    long rc = (long)outerTableCostController.baseRowCount();
                    if (rc == 0)
                        return true;
                }
            }
        }

        return false;
    }
    private boolean mergeable(RowOrdering outerRowOrdering,
                                IndexRowGenerator innerRowGenerator,
                                OptimizablePredicateList predList,
                                Optimizable innerTable) throws StandardException{
        int[] keyColumnPositionMap = innerRowGenerator.baseColumnPositions();
        boolean[] keyAscending = innerRowGenerator.isAscending();

        BitSet innerColumns = new BitSet(keyColumnPositionMap.length);
        BitSet outerColumns = new BitSet(keyColumnPositionMap.length);
        for(int p = 0;p<predList.size();p++){
            Predicate pred = (Predicate)predList.getOptPredicate(p);
            if(pred.isJoinPredicate()) continue; //we'll deal with these later
            RelationalOperator relop=pred.getRelop();
            if(!(relop instanceof BinaryRelationalOperatorNode)) continue;
            if(relop.getOperator()==RelationalOperator.EQUALS_RELOP) {
                int innerEquals = pred.hasEqualOnColumnList(keyColumnPositionMap, innerTable);
                if (innerEquals >= 0) innerColumns.set(innerEquals);
                else {

                    BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode) relop;
                    ValueNode vn = bron.getLeftOperand();
                    if (!(vn instanceof ColumnReference))
                        vn = bron.getRightOperand();
                    if (!(vn instanceof ColumnReference)) continue;
                    ColumnReference outerColumn = (ColumnReference) vn;
                    /*
                     * We are still sortable if we have constant predicates on the first N keys on the outer
                     * side of the join, as long as we match the inner columns
                     */
                    int outerTableNum = outerColumn.getTableNumber();
                    int outerColNum = outerColumn.getColumnNumber();
                    //we don't care what the sort order for this column is, since it's an equals predicate anyway
                    int pos = outerRowOrdering.orderedPositionForColumn(RowOrdering.ASCENDING, outerTableNum, outerColNum);
                    if (pos >= 0) {
                        outerColumns.set(pos);
                    }
                    else {
                        pos = outerRowOrdering.orderedPositionForColumn(RowOrdering.DESCENDING, outerTableNum, outerColNum);
                        if (pos >= 0)
                            outerColumns.set(pos);
                    }
                }
            }
        }

        int[] innerToOuterJoinColumnMap = new int[keyColumnPositionMap.length];
        Arrays.fill(innerToOuterJoinColumnMap,-1);
        for(int i=0;i<keyColumnPositionMap.length;i++){
            /*
             * If we have equals predicates on the inner and outer columns already, then we don't
             * care about this position
             */
            int innerColumnPosition = keyColumnPositionMap[i];
            boolean ascending = keyAscending[i];

            for(int p=0;p<predList.size();p++){
                Predicate pred = (Predicate)predList.getOptPredicate(p);
                if(!pred.isJoinPredicate()) continue; //we've already dealt with those
                RelationalOperator relop=pred.getRelop();
                assert relop instanceof BinaryRelationalOperatorNode:
                        "Programmer error: RelationalOperator of type "+ relop.getClass()+" detected";
                BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode)relop;
                ColumnReference innerColumn=relop.getColumnOperand(innerTable);
                ColumnReference outerColumn=getOuterColumn(bron,innerColumn);
                if (innerColumn == null || outerColumn == null) continue;
                int innerColumnNumber = innerColumn.getColumnNumber();
                if(innerColumnNumber==innerColumnPosition){
                    innerColumns.set(i);
                    int outerTableNum=outerColumn.getTableNumber();
                    int outerColNum=outerColumn.getColumnNumber();
                    if(ascending){
                        int outerPos = outerRowOrdering.orderedPositionForColumn(RowOrdering.ASCENDING,outerTableNum,outerColNum);
                        if(outerPos>=0){
                            outerColumns.set(outerPos);
                            innerToOuterJoinColumnMap[i] = outerPos;
                        }
                        else
                            return false;
                    }else {
                        int outerPos = outerRowOrdering.orderedPositionForColumn(RowOrdering.DESCENDING,outerTableNum,outerColNum);
                        if(outerPos>=0){
                            outerColumns.set(outerPos);
                            innerToOuterJoinColumnMap[i] = outerPos;
                        }
                        else
                            return false;
                    }
                }
            }
        }
        if(innerColumns.cardinality()<=0) return false; //we have no matching join predicates, so we can't work
        //compute the and to look for the mismatch position
        outerColumns.and(innerColumns);
        int misMatchPos = outerColumns.nextClearBit(0);
        if(misMatchPos==0) return false; //we are missing the first key, so that won't work

        /*
         * We need to determine that the join predicates are on matched columns--i.e. that
         * innerToOuterJoinColumnMap[i+1] > innerToOuterJoinColumnMap[i] and all columns between
         * (innerToOuterJoinColumnMap[i], innerToOuterJoinColumnMap[i+1]) are in innerColumns. Same for outerColumns.
         */
        int lastOuterCol = -1;
        int lastInnerCol = -1;
        for(int i=0;i<innerToOuterJoinColumnMap.length;i++){
            int outerCol = innerToOuterJoinColumnMap[i];
            if(outerCol==-1) continue;
            if(outerCol<lastOuterCol) return false; //we have a join out of order
            for (int j = lastOuterCol+1; j < outerCol; ++j) {
                if (!outerColumns.get(j)) return false;
            }

            for (int j = lastInnerCol+1; j < i; ++j) {
                if (!innerColumns.get(j)) return false;
            }
            lastOuterCol = outerCol;
            lastInnerCol = i;
        }

        /*
         * Find the first inner join column, make sure all columns before it appear in innerColumns and outerColumns.
         * These columnsare referenced in equal predicates
         */
        if (innerToOuterJoinColumnMap.length > 0) {
            int first = 0;
            while (first < innerToOuterJoinColumnMap.length && innerToOuterJoinColumnMap[first] == -1) {
                first++;
            }
            // No inner join columns, merge join is not feasible
            if (first >= innerToOuterJoinColumnMap.length)
                return false;

            for (int i = 0; i < first; ++i) {
                if (!innerColumns.get(i)) {
                    return false;
                }
            }

            int outerCol = innerToOuterJoinColumnMap[first];
            for (int i = 0; i < outerCol; i++) {
                if (!outerColumns.get(i)) {
                    return false;
                }
            }
        }
        return true;
    }

    private ColumnReference getOuterColumn(BinaryRelationalOperatorNode bron,ColumnReference innerColumn){
        if (! (bron.getRightOperand() instanceof ColumnReference)) { // Not A Column, return null
            return null;
        }
        ColumnReference outerColumn = (ColumnReference)bron.getRightOperand();
        if(outerColumn==innerColumn) {
            if (! (bron.getLeftOperand() instanceof ColumnReference)) { // Not A Column, return null
                return null;
            }
            outerColumn = (ColumnReference) bron.getLeftOperand();
        }
        return outerColumn;
    }

    @Override
    public JoinStrategyType getJoinStrategyType() {
        return JoinStrategyType.MERGE;
    }

}