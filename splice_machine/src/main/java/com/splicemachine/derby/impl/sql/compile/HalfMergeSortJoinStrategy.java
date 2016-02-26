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
     * @see JoinStrategy#resultSetMethodName
     */
    public String resultSetMethodName(boolean bulkFetch, boolean multiprobe) {
        if (bulkFetch)
            return "getBulkTableScanResultSet";
        else if (multiprobe)
            return "getMultiProbeTableScanResultSet";
        else
            return "getTableScanResultSet";
    }

    /**
     * @see JoinStrategy#joinResultSetMethodName
     */
    public String joinResultSetMethodName() {
        return "getMergeSortJoinResultSet";
    }

    /**
     * @see JoinStrategy#halfOuterJoinResultSetMethodName
     */
    public String halfOuterJoinResultSetMethodName() {
        return "getMergeSortLeftOuterJoinResultSet";
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
        innerCost.setNumPartitions(outerCost.partitionCount());
        innerCost.setLocalCost(SelectivityUtil.halfMergeSortJoinStrategyLocalCost(innerCost, outerCost, 3));
        innerCost.setRemoteCost(SelectivityUtil.getTotalRemoteCost(innerCost, outerCost, totalOutputRows));
        innerCost.setRowOrdering(outerCost.getRowOrdering());
        innerCost.setRowCount(totalOutputRows);
        innerCost.setEstimatedHeapSize((long) SelectivityUtil.getTotalHeapSize(innerCost, outerCost, totalOutputRows));
        innerCost.setRowOrdering(null);
        innerCost.setNumPartitions(16);
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
        for(int p = 0;p<predList.size();p++){
            Predicate pred = (Predicate)predList.getOptPredicate(p);
            if(pred.isJoinPredicate()) continue; //we'll deal with these later
            RelationalOperator relop=pred.getRelop();
            if(!(relop instanceof BinaryRelationalOperatorNode)) continue;
            if(relop.getOperator()==RelationalOperator.EQUALS_RELOP) {
                int innerEquals = pred.hasEqualOnColumnList(keyColumnPositionMap, innerTable);
                if (innerEquals >= 0) innerColumns.set(innerEquals);
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

            for(int p=0;p<predList.size();p++){
                Predicate pred = (Predicate)predList.getOptPredicate(p);
                if(!pred.isJoinPredicate()) continue; //we've already dealt with those
                RelationalOperator relop=pred.getRelop();
                assert relop instanceof BinaryRelationalOperatorNode:
                        "Programmer error: RelationalOperator of type "+ relop.getClass()+" detected";
                BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode)relop;
                ColumnReference innerColumn=relop.getColumnOperand(innerTable);
                if (innerColumn == null ) continue;
                int innerColumnNumber = innerColumn.getColumnNumber();
                if(innerColumnNumber==innerColumnPosition){
                    innerColumns.set(i);
                    innerToOuterJoinColumnMap[i] = i;
                }
            }
        }
        if(innerColumns.cardinality()<=0) return false; //we have no matching join predicates, so we can't work

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
        }
        return true;
    }
}