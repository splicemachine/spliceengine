package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.impl.sql.compile.*;

import java.util.Arrays;
import java.util.BitSet;

public class MergeJoinStrategy extends BaseCostedHashableJoinStrategy{

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
    public String resultSetMethodName(boolean bulkFetch,boolean multiprobe){
        if(bulkFetch)
            return "getBulkTableScanResultSet";
        else if(multiprobe)
            return "getMultiProbeTableScanResultSet";
        else
            return "getTableScanResultSet";
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
        boolean hashFeasible=super.feasible(innerTable,predList,optimizer,outerCost,wasHinted);
        if(!hashFeasible) return false;

        /*
         * MergeJoin is only feasible if the inner and outer tables are both
         * sorted along the join columns *in the same order*.
         */
        ConglomerateDescriptor currentCd=innerTable.getCurrentAccessPath().getConglomerateDescriptor();
        if(currentCd==null) return false; //TODO -sf- this happens when over a non table scan, we should fix that

        IndexRowGenerator innerRowGen=currentCd.getIndexDescriptor();
        return innerRowGen!=null
                && innerRowGen.getIndexDescriptor()!=null
                && mergeable(outerRowOrdering,innerRowGen,predList,innerTable);
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

        /*
         * The Merge Join algorithm is quite simple--for each outer row, we read rows until
         * either A) we reach a row which matches the join predicates, or B) we reach a row
         * which is past the join predicates. In all cases, we read the outer table fully,
         * and we read the inner row equally fully (although we terminate early once we exceed the
         * other table scan's stop key, and we start the inner table scan at the correct point
         * for the first outer row). As a result, we know that we touch each inner row exactly once,
         * and we touch each outer row exactly once. Therefore, we have an additive cost as follows:
         *
         * totalLocalCost = outer.localCost+inner.localCost+inner.remoteCost
         * totalRemoteCost = outer.remoteCost + inner.remoteCost
         *
         * Which includes network traffic to the outer table's region and network traffic from
         * that region to the control node
         */
        double outerRowCount=outerCost.rowCount();
        double innerRowCount=innerCost.rowCount();
        if(outerRowCount==0){
            /*
             * There is no way that merge will do anything, so we can just stop here
             */
            return;
        }
        if(innerRowCount==0){
            /*
             * We don't modify the scan any, but it's possible that the inner side
             * has non-negligible costs in order to generate 0 rows, so we can't disregard it.
             * Instead, we just assume that innerRowCount==1
             */
            innerRowCount = 1d;
        }
        double joinSelectivity =JoinSelectivity.estimateJoinSelectivity(innerTable, cd, predList,(long) innerRowCount,(long) outerRowCount,
                JoinStrategyType.BROADCAST,outerCost);

        double outerRemoteCost=outerCost.remoteCost();

        double rowCount = joinSelectivity*outerRowCount*innerRowCount;

        double innerRemoteCost=innerCost.remoteCost();
        double totalLocalCost = outerCost.localCost()+innerCost.localCost()+innerRemoteCost;
        totalLocalCost+=innerCost.partitionCount()*(innerCost.getOpenCost()+innerCost.getCloseCost());

        /*
         * The costing for broadcast and merge joins are essentially identical, so
         * it's possible (particularly in the event of high-selectivity joins)
         * that merge and broadcast joins will cost identicaly. In these situations, we want to
         * favor merge join, since it can start late and stop early, and thus shave off a few
         * microseconds of latency. We do this by downshifting the remote cost by a very small
         * factor (just 5). That way, the costing strategies are slightly different, and we
         * can favor merge join when all other things are equal
         */
        double totalRemoteCost = getTotalRemoteCost(outerRemoteCost,
                innerRemoteCost,
                outerRowCount,
                innerRowCount,
                rowCount)-5;
        double heapSize = getTotalHeapSize(outerCost.getEstimatedHeapSize(),
                innerCost.getEstimatedHeapSize(),
                innerRowCount,
                outerRowCount,
                rowCount);
        int numPartitions = outerCost.partitionCount()*innerCost.partitionCount();

        /*
         * MergeJoin is sorted according to the outer table first, then the inner table
         */
        RowOrdering outerRowOrdering = outerCost.getRowOrdering();
        RowOrdering innerRowOrdering = innerCost.getRowOrdering();
        RowOrdering outputOrder = outerRowOrdering.getClone();
        innerRowOrdering.copy(outputOrder);

        innerCost.setRowOrdering(outputOrder);
        innerCost.setLocalCost(totalLocalCost);
        innerCost.setRemoteCost(totalRemoteCost);
        innerCost.setRowCount(rowCount);
        innerCost.setEstimatedHeapSize((long)heapSize);
        innerCost.setNumPartitions(numPartitions);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
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
            if(relop.getOperator()==RelationalOperator.EQUALS_RELOP){
                int innerEquals = pred.hasEqualOnColumnList(keyColumnPositionMap,innerTable);
                if(innerEquals>=0) innerColumns.set(innerEquals);
                else{

                    BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode)relop;
                    ValueNode vn = bron.getLeftOperand();
                    if(!(vn instanceof ColumnReference))
                        vn = bron.getRightOperand();
                    if(!(vn instanceof ColumnReference)) continue;
                    ColumnReference outerColumn = (ColumnReference)vn;
                    /*
                     * We are still sortable if we have constant predicates on the first N keys on the outer
                     * side of the join, as long as we match the inner columns
                     */
                    int outerTableNum=outerColumn.getTableNumber();
                    int outerColNum=outerColumn.getColumnNumber();
                    //we don't care what the sort order for this column is, since it's an equals predicate anyway
                    int pos = outerRowOrdering.orderedPositionForColumn(RowOrdering.ASCENDING,outerTableNum,outerColNum);
                    if(pos>=0)
                        outerColumns.set(pos);
                    else{
                        pos = outerRowOrdering.orderedPositionForColumn(RowOrdering.DESCENDING,outerTableNum,outerColNum);
                        if(pos>=0)
                            outerColumns.set(pos);
                    }
                }
            }else if(relop.getOperator()==RelationalOperator.GREATER_EQUALS_RELOP){
                //we only care if this is on the outside, since the inside it won't work correctly
                int innerEquals = pred.hasEqualOnColumnList(keyColumnPositionMap,innerTable);
                if(innerEquals>=0) continue;
                assert relop instanceof BinaryRelationalOperatorNode:
                        "Programmer error: RelationalOperator of type "+ relop.getClass()+" detected";

                BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode)relop;
                ValueNode vn = bron.getLeftOperand();
                if(!(vn instanceof ColumnReference))
                    vn = bron.getRightOperand();
                if(!(vn instanceof ColumnReference)) continue;
                ColumnReference outerColumn = (ColumnReference)vn;
                    /*
                     * We are still sortable if we have constant predicates on the first N keys on the outer
                     * side of the join, as long as we match the inner columns
                     */
                int outerTableNum=outerColumn.getTableNumber();
                int outerColNum=outerColumn.getColumnNumber();
                //we don't care what the sort order for this column is, since it's an equals predicate anyway
                int pos = outerRowOrdering.orderedPositionForColumn(RowOrdering.ASCENDING,outerTableNum,outerColNum);
                if(pos==0)
                    outerColumns.set(pos);
                else{
                    pos = outerRowOrdering.orderedPositionForColumn(RowOrdering.DESCENDING,outerTableNum,outerColNum);
                    if(pos==0)
                        outerColumns.set(pos);
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
                    }else {
                        int outerPos = outerRowOrdering.orderedPositionForColumn(RowOrdering.DESCENDING,outerTableNum,outerColNum);
                        if(outerPos>=0){
                            outerColumns.set(outerPos);
                            innerToOuterJoinColumnMap[i] = outerPos;
                        }
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
         * We need to determine that the join predicates are on matched columns--i.e. that innercolumn[i+1] > innerColumn[i]
         * for all set inner join columns
         */
        int lastOuterCol = -1;
        for(int i=0;i<innerToOuterJoinColumnMap.length;i++){
            int outerCol = innerToOuterJoinColumnMap[i];
            if(outerCol==-1) continue;
            if(outerCol<lastOuterCol) return false; //we have a join out of order
            lastOuterCol = outerCol;
        }
        return true;
    }

    private ColumnReference getOuterColumn(BinaryRelationalOperatorNode bron,ColumnReference innerColumn){
        ColumnReference outerColumn = (ColumnReference)bron.getRightOperand();
        if(outerColumn==innerColumn)
            outerColumn = (ColumnReference)bron.getLeftOperand();
        return outerColumn;
    }

    @Override
    public JoinStrategyType getJoinStrategyType() {
        return JoinStrategyType.MERGE;
    }

}