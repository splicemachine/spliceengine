package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.impl.sql.compile.*;

public class MergeJoinStrategy extends BaseCostedHashableJoinStrategy{

    public MergeJoinStrategy(){
    }

    /**
     * @see JoinStrategy#getName
     */
    public String getName(){
        return "MERGE";
    }

    /**
     * @see JoinStrategy#resultSetMethodName
     */
    @Override
    public String resultSetMethodName(boolean bulkFetch,boolean multiprobe){
        if(bulkFetch)
            return "getBulkTableScanResultSet";
        else if(multiprobe)
            return "getMultiProbeTableScanResultSet";
        else
            return "getTableScanResultSet";
    }

    /**
     * @see JoinStrategy#joinResultSetMethodName
     */
    @Override
    public String joinResultSetMethodName(){
        return "getMergeJoinResultSet";
    }

    /**
     * @see JoinStrategy#multiplyBaseCostByOuterRows
     */
    public boolean multiplyBaseCostByOuterRows(){
        return true;
    }

    /**
     * @see JoinStrategy#halfOuterJoinResultSetMethodName
     */
    @Override
    public String halfOuterJoinResultSetMethodName(){
        return "getMergeLeftOuterJoinResultSet";
    }

    @Override
    public boolean feasible(Optimizable innerTable,
                            OptimizablePredicateList predList,
                            Optimizer optimizer,
                            CostEstimate outerCost) throws StandardException{
        //we can't work if the outer table isn't sorted, regardless of what else happens
        if(outerCost==null) return false;
        RowOrdering outerRowOrdering=outerCost.getRowOrdering();
        if(outerRowOrdering==null) return false;

        /* Currently MergeJoin does not work with a right side IndexRowToBaseRowOperation */
        if(JoinStrategyUtil.isNonCoveringIndex(innerTable)){
            return false;
        }
        boolean hashFeasible=super.feasible(innerTable,predList,optimizer,outerCost);
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
                && isMergeable(outerRowOrdering,innerRowGen,predList,innerTable);
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
        double joinSelectivity = estimateJoinSelectivity(innerTable,cd,predList,outerCost,innerCost);

        double rowCount = joinSelectivity*outerCost.rowCount()*innerCost.rowCount();

        double totalLocalCost = outerCost.localCost()+innerCost.localCost()+innerCost.remoteCost();
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
        double totalRemoteCost = getTotalRemoteCost(outerCost,innerCost,rowCount)-5;
        double heapSize = getTotalHeapSize(outerCost,innerCost,rowCount);
        int numPartitions = outerCost.partitionCount()*innerCost.partitionCount();

        innerCost.setRowOrdering(outerCost.getRowOrdering()); //merge join inherits the sort order from the outer table
        innerCost.setLocalCost(totalLocalCost);
        innerCost.setRemoteCost(totalRemoteCost);
        innerCost.setRowCount(rowCount);
        innerCost.setEstimatedHeapSize((long)heapSize);
        innerCost.setNumPartitions(numPartitions);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private boolean isMergeable(RowOrdering outerRowOrdering,
                                IndexRowGenerator innerRowGenerator,
                                OptimizablePredicateList predList,
                                Optimizable innerTable) throws StandardException{
        /*
         * We need to ensure that the sort order of both the inner and outer tables
         * contains all join predicates *AND* that the join predicates on the inner table
         * are sorted in contiguous order (e.g. that we can build start and stop keys off
         * of the predicates)
         */
        //map from index key position -> column number (e.g. key column 0 is column key[0], etc.)
        int[] keyColumnPositionMap = innerRowGenerator.baseColumnPositions();
        boolean[] keyAscending = innerRowGenerator.isAscending();

        boolean foundZerothKey = false;
        for(int i=0;i<predList.size();i++){
            Predicate p = (Predicate)predList.getOptPredicate(i);
            if(!p.isJoinPredicate()) continue; //ignore non-join predicates
            RelationalOperator relop=p.getRelop();
            assert relop instanceof BinaryRelationalOperatorNode:
                    "Programmer error: RelationalOperator of type "+ relop.getClass()+" detected";

            BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode)relop;
            ColumnReference innerColumn=relop.getColumnOperand(innerTable);
            int innerColumnNumber = innerColumn.getColumnNumber();
            boolean found = false;
            int keyPos = 0;
            boolean ascending = false;
            for(int keyColumn:keyColumnPositionMap){
                if(innerColumnNumber==keyColumn){
                    found = true;
                    ascending = keyAscending[keyPos];
                    break;
                }
                keyPos++;
            }
            if(!found) return false; //we are not sorted according to this join predicate
            foundZerothKey = foundZerothKey || keyPos==0;

            ColumnReference outerColumn = (ColumnReference)bron.getRightOperand();
            if(outerColumn==innerColumn)
                outerColumn = (ColumnReference)bron.getLeftOperand();

            //TODO -sf- is this correct?
            int outerTableNum=outerColumn.getTableNumber()+1;
            int outerColNum=outerColumn.getColumnNumber();
            if(ascending){
                if(!outerRowOrdering.orderedOnColumn(RowOrdering.ASCENDING,outerTableNum,outerColNum))
                    return false;
            }else{
                if(!outerRowOrdering.orderedOnColumn(RowOrdering.DESCENDING,outerTableNum,outerColNum))
                    return false;

            }
        }
        return foundZerothKey;
    }

}