package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;

public class MergeSortJoinStrategy extends BaseCostedHashableJoinStrategy {

    public MergeSortJoinStrategy() {
    }

    @Override
	public boolean feasible(Optimizable innerTable,
                            OptimizablePredicateList predList,
                            Optimizer optimizer,
                            CostEstimate outerCost) throws StandardException {
//		if (CostUtils.isThisBaseTable(optimizer))
//			return false;
		return super.feasible(innerTable, predList, optimizer,outerCost);
	}

	/**
     * @see JoinStrategy#getName
     */
    public String getName() {
        return "SORTMERGE";
    }

	/** @see JoinStrategy#multiplyBaseCostByOuterRows */
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
	 * 
	 * Right Side Cost + (LeftSideRows+RightSideRows)*WriteCost 
	 * 
	 */
	@Override
    public void estimateCost(Optimizable innerTable,
                             OptimizablePredicateList predList,
                             ConglomerateDescriptor cd,
                             CostEstimate outerCost,
                             Optimizer optimizer,
                             CostEstimate innerCost) throws StandardException{
        if(outerCost.isUninitialized() ||(outerCost.localCost()==0d && outerCost.getEstimatedRowCount()==1.0d))
            return; //actually a scan, don't change the cost

        //set the base costing so that we don't lose the underlying table costs
        innerCost.setBase(innerCost.cloneMe());
        outerCost.setBase(outerCost.cloneMe());
        /*
         * MergeSortJoins are complex, and likely to change. We break this up
         * into two different versions: a "TempTable" version, and a "Spark version".
         * Currently, the "Spark version" is unimplemented, but when this merges
         * in we'll need to re-cost that
         */
        tempTableCost(innerTable,predList,cd,outerCost,optimizer,innerCost);
	}

    private void tempTableCost(Optimizable innerTable,
                               OptimizablePredicateList predList,
                               ConglomerateDescriptor cd,
                               CostEstimate outerCost,
                               Optimizer optimizer,
                               CostEstimate innerCost) throws StandardException{
        /*
         * Using the TEMP table algorithm, MergeSort Joins have a complicated
         * algorithmic structure(and therefore, a complicated cost structure).
         *
         * There are two phases to the merge sort join: the "sort" and the "merge"
         * phases. The Sort phase *must* occur completely before the Merge phase
         * can begin (in the TEMP-based algorithm). Because of this, we
         * cost the two phases independently, and add them together at the end
         *
         * ------
         * Sort Phase costing:
         *
         * The sort phase is a parallel writing algorithm which concurrently reads
         * the inner and outer tables and writes their output into 16 TEMP blocks.
         * The cost to read and move the outer table is
         *
         * outerTableSortCost = outerTable.localCost+outerTable.remoteCost
         *
         * (Note that we assume the cost to write data is the same as the cost
         * to read data, which is probably not entirely true--still, it's convenient
         * for now)
         *
         * And the cost to read and move the inner table is
         *
         * innerTableSortCost = innerTable.localCost + innerTable.remoteCost
         *
         * Since both the inner and outer tables are being read and moved concurrently,
         * we note that
         *
         * sortLocalCost    = max(outerTableSortCost,innerTableSortCost)
         * sortRemoteCost   = sortLocalCost
         * sortOutputRows   = outerTable.rowCount +innerTable.rowCount
         * sortHeapSize     = outerTable.heapSize + innerTable.heapSize
         * sortPartitions   = outerTable.partitions+innerTable.partitions
         *
         * -------
         * Merge Phase costing:
         *
         * The merge phase consists of reading all data stored in
         * the sorted partitions, and generating merged rows. Fundamentally,
         * we don't know the latency to read data from TEMP(this is something for the
         * future),so we approximate it by
         *
         * L = outerTable.localCost/2*outerTable.rowCount
         *          +innerTable.localCost/2*innerTable.rowCount
         *
         * Which allows us to compute the merge costing as
         *
         * mergeLocalCost = L*(outerTable.numRows+innerTable.numRows)
         * mergeRemoteCost = joinSelectivity*(outerTable.numRows*innerTable.numRows)
         * mergeHeapSize = joinSelectivity*(outerTable.heapSize*innerTable.heapSize)
         * mergePartitions = 16(or whatever the size of TEMP is currently)
         */
        double innerRowCount=innerCost.rowCount();
        double outerRowCount=outerCost.rowCount();
        if(innerRowCount==0d){
            if(outerRowCount==0d) return; //we don't expect to change the cost any when we run this, because we expect to be empty
            else{
                /*
                 * For the purposes of estimation, we will assume that the innerRowCount = 1, so that
                 * we can safely compute the estimate. The difference between 1 and 0 is generally negliable,
                 * and it will allow us to get a sensical cost estimate
                 */
                innerRowCount = 1d;
            }
        }else if(outerRowCount==0d){
            /*
             * For the purposes of safe estimation, we assume that we are returning at least one row. The
             * cost difference is relatively negligiable, but this way we avoid NaNs and so forth.
             */
            outerRowCount = 1d;
        }

        double outerRemoteCost=outerCost.remoteCost();
        double innerRemoteCost=innerCost.remoteCost();

        double outerSortCost = (outerCost.localCost()+outerRemoteCost)/outerCost.partitionCount();
        double innerSortCost = (innerCost.localCost()+innerRemoteCost)/innerCost.partitionCount();
        double sortCost = Math.max(outerSortCost,innerSortCost);

        double perRowLocalLatency = outerCost.localCost()/(2*outerRowCount);
        perRowLocalLatency+=innerCost.localCost()/(2*innerRowCount);

        double joinSelectivity = estimateJoinSelectivity(innerTable,cd,predList,innerRowCount);

        double mergeRows = joinSelectivity*(outerRowCount*innerRowCount);
        int mergePartitions = (int)Math.round(Math.min(16,mergeRows));
        double mergeLocalCost = perRowLocalLatency*(outerRowCount+innerRowCount);
        double avgOpenCost = (outerCost.getOpenCost()+innerCost.getOpenCost())/2;
        double avgCloseCost = (outerCost.getCloseCost()+innerCost.getCloseCost())/2;
        mergeLocalCost+=mergePartitions*(avgOpenCost+avgCloseCost);
        double mergeRemoteCost = getTotalRemoteCost(outerRemoteCost,innerRemoteCost,outerRowCount,innerRowCount,mergeRows);
        double mergeHeapSize = getTotalHeapSize(outerCost.getEstimatedHeapSize(),
                innerCost.getEstimatedHeapSize(),
                outerRowCount,
                innerRowCount,
                mergeRows);

        double totalLocalCost = sortCost+mergeLocalCost;

        innerCost.setRemoteCost(mergeRemoteCost);
        innerCost.setLocalCost(totalLocalCost);
        innerCost.setRowCount(mergeRows);
        innerCost.setEstimatedHeapSize((long)mergeHeapSize);
        innerCost.setNumPartitions(mergePartitions);
        /*
         * The TEMP algorithm re-sorts data according to join keys, which eliminates the sort order
         * of the data.
         */
        innerCost.setRowOrdering(null);
    }

}
