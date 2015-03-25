package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.impl.sql.compile.HashableJoinStrategy;
import com.splicemachine.db.impl.sql.compile.Predicate;

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
        if(outerCost.localCost()==0d && outerCost.getEstimatedRowCount()==1.0d)
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
        double outerSortCost = outerCost.localCost()+outerCost.remoteCost();
        double innerSortCost = innerCost.localCost()+innerCost.remoteCost();
        double sortCost = Math.max(outerSortCost,innerSortCost);

        double perRowLocalLatency = outerCost.localCost()/(2*outerCost.rowCount());
        perRowLocalLatency+=innerCost.localCost()/(2*innerCost.rowCount());

        double joinSelectivity = estimateJoinSelectivity(innerTable,cd,predList,outerCost,innerCost);

        int mergePartitions = 16;
        double mergeLocalCost = perRowLocalLatency*(outerCost.rowCount()+innerCost.rowCount());
        double avgOpenCost = (outerCost.getOpenCost()+innerCost.getOpenCost())/2;
        double avgCloseCost = (outerCost.getCloseCost()+innerCost.getCloseCost())/2;
        mergeLocalCost+=mergePartitions*(avgOpenCost+avgCloseCost);
        double mergeRows = joinSelectivity*(outerCost.rowCount()*innerCost.rowCount());
        double mergeRemoteCost = getTotalRemoteCost(outerCost,innerCost,mergeRows);
        double mergeHeapSize = getTotalHeapSize(outerCost,innerCost,mergeRows);

        double totalLocalCost = sortCost+mergeLocalCost;

        innerCost.setRemoteCost(mergeRemoteCost);
        innerCost.setLocalCost(totalLocalCost);
        innerCost.setEstimatedRowCount((long)mergeRows);
        innerCost.setEstimatedHeapSize((long)mergeHeapSize);
        innerCost.setNumPartitions(mergePartitions);
        /*
         * The TEMP algorithm re-sorts data according to join keys, which eliminates the sort order
         * of the data.
         */
        innerCost.setRowOrdering(null);
    }

}
