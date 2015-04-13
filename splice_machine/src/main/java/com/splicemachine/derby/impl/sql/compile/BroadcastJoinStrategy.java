package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;

public class BroadcastJoinStrategy extends BaseCostedHashableJoinStrategy {
    public BroadcastJoinStrategy() { }

    /**
     * @see JoinStrategy#getName
     */
    public String getName() {
        return "BROADCAST";
    }

    /**
     * @see JoinStrategy#resultSetMethodName
     */
	@Override
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
	@Override
    public String joinResultSetMethodName() {
        return "getBroadcastJoinResultSet";
    }

    /**
     * @see JoinStrategy#halfOuterJoinResultSetMethodName
     */
	@Override
    public String halfOuterJoinResultSetMethodName() {
        return "getBroadcastLeftOuterJoinResultSet";
    }
	
	/** @see JoinStrategy#multiplyBaseCostByOuterRows */
	public boolean multiplyBaseCostByOuterRows() {
		return true;
	}
    
    /**
     * 
     * Checks to see if the innerTable is hashable.  If so, it then checks to make sure the
     * data size of the conglomerate (Table or Index) is less than SpliceConstants.broadcastRegionMBThreshold
     * using the HBaseRegionLoads.memstoreAndStorefileSize method on each region load.
     * 
     */
	@Override
	public boolean feasible(Optimizable innerTable,
                            OptimizablePredicateList predList,
                            Optimizer optimizer,
                            CostEstimate outerCost) throws StandardException {
        boolean hashFeasible = super.feasible(innerTable,predList,optimizer,outerCost) && innerTable.isBaseTable();
        if(!hashFeasible) return false;
		TableDescriptor td = innerTable.getTableDescriptor();
        if(td==null) return false;
        ConglomerateDescriptor[] cd = td.getConglomerateDescriptors();
        if(cd==null || cd.length<1) return false;

        /* Currently BroadcastJoin does not work with a right side IndexRowToBaseRowOperation */
        if(JoinStrategyUtil.isNonCoveringIndex(innerTable)) {
            return false;
        }

        CostEstimate baseEstimate = innerTable.estimateCost(predList,cd[0],optimizer.newCostEstimate(),optimizer,null);
        double estimatedMemoryMB = baseEstimate.getEstimatedHeapSize()/1024d/1024d;
        return estimatedMemoryMB<SpliceConstants.broadcastRegionMBThreshold;
	}

    @Override
    public void estimateCost(Optimizable innerTable,
                             OptimizablePredicateList predList,
                             ConglomerateDescriptor cd,
                             CostEstimate outerCost,
                             Optimizer optimizer,
                             CostEstimate innerCost) throws StandardException{
        /*
         * Broadcast Joins are relatively straightforward. Before scanning a single outer row,
         * it first reads all inner table rows into a local hashtable; then, as each outer row
         * is read, the inner hashtable is probed for any matching rows.
         *
         * The big effect here is that the cost to read the inner table is constant, regardless of
         * the join predicates (because the join predicate are applied after the inner table is read).
         *
         * totalCost.localCost = outerCost.localCost + innerCost.localCost+innerCost.remoteCost
         *
         * But the output metrics are different, based on the join predicates. Thus, we compute
         * the output joinSelectivity of all join predicates, and adjust the cost as
         *
         * totalCost.remoteCost = joinSelectivity*(outerCost.remoteCost+innerCost.remoteCost)
         * totalCost.outputRows = joinSelectivity*outerCost.outputRows
         * totalCost.heapSize = joinSelectivity*(outerCost.heapSize + innerCost.heapSize)
         *
         * Note that we count the innerCost.remoteCost twice. This accounts for the fact that we
         * have to read the inner table's data twice--once to build the hashtable, and once
         * to account for the final scan of data to the control node.
         */
        if(outerCost.isUninitialized() ||(outerCost.localCost()==0d && outerCost.getEstimatedRowCount()==1.0d)){
            return; //actually a scan, don't do anything
        }
        innerCost.setBase(innerCost.cloneMe());
        double outerRowCount=outerCost.rowCount();
        double innerRowCount=innerCost.rowCount();
        if(innerRowCount==0d){
            //we don't do anything to the base scan costs, but we DO affect the join
            //selectivity, so we treat the inner row count as 1
            innerRowCount = 1d;
        }
        if(outerRowCount==0d){
            outerRowCount = 1d;
        }

        double outerRemoteCost=outerCost.remoteCost();
        double innerRemoteCost=innerCost.remoteCost();

        double joinSelectivity = estimateJoinSelectivity(innerTable,cd,predList,innerRowCount);
        double totalLocalCost = outerCost.localCost()+innerCost.localCost()+innerRemoteCost;
        //add in the overhead to open the inner table scan
        totalLocalCost+= innerCost.partitionCount()*(innerCost.getOpenCost()+innerCost.getCloseCost());


        double totalOutputRows = joinSelectivity*(outerRowCount*innerRowCount);

        double totalRemoteCost=getTotalRemoteCost(outerRemoteCost,innerRemoteCost,outerRowCount,innerRowCount,totalOutputRows);

        //each partition of the outer table will see inner table's partitionCount
        int totalPartitionCount = outerCost.partitionCount()*innerCost.partitionCount();

        double totalHeapSize=getTotalHeapSize(outerCost.getEstimatedHeapSize(),
                innerCost.getEstimatedHeapSize(),
                outerRowCount,
                innerRowCount,
                totalOutputRows);

        innerCost.setNumPartitions(totalPartitionCount);
        innerCost.setLocalCost(totalLocalCost);
        innerCost.setRemoteCost(totalRemoteCost);
        innerCost.setRowOrdering(outerCost.getRowOrdering());
        innerCost.setRowCount(totalOutputRows);
        innerCost.setEstimatedHeapSize((long)totalHeapSize);
        innerCost.setSingleScanRowCount(joinSelectivity*outerCost.singleScanRowCount());
    }
}

