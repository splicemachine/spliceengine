package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.impl.sql.compile.FromBaseTable;
import com.splicemachine.db.impl.sql.compile.FromTable;
import com.splicemachine.db.impl.sql.compile.HashableJoinStrategy;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import org.apache.log4j.Logger;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.HBaseRegionLoads;
import com.splicemachine.utils.SpliceLogUtils;

public class BroadcastJoinStrategy extends HashableJoinStrategy {
    private static final Logger LOG = Logger.getLogger(BroadcastJoinStrategy.class);
    public BroadcastJoinStrategy() {
    }

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
     * @see com.splicemachine.db.iapi.sql.compile.JoinStrategy#estimateCost */
    @Override
    public void estimateCost(Optimizable innerTable,
                             OptimizablePredicateList predList,
                             ConglomerateDescriptor cd,
                             CostEstimate outerCost,
                             Optimizer optimizer,
                             CostEstimate costEstimate) {
    	SpliceLogUtils.trace(LOG, "estimateCost innerTable=%s,predList=%s,conglomerateDescriptor=%s,outerCost=%s,optimizer=%s,costEstimate=%s",innerTable,predList,cd,outerCost,optimizer,costEstimate);
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
                            Optimizer optimizer) throws StandardException {
//		if (CostUtils.isThisBaseTable(optimizer))
//			return false;
        boolean hashFeasible = super.feasible(innerTable, predList, optimizer) && innerTable.isBaseTable();
		TableDescriptor td = innerTable.getTableDescriptor();
        hashFeasible  = hashFeasible && (td!=null);
        if(!hashFeasible) return false;
        ConglomerateDescriptor[] cd = td.getConglomerateDescriptors();
        if(cd==null || cd.length<1) return false;

        /* Currently BroadcastJoin does not work with a right side IndexRowToBaseRowOperation */
        if(JoinStrategyUtil.isNonCoveringIndex(innerTable)) {
            return false;
        }

        CostEstimate baseEstimate = innerTable.estimateCost(predList,cd[0],optimizer.newCostEstimate(),optimizer,null);
        double estimatedMemory = baseEstimate.getEstimatedHeapSize()/1024d/1024d;
        return estimatedMemory<SpliceConstants.broadcastRegionMBThreshold;
	}

    @Override
    public void estimateCost(OptimizablePredicateList predList,CostEstimate outerCost,CostEstimate innerCost) {
        /*
         * The algorithm for Broadcast is as follows:
         *
         * 1. Read the entirety of the inner table into memory (locally)
         * 2. For each left hand row, probe memory for the join condition
         *
         * In this case, the overall cost is
         *
         * inner.local + inner.remote + outer.local+outer.remote
         */
        if(outerCost.localCost()==0d && outerCost.getEstimatedRowCount()==1.0d){
            return; //actually a scan, don't do anything
        }
        innerCost.setBase(innerCost.cloneMe());
        outerCost.setBase(outerCost.cloneMe());

        innerCost.setNumPartitions(outerCost.partitionCount());
        innerCost.setLocalCost(innerCost.localCost()+outerCost.localCost());
        innerCost.setRemoteCost(innerCost.remoteCost()+outerCost.remoteCost());
        innerCost.setRowOrdering(outerCost.getRowOrdering());
        innerCost.setEstimatedRowCount((long)outerCost.rowCount());
        innerCost.setEstimatedHeapSize(outerCost.getEstimatedHeapSize()+innerCost.getEstimatedHeapSize());
//        SpliceLogUtils.trace(LOG, "rightResultSetCostEstimate outerCost=%s, innerFullKeyCost=%s", outerCost, innerCost);
//
//        SpliceCostEstimateImpl inner = (SpliceCostEstimateImpl) innerCost;
//        SpliceCostEstimateImpl outer = (SpliceCostEstimateImpl) outerCost;
//
//        inner.setBase(innerCost.cloneMe());
//
//        double cost = inner.getEstimatedRowCount() * (SpliceConstants.remoteRead + SpliceConstants.optimizerHashCost) + inner.cost + outer.cost;
//        double rowCount = Math.max(innerCost.rowCount(),outerCost.rowCount());
//        inner.setCost(cost, rowCount, rowCount);
//        inner.setNumberOfRegions(outer.numberOfRegions);
//        inner.setRowOrdering(outer.rowOrdering);
//
//        SpliceLogUtils.trace(LOG, "rightResultSetCostEstimate computed cost innerCost=%s", innerCost);
    }
}

