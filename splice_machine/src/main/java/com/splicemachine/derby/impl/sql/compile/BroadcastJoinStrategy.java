package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.JoinStrategy;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicateList;
import com.splicemachine.db.iapi.sql.compile.Optimizer;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.impl.sql.compile.HashableJoinStrategy;
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
        boolean hashFeasible = super.feasible(innerTable, predList, optimizer);
		SpliceLogUtils.trace(LOG, "feasible innerTable=%s, predList=%s, optimizer=%s, hashFeasible=%s",innerTable,predList,optimizer,hashFeasible);
		TableDescriptor td;
		ConglomerateDescriptor[] cd;

        /* Currently BroadcastJoin does not work with a right side IndexRowToBaseRowOperation */
        if(JoinStrategyUtil.isNonCoveringIndex(innerTable)) {
            return false;
        }

		if (hashFeasible
                && innerTable.isBaseTable()
                && (td=innerTable.getTableDescriptor())!=null
                && (cd=td.getConglomerateDescriptors())!=null && cd.length>=1) {
	        long cost = HBaseRegionLoads.memstoreAndStoreFileSize(Long.toString(cd[0].getConglomerateNumber()));
	        if (cost<0)
	        	return false;
	        SpliceLogUtils.trace(LOG, "feasible cost=%d",cost);
	        if (cost < SpliceConstants.broadcastRegionMBThreshold) {
		        SpliceLogUtils.trace(LOG, "broadcast join is feasible");
		        return true;
	        }
		}
        SpliceLogUtils.trace(LOG, "broadcast join is not feasible");
		return false;
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
        innerCost.setNumPartitions(outerCost.partitionCount());
        innerCost.setLocalCost(innerCost.localCost()+outerCost.localCost());
        innerCost.setRemoteCost(innerCost.remoteCost()+outerCost.remoteCost());
        innerCost.setRowOrdering(outerCost.getRowOrdering());
        innerCost.setEstimatedRowCount((long)outerCost.rowCount());
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

