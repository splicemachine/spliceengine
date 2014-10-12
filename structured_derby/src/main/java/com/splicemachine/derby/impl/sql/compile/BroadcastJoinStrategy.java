package com.splicemachine.derby.impl.sql.compile;

import java.util.Map;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.JoinStrategy;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.impl.sql.compile.HashableJoinStrategy;
import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
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
     * 
     * 
     * 
     * @see org.apache.derby.iapi.sql.compile.JoinStrategy#estimateCost */
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
	public boolean feasible(Optimizable innerTable,OptimizablePredicateList predList, Optimizer optimizer) throws StandardException {
		if (CostUtils.isThisBaseTable(optimizer)) 
			return false;
		SpliceLevel2OptimizerImpl opt = (SpliceLevel2OptimizerImpl) optimizer;
		boolean hashFeasible = super.feasible(innerTable, predList, optimizer);
		SpliceLogUtils.trace(LOG, "feasible innerTable=%s, predList=%s, optimizer=%s, hashFeasible=%s",innerTable,predList,optimizer,hashFeasible);
		TableDescriptor td;
		ConglomerateDescriptor[] cd;
		if (hashFeasible && innerTable != null && innerTable.isBaseTable() && (td = innerTable.getTableDescriptor())!= null && 
				(cd = td.getConglomerateDescriptors()) != null && cd.length >= 1) {
	        Map<String,RegionLoad> regionLoads = HBaseRegionLoads.getCachedRegionLoadsMapForTable(cd[0].getConglomerateNumber()+"");
			if (regionLoads == null)
				return false;
			int cost = 0;
	        for (RegionLoad regionLoad: regionLoads.values()) {
	        	cost += HBaseRegionLoads.memstoreAndStorefileSize(regionLoad);
	        }
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
	public void oneRowRightResultSetCostEstimate(OptimizablePredicateList predList, CostEstimate outerCost, CostEstimate innerFullKeyCost) {
		rightResultSetCostEstimate(predList,outerCost,innerFullKeyCost);
	};

	/**
	 * 
	 * Right Side Cost + Network Cost + RightSideRows*Hash Cost 
	 * 
	 */
	@Override
	public void rightResultSetCostEstimate(OptimizablePredicateList predList, CostEstimate outerCost, CostEstimate innerCost) {
		SpliceLogUtils.trace(LOG, "rightResultSetCostEstimate outerCost=%s, innerFullKeyCost=%s",outerCost, innerCost);
		// InnerCost does not change
		SpliceCostEstimateImpl inner = (SpliceCostEstimateImpl) innerCost;
		inner.setBaseCost((SpliceCostEstimateImpl) innerCost.cloneMe());
		SpliceCostEstimateImpl outer = (SpliceCostEstimateImpl) outerCost;
		double joinCost = inner.getEstimatedRowCount()*(SpliceConstants.remoteRead+SpliceConstants.optimizerHashCost);				
		inner.setCost(joinCost+inner.cost+outer.cost, outer.getEstimatedRowCount(), outer.getEstimatedRowCount());
		inner.setNumberOfRegions(outer.numberOfRegions);
		inner.setRowOrdering(outer.rowOrdering);
		SpliceLogUtils.trace(LOG, "rightResultSetCostEstimate computed cost innerCost=%s",innerCost);
	};	
}

