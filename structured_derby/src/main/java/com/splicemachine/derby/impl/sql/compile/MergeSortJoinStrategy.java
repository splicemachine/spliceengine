package com.splicemachine.derby.impl.sql.compile;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.JoinStrategy;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.impl.sql.compile.HashableJoinStrategy;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.SpliceLogUtils;

public class MergeSortJoinStrategy extends HashableJoinStrategy {
    private static final Logger LOG = Logger.getLogger(MergeSortJoinStrategy.class);

    public MergeSortJoinStrategy() {
    }

    @Override
	public boolean feasible(Optimizable innerTable,
			OptimizablePredicateList predList, Optimizer optimizer)
			throws StandardException {
		if (CostUtils.isThisBaseTable(optimizer)) 
			return false;
		return super.feasible(innerTable, predList, optimizer);
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
    
    /** @see org.apache.derby.iapi.sql.compile.JoinStrategy#estimateCost */
    @Override
    public void estimateCost(Optimizable innerTable,
                             OptimizablePredicateList predList,
                             ConglomerateDescriptor cd,
                             CostEstimate outerCost,
                             Optimizer optimizer,
                             CostEstimate costEstimate) {
    	SpliceLogUtils.trace(LOG, "estimateCost innerTable=%s,predList=%s,conglomerateDescriptor=%s,outerCost=%s,optimizer=%s,costEstimate=%s",innerTable,predList,cd,outerCost,optimizer,costEstimate);
    }

	@Override
	public void oneRowRightResultSetCostEstimate(OptimizablePredicateList predList, CostEstimate outerCost, CostEstimate innerFullKeyCost) {
		rightResultSetCostEstimate(predList,outerCost,innerFullKeyCost);
	};

	/**
	 * 
	 * Right Side Cost + (LeftSideRows+RightSideRows)*WriteCost 
	 * 
	 */
	@Override
	public void rightResultSetCostEstimate(OptimizablePredicateList predList, CostEstimate outerCost, CostEstimate innerCost) {		
		SpliceLogUtils.trace(LOG, "rightResultSetCostEstimate outerCost=%s, innerFullKeyCost=%s",outerCost, innerCost);
		// InnerCost does not change
		SpliceCostEstimateImpl inner = (SpliceCostEstimateImpl) innerCost;
		inner.setBaseCost((SpliceCostEstimateImpl) innerCost.cloneMe());
		SpliceCostEstimateImpl outer = (SpliceCostEstimateImpl) outerCost;
		double joinCost = ((inner.getEstimatedRowCount() + outer.getEstimatedRowCount()) * SpliceConstants.optimizerWriteCost)/(inner.numberOfRegions + outer.numberOfRegions) ;				
		inner.setCost(joinCost+inner.cost+outer.cost, outer.getEstimatedRowCount(), outer.getEstimatedRowCount());
		inner.setNumberOfRegions(16);
		inner.setRowOrdering(null);
		SpliceLogUtils.trace(LOG, "rightResultSetCostEstimate computed cost innerCost=%s",innerCost);
	};	
    
}
