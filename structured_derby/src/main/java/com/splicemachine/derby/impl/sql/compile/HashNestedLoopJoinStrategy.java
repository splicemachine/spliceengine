package com.splicemachine.derby.impl.sql.compile;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.JoinStrategy;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.impl.sql.compile.FromBaseTable;
import org.apache.derby.impl.sql.compile.HashableJoinStrategy;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * @author Scott Fines
 *         Date: 7/24/14
 */
public class HashNestedLoopJoinStrategy extends HashableJoinStrategy {
    private static final Logger LOG = Logger.getLogger(HashNestedLoopJoinStrategy.class);
    @Override
    public String getName() {
        return "HASH";
    }

    @Override
    public String resultSetMethodName(boolean bulkFetch, boolean multiprobe) {
        if (bulkFetch)
            return "getBulkTableScanResultSet";
        else if (multiprobe)
            return "getMultiProbeTableScanResultSet";
        else
            return "getTableScanResultSet";
    }

    @Override
    public String joinResultSetMethodName() {
        return "getHashJoinResultSet";
    }

    @Override
    public String halfOuterJoinResultSetMethodName() {
        return "getHashLeftOuterJoinResultSet";
    }
    
 

    /** @see org.apache.derby.iapi.sql.compile.JoinStrategy#nonBasePredicateSelectivity */
    public double nonBasePredicateSelectivity(Optimizable innerTable,OptimizablePredicateList predList) throws StandardException {
        double retval = 1.0;
        if (predList != null) {
            for (int i = 0; i < predList.size(); i++) {
                // Don't include redundant join predicates in selectivity calculations
                if (predList.isRedundantPredicate(i)) {
                    continue;
                }
                retval *= predList.getOptPredicate(i).selectivity(innerTable);
            }
        }
        return retval;
    }

    /**
     * @see org.apache.derby.iapi.sql.compile.JoinStrategy#putBasePredicates
     *
     * @exception StandardException		Thrown on error
     */
    public void putBasePredicates(OptimizablePredicateList predList,OptimizablePredicateList basePredicates) throws StandardException {
        for (int i = basePredicates.size() - 1; i >= 0; i--) {
            OptimizablePredicate pred = basePredicates.getOptPredicate(i);
            predList.addOptPredicate(pred);
            basePredicates.removeOptPredicate(i);
        }
    }
    
    @Override
    public boolean isHashJoin() {
        return false;
    }

    @Override
    public boolean feasible(Optimizable innerTable, OptimizablePredicateList predList, Optimizer optimizer) throws StandardException {
    	SpliceLogUtils.trace(LOG, "feasible innerTable=%s, predList=%s, optimizer=%s",innerTable,predList,optimizer);
        /*
         * Somewhat Temporary Fix.
         *
         * The HashNestedLoopJoinStrategy doesn't make a whole lot of sense unless it is used over
         * a raw table scan or index lookup (e.g. doing it over a sink operation isn't very useful). Additionally,
         * in that case the raw NestedLoopJoin or MergeSortJoin are both more preferable operations than this anyway.
         * Thus, we just make this plan infeasible if there is a sink node under this tree. In addition to
         * this, we also don't want to use MultiProbe scans under this instance--better to use Raw NLJ or MergeSort
         * to make those work.
         */
    	// Could Check for Sortability here?
    	boolean hashableFeasible = super.feasible(innerTable,predList,optimizer);
    	boolean isOneRowResultSet = false;
    	if (innerTable instanceof FromBaseTable)
    		isOneRowResultSet = ((FromBaseTable) innerTable).isOneRowResultSet(predList);
    	SpliceLogUtils.trace(LOG, "feasible? hashableFeasible=%s, isOneRowResultSet=%s",hashableFeasible,isOneRowResultSet);    	
    	return hashableFeasible && isOneRowResultSet;        
    }
    

    
	@Override
	public void oneRowRightResultSetCostEstimate(OptimizablePredicateList predList, CostEstimate outerCost, CostEstimate innerFullKeyCost) {
		rightResultSetCostEstimate(predList, outerCost,innerFullKeyCost);
	};

	/**
	 * 
	 * Right Side Cost + Network Cost + RightSideRows*Hash Cost 
	 * 
	 */
	@Override
	public void rightResultSetCostEstimate(OptimizablePredicateList predList, CostEstimate outerCost, CostEstimate innerCost) {
		SpliceLogUtils.trace(LOG, "rightResultSetCostEstimate outerCost=%s, innerFullKeyCost=%s",outerCost, innerCost);
		double cost;
//		if (outerCost.getEstimatedRowCount() > 10000)
//			cost = Double.MAX_VALUE;
//		else 
	    if (outerCost.getEstimatedRowCount() > SpliceConstants.hashNLJRightHashTableSize)
			cost = (SpliceConstants.optimizerNetworkCost/(double)SpliceConstants.hashNLJRightHashTableSize+innerCost.getEstimatedCost()+SpliceConstants.optimizerHashCost)* outerCost.getEstimatedRowCount();
		else
			cost = SpliceConstants.optimizerNetworkCost + (innerCost.getEstimatedCost()+SpliceConstants.optimizerHashCost)* outerCost.getEstimatedRowCount();
		innerCost.setCost(cost, innerCost.rowCount() * outerCost.rowCount(), innerCost.singleScanRowCount());
		SpliceLogUtils.trace(LOG, "rightResultSetCostEstimate computed cost innerCost=%s",innerCost);
	};	
	public boolean singleRowOnly() {
		return true;
	};

}
