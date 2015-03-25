package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.JoinStrategy;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicate;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicateList;
import com.splicemachine.db.iapi.sql.compile.Optimizer;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.impl.sql.compile.FromBaseTable;
import com.splicemachine.db.impl.sql.compile.HashableJoinStrategy;
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
    
 

    /** @see com.splicemachine.db.iapi.sql.compile.JoinStrategy#nonBasePredicateSelectivity */
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
     * @see com.splicemachine.db.iapi.sql.compile.JoinStrategy#putBasePredicates
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
    public boolean feasible(Optimizable innerTable,
                            OptimizablePredicateList predList,
                            Optimizer optimizer,
                            CostEstimate outerCost) throws StandardException {
//		if (CostUtils.isThisBaseTable(optimizer))
//			return false;
    	SpliceLogUtils.trace(LOG, "feasible innerTable=%s, predList=%s, optimizer=%s",innerTable,predList,optimizer);
    	return false;  // Temprorary until we can reason about this join algorithm
    	
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
        /*
    	boolean hashableFeasible = super.feasible(innerTable,predList,optimizer);
    	boolean isOneRowResultSet = false;
    	if (innerTable instanceof FromBaseTable)
    		isOneRowResultSet = ((FromBaseTable) innerTable).isOneRowResultSet(predList);
    	SpliceLogUtils.trace(LOG, "feasible? hashableFeasible=%s, isOneRowResultSet=%s",hashableFeasible,isOneRowResultSet);    	
    	return hashableFeasible && isOneRowResultSet;   
    	*/
    }


    ;

	/**
	 * 
	 * Right Side Cost + Network Cost + RightSideRows*Hash Cost 
	 * 
	 */
	@Override
	public void estimateCost(Optimizable innerTable,
                             OptimizablePredicateList predList,
                             ConglomerateDescriptor cd,
                             CostEstimate outerCost,
                             Optimizer optimizer,
                             CostEstimate innerCost) {
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
	}

	@Override
	public void divideUpPredicateLists(Optimizable innerTable,
			OptimizablePredicateList originalRestrictionList,
			OptimizablePredicateList storeRestrictionList,
			OptimizablePredicateList nonStoreRestrictionList,
			OptimizablePredicateList requalificationRestrictionList,
			DataDictionary dd) throws StandardException {
				originalRestrictionList.setPredicatesAndProperties(storeRestrictionList);
	};

}
