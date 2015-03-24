package com.splicemachine.derby.impl.sql.compile;

import com.google.common.base.Preconditions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.JoinStrategy;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicateList;
import com.splicemachine.db.iapi.sql.compile.Optimizer;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.impl.sql.compile.HashableJoinStrategy;
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
//		if (CostUtils.isThisBaseTable(optimizer))
//			return false;
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
                             CostEstimate innerCost) {
        /*
         * The SortMerge Algorithm (at least, in FUJI) is as follows:
         *
         * 1. read inner table data
         * 2. write inner table data into 16 TEMP regions
         * 3. read outer table data
         * 4. write outer table data into 16 TEMP regions, interleaved with inner table data
         * 5. read outer AND inner table data together, and generate the output rows
         *
         * Where steps 1-2 and 3-4 occur simultaneously. Thus, we estimate the cost as follows:
         *
         * Cost of steps 1-4: (0,max(outer.local+outer.remote,inner.local,inner.remote)) (e.g. remote cost only)
         * Cost of step 5: (outer.local+inner.local,outer.remote+inner.remote)*outerRows
         *
         * Ultimately, we will need to also consider the following items:
         *
         * 1. the cost of writing data to TEMP(for computing the max)
         * 2. the join selectivity of the predicates (for computing how many rows are output)
         * 3. the size of scanning that data.
         * 4. The cost of scanning data (local and remote) from TEMP
         *
         * getting #2 will require additional statistics which is not present in LASSEN, and getting #3 will
         * require some voodoo in fetching the base table and estimating relative output byte sizes and column
         * sizes etc. This version is a first cut approximation.
         *
         * Note also that at some point we will need to make a distinction in the optimizer about "local"
         * operations versus "remote" operations, and include different costs accordingly.
         */
        if(outerCost.getEstimatedRowCount()==1.0 && outerCost.getEstimatedCost()==0d) return;
        innerCost.setBase(innerCost.cloneMe());
        outerCost.setBase(outerCost.cloneMe());

        double innerSinkCost = innerCost.remoteCost()+outerCost.localCost();
        double outerSinkCost = outerCost.remoteCost()+outerCost.localCost();
        double sinkCost = Math.max(innerSinkCost,outerSinkCost);

        /*
         * Note that we have some knowledge about the scan cost because we know something about the
         * base data itself. That is, we know the estimated cost to read both of those data points off disk,
         * and since they are being merged together in TEMP, we know that we will have to touch
         * the total number of rows in both cases, so our read cost is essentially outerCost+innerCost.
         *
         * Unfortunately, we have a difficult time estimating whether we are performing the
         * read locally or remotely, and knowing the remote cost requires us to know something about the
         * selectivity of the join predicates (which we don't yet know). So, for v1 we have to settle
         * for additive costs.
         */
        double localScanCost = innerCost.localCost()+outerCost.localCost();
        double remoteScanCost = innerCost.remoteCost()+outerCost.remoteCost();
        double outputRows = outerCost.getEstimatedRowCount(); //TODO -sf- scale this according to the join predicates
        innerCost.setNumPartitions(16); //the size of the TEMP table by default
        innerCost.setLocalCost(localScanCost);
        innerCost.setRemoteCost(sinkCost+remoteScanCost);
        innerCost.setEstimatedRowCount((long)outputRows); //estimate 1 row per left side
        innerCost.setRowOrdering(null); //any inherent sort order is lost


//		SpliceLogUtils.trace(LOG, "rightResultSetCostEstimate outerCost=%s, innerFullKeyCost=%s",outerCost, innerCost);
//		// InnerCost does not change
//		SpliceCostEstimateImpl inner = (SpliceCostEstimateImpl) innerCost;
//		inner.setBase(innerCost.cloneMe());
//		SpliceCostEstimateImpl outer = (SpliceCostEstimateImpl) outerCost;
//        long rowCount = Math.max(inner.getEstimatedRowCount(), outer.getEstimatedRowCount());
//
//        long rows = CostUtils.add(inner.getEstimatedRowCount(), outer.getEstimatedRowCount());
//        int regions = inner.numberOfRegions + outer.numberOfRegions;
//        Preconditions.checkState(regions > 0);
//        double joinCost = ((rows * SpliceConstants.optimizerWriteCost) / regions);
//        // Prefer larger table on the left
//        double waggle = 0.0;
//        if (outer.singleScanRowCount<inner.singleScanRowCount)
//        	waggle = 0.5;
//		inner.setCost(joinCost+inner.cost+outer.cost+waggle, rowCount, rowCount);
//		inner.setNumberOfRegions(16);
//		inner.setRowOrdering(null);
//		SpliceLogUtils.trace(LOG, "rightResultSetCostEstimate computed cost innerCost=%s",innerCost);
	};
    
}
