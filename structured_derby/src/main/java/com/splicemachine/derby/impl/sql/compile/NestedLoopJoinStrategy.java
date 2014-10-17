package com.splicemachine.derby.impl.sql.compile;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.ExpressionClassBuilderInterface;
import org.apache.derby.iapi.sql.compile.JoinStrategy;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.impl.sql.compile.BaseJoinStrategy;
import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.impl.sql.compile.Predicate;
import org.apache.derby.impl.sql.compile.PredicateList;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.SpliceLogUtils;

public class NestedLoopJoinStrategy extends BaseJoinStrategy {
    private static final Logger LOG = Logger.getLogger(NestedLoopJoinStrategy.class);
	public NestedLoopJoinStrategy() {
	}


	/**
	 * @see JoinStrategy#feasible
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean feasible(Optimizable innerTable,
							OptimizablePredicateList predList,
							Optimizer optimizer
							)
					throws StandardException 
	{
		/* Nested loop is feasible, except in the corner case
		 * where innerTable is a VTI that cannot be materialized
		 * (because it has a join column as a parameter) and
		 * it cannot be instantiated multiple times.
		 * RESOLVE - Actually, the above would work if all of 
		 * the tables outer to innerTable were 1 row tables, but
		 * we don't have that info yet, and it should probably
		 * be hidden in inner table somewhere.
		 * NOTE: A derived table that is correlated with an outer
		 * query block is not materializable, but it can be
		 * "instantiated" multiple times because that only has
		 * meaning for VTIs.
		 */
		if (innerTable.isMaterializable())
		{
			return true;
		}
		if (innerTable.supportsMultipleInstantiations())
		{
			return true;
		}
		return false;
	}

	/** @see JoinStrategy#multiplyBaseCostByOuterRows */
	public boolean multiplyBaseCostByOuterRows() {
		return true;
	}

	/**
	 * @see JoinStrategy#getBasePredicates
	 *
	 * @exception StandardException		Thrown on error
	 */
	public OptimizablePredicateList getBasePredicates(
									OptimizablePredicateList predList,
									OptimizablePredicateList basePredicates,
									Optimizable innerTable)
							throws StandardException {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(basePredicates == null ||
								 basePredicates.size() == 0,
				"The base predicate list should be empty.");
		}

		if (predList != null) {
			predList.transferAllPredicates(basePredicates);
			basePredicates.classify(innerTable,
				innerTable.getCurrentAccessPath().getConglomerateDescriptor());
		}

		return basePredicates;
	}

	/** @see JoinStrategy#nonBasePredicateSelectivity */
	public double nonBasePredicateSelectivity(
										Optimizable innerTable,
										OptimizablePredicateList predList) {
		/*
		** For nested loop, all predicates are base predicates, so there
		** is no extra selectivity.
		*/
		return 1.0;
	}
	
	/**
	 * @see JoinStrategy#putBasePredicates
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void putBasePredicates(OptimizablePredicateList predList,
									OptimizablePredicateList basePredicates)
					throws StandardException {
		for (int i = basePredicates.size() - 1; i >= 0; i--) {
			OptimizablePredicate pred = basePredicates.getOptPredicate(i);

			predList.addOptPredicate(pred);
			basePredicates.removeOptPredicate(i);
		}
	}

	/* @see JoinStrategy#estimateCost */
	public void estimateCost(Optimizable innerTable,
							 OptimizablePredicateList predList,
							 ConglomerateDescriptor cd,
							 CostEstimate outerCost,
							 Optimizer optimizer,
							 CostEstimate costEstimate) {
    	SpliceLogUtils.trace(LOG, "estimateCost innerTable=%s,predList=%s,conglomerateDescriptor=%s,outerCost=%s,optimizer=%s,costEstimate=%s",innerTable,predList,cd,outerCost,optimizer,costEstimate);
    	costEstimate.multiply(outerCost.rowCount(), costEstimate);
		optimizer.trace(Optimizer.COST_OF_N_SCANS, innerTable.getTableNumber(), 0, outerCost.rowCount(),
						costEstimate);
	}

	/** @see JoinStrategy#maxCapacity */
	public int maxCapacity( int userSpecifiedCapacity,
                            int maxMemoryPerTable,
                            double perRowUsage) {
		return Integer.MAX_VALUE;
	}

	/** @see JoinStrategy#getName */
	public String getName() {
		return "NESTEDLOOP";
	}

	/** @see JoinStrategy#scanCostType */
	public int scanCostType() {
		return StoreCostController.STORECOST_SCAN_NORMAL;
	}

	/** @see JoinStrategy#resultSetMethodName */
	public String resultSetMethodName(boolean bulkFetch, boolean multiprobe) {
		if (bulkFetch)
			return "getBulkTableScanResultSet";
		else if (multiprobe)
			return "getMultiProbeTableScanResultSet";
		else
			return "getTableScanResultSet";
	}

	/** @see JoinStrategy#joinResultSetMethodName */
	public String joinResultSetMethodName() {
		return "getNestedLoopJoinResultSet";
	}

	/** @see JoinStrategy#halfOuterJoinResultSetMethodName */
	public String halfOuterJoinResultSetMethodName() {
		return "getNestedLoopLeftOuterJoinResultSet";
	}

	/**
	 * @see JoinStrategy#getScanArgs
	 *
	 * @exception StandardException		Thrown on error
	 */
	public int getScanArgs(
							TransactionController tc,
							MethodBuilder mb,
							Optimizable innerTable,
							OptimizablePredicateList storeRestrictionList,
							OptimizablePredicateList nonStoreRestrictionList,
							ExpressionClassBuilderInterface acbi,
							int bulkFetch,
							MethodBuilder resultRowAllocator,
							int colRefItem,
							int indexColItem,
							int lockMode,
							boolean tableLocked,
							int isolationLevel,
							int maxMemoryPerTable,
							boolean genInListVals
							)
						throws StandardException {
		ExpressionClassBuilder acb = (ExpressionClassBuilder) acbi;
		int numArgs;

		if (SanityManager.DEBUG) {
			if (nonStoreRestrictionList.size() != 0) {
				SanityManager.THROWASSERT(
					"nonStoreRestrictionList should be empty for " +
					"nested loop join strategy, but it contains " +
					nonStoreRestrictionList.size() +
					" elements");
			}
		}

		/* If we're going to generate a list of IN-values for index probing
		 * at execution time then we push TableScanResultSet arguments plus
		 * two additional arguments: 1) the list of IN-list values, and 2)
		 * a boolean indicating whether or not the IN-list values are already
		 * sorted.
		 */
		if (genInListVals)
		{
			numArgs = 26;
		}
		else if (bulkFetch > 1)
		{
            // Bulk-fetch uses TableScanResultSet arguments plus two
            // additional arguments: 1) bulk fetch size, and 2) whether the
            // table contains LOB columns (used at runtime to decide if
            // bulk fetch is safe DERBY-1511).
            numArgs = 26;
		}
		else
		{
			numArgs = 24 ;
		}

		fillInScanArgs1(tc, mb,
										innerTable,
										storeRestrictionList,
										acb,
										resultRowAllocator);

		if (genInListVals)
			((PredicateList)storeRestrictionList).generateInListValues(acb, mb);

		if (SanityManager.DEBUG)
		{
			/* If we're not generating IN-list values with which to probe
			 * the table then storeRestrictionList should not have any
			 * IN-list probing predicates.  Make sure that's the case.
			 */
			if (!genInListVals)
			{
				Predicate pred = null;
				for (int i = storeRestrictionList.size() - 1; i >= 0; i--)
				{
					pred = (Predicate)storeRestrictionList.getOptPredicate(i);
					if (pred.isInListProbePredicate())
					{
						SanityManager.THROWASSERT("Found IN-list probing " +
							"predicate (" + pred.binaryRelOpColRefsToString() +
							") when no such predicates were expected.");
					}
				}
			}
		}

		fillInScanArgs2(mb,
						innerTable,
						bulkFetch,
						colRefItem,
						indexColItem,
						lockMode,
						tableLocked,
						isolationLevel);

		return numArgs;
	}

	/**
	 * @see JoinStrategy#divideUpPredicateLists
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void divideUpPredicateLists(
					Optimizable				 innerTable,
					OptimizablePredicateList originalRestrictionList,
					OptimizablePredicateList storeRestrictionList,
					OptimizablePredicateList nonStoreRestrictionList,
					OptimizablePredicateList requalificationRestrictionList,
					DataDictionary			 dd
					) throws StandardException
	{
		/*
		** All predicates are store predicates.  No requalification is
		** necessary for non-covering index scans.
		*/
		originalRestrictionList.setPredicatesAndProperties(storeRestrictionList);
	}

	/**
	 * @see JoinStrategy#doesMaterialization
	 */
	public boolean doesMaterialization()
	{
		return false;
	}

	public String toString() {
		return getName();
	}

	/**
	 * Can this join strategy be used on the
	 * outermost table of a join.
	 *
	 * @return Whether or not this join strategy
	 * can be used on the outermose table of a join.
	 */
	protected boolean validForOutermostTable() {
		return true;
	}
	
	@Override
	public void oneRowRightResultSetCostEstimate(OptimizablePredicateList predList, CostEstimate outerCost, CostEstimate innerFullKeyCost) {
		SpliceLogUtils.trace(LOG, "oneRowRightResultSetCostEstimate");
		rightResultSetCostEstimate(predList, outerCost, innerFullKeyCost);
	};
	/**
	 * 
	 * (Network Cost + Right Side Cost)* Left Side Number of Rows
	 * 
	 */
	@Override
	public void rightResultSetCostEstimate(OptimizablePredicateList predList, CostEstimate outerCost, CostEstimate innerCost) {
		
		SpliceLogUtils.trace(LOG, "rightResultSetCostEstimate outerCost=%s, innerFullKeyCost=%s",outerCost, innerCost);
		if (outerCost.getEstimatedCost() == 0.0 && outerCost.getEstimatedRowCount() == 1) // I am not really a NLJ but a table scan, do not add network costs...
			return;
		SpliceCostEstimateImpl inner = (SpliceCostEstimateImpl) innerCost;
		inner.setBase(innerCost.cloneMe());
		SpliceCostEstimateImpl outer = (SpliceCostEstimateImpl) outerCost;		
		double rightSideCost = (innerCost.getEstimatedCost()* (double) outer.getEstimatedRowCount()*(double) outer.getEstimatedRowCount()* SpliceConstants.optimizerNetworkCost)/outer.numberOfRegions;
		inner.baseCost.setEstimatedRowCount((long)(innerCost.rowCount() * outer.rowCount()));
		inner.baseCost.setSingleScanRowCount(innerCost.rowCount());
		inner.baseCost.cost = rightSideCost;
		double cost = rightSideCost + outer.getEstimatedCost();
		innerCost.setCost(cost, innerCost.rowCount() * outer.rowCount(), innerCost.rowCount() * outer.rowCount());		
		inner.setNumberOfRegions(outer.numberOfRegions);
		inner.setRowOrdering(outer.rowOrdering);
		SpliceLogUtils.trace(LOG, "rightResultSetCostEstimate computed cost innerCost=%s",innerCost);
	};	
	
}

