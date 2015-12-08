package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

public class NestedLoopJoinStrategy extends BaseJoinStrategy{
    private static final Logger LOG=Logger.getLogger(NestedLoopJoinStrategy.class);

    public NestedLoopJoinStrategy(){
    }

    @Override
    public boolean feasible(Optimizable innerTable,
                            OptimizablePredicateList predList,
                            Optimizer optimizer,
                            CostEstimate outerCost,
                            boolean wasHinted) throws StandardException{
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
        return innerTable.isMaterializable() || innerTable.supportsMultipleInstantiations();
    }

    @Override
    public boolean multiplyBaseCostByOuterRows(){
        return true;
    }

    @Override
    public OptimizablePredicateList getBasePredicates(OptimizablePredicateList predList,
                                                      OptimizablePredicateList basePredicates,
                                                      Optimizable innerTable) throws StandardException{
        assert (basePredicates==null || basePredicates.size()==0):"The base predicate list should be empty.";

        if(predList!=null && basePredicates!=null){
            predList.transferAllPredicates(basePredicates);
            basePredicates.classify(innerTable,innerTable.getCurrentAccessPath().getConglomerateDescriptor());
        }

        return basePredicates;
    }

    @Override
    public double nonBasePredicateSelectivity(Optimizable innerTable,OptimizablePredicateList predList){
		/*
		** For nested loop, all predicates are base predicates, so there
		** is no extra selectivity.
		*/
        return 1.0;
    }

    @Override
    public void putBasePredicates(OptimizablePredicateList predList,OptimizablePredicateList basePredicates) throws StandardException{
        for(int i=basePredicates.size()-1;i>=0;i--){
            OptimizablePredicate pred=basePredicates.getOptPredicate(i);

            predList.addOptPredicate(pred);
            basePredicates.removeOptPredicate(i);
        }
    }

    @Override
    public int maxCapacity(int userSpecifiedCapacity,int maxMemoryPerTable,double perRowUsage){
        return Integer.MAX_VALUE;
    }

    @Override
    public String getName(){
        return "NESTEDLOOP";
    }

    @Override
    public String resultSetMethodName(boolean bulkFetch,boolean multiprobe){
        if(bulkFetch)
            return "getBulkTableScanResultSet";
        else if(multiprobe)
            return "getMultiProbeTableScanResultSet";
        else
            return "getTableScanResultSet";
    }
    @Override
    public String joinResultSetMethodName(){
        return "getNestedLoopJoinResultSet";
    }

    @Override
    public String halfOuterJoinResultSetMethodName(){
        return "getNestedLoopLeftOuterJoinResultSet";
    }

    @Override
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
            boolean genInListVals,
            String tableVersion) throws StandardException{
        ExpressionClassBuilder acb=(ExpressionClassBuilder)acbi;
        int numArgs;

        if(SanityManager.DEBUG){
            if(nonStoreRestrictionList.size()!=0){
                SanityManager.THROWASSERT(
                        "nonStoreRestrictionList should be empty for "+
                                "nested loop join strategy, but it contains "+
                                nonStoreRestrictionList.size()+
                                " elements");
            }
        }

		/* If we're going to generate a list of IN-values for index probing
		 * at execution time then we push TableScanResultSet arguments plus
		 * two additional arguments: 1) the list of IN-list values, and 2)
		 * a boolean indicating whether or not the IN-list values are already
		 * sorted.
		 */
        if(genInListVals){
            numArgs=29;
        }else if(bulkFetch>1){
            // Bulk-fetch uses TableScanResultSet arguments plus two
            // additional arguments: 1) bulk fetch size, and 2) whether the
            // table contains LOB columns (used at runtime to decide if
            // bulk fetch is safe DERBY-1511).
            numArgs=29;
        }else{
            numArgs=27;
        }

        fillInScanArgs1(tc,mb,
                innerTable,
                storeRestrictionList,
                acb,
                resultRowAllocator);

        if(genInListVals)
            ((PredicateList)storeRestrictionList).generateInListValues(acb,mb);

        if(SanityManager.DEBUG){
			/* If we're not generating IN-list values with which to probe
			 * the table then storeRestrictionList should not have any
			 * IN-list probing predicates.  Make sure that's the case.
			 */
            if(!genInListVals){
                Predicate pred;
                for(int i=storeRestrictionList.size()-1;i>=0;i--){
                    pred=(Predicate)storeRestrictionList.getOptPredicate(i);
                    if(pred.isInListProbePredicate()){
                        SanityManager.THROWASSERT("Found IN-list probing "+
                                "predicate ("+pred.binaryRelOpColRefsToString()+
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
                isolationLevel,
                tableVersion);

        return numArgs;
    }

    @Override
    public void divideUpPredicateLists(
            Optimizable innerTable,
            OptimizablePredicateList originalRestrictionList,
            OptimizablePredicateList storeRestrictionList,
            OptimizablePredicateList nonStoreRestrictionList,
            OptimizablePredicateList requalificationRestrictionList,
            DataDictionary dd) throws StandardException{
		/*
		** All predicates are store predicates.  No requalification is
		** necessary for non-covering index scans.
		*/
        originalRestrictionList.setPredicatesAndProperties(storeRestrictionList);
    }

    @Override
    public boolean doesMaterialization(){
        return false;
    }

    @Override
    public String toString(){
        return "NestedLoopJoin";
    }

    @Override
    public void estimateCost(Optimizable innerTable,
                             OptimizablePredicateList predList,
                             ConglomerateDescriptor cd,
                             CostEstimate outerCost,
                             Optimizer optimizer,
                             CostEstimate innerCost) throws StandardException{

        SpliceLogUtils.trace(LOG,"rightResultSetCostEstimate outerCost=%s, innerFullKeyCost=%s",outerCost,innerCost);
        if(outerCost.isUninitialized() ||(outerCost.localCost()==0d && outerCost.getEstimatedRowCount()==1.0)){
            /*
             * Derby calls this method at the end of each table scan, even if it's not a join (or if it's
             * the left side of the join). When this happens, the outer cost is still unitialized, so there's
             * nothing to do in this method;
             */
            RowOrdering ro = outerCost.getRowOrdering();
            if(ro!=null)
                outerCost.setRowOrdering(ro); //force a cloning
            return;
        }
        //set the base costs for the join
        innerCost.setBase(innerCost.cloneMe());
        double totalRowCount = outerCost.rowCount()*innerCost.rowCount();

        innerCost.setEstimatedHeapSize((long)SelectivityUtil.getTotalHeapSize(innerCost,outerCost,totalRowCount));
        innerCost.setNumPartitions(outerCost.partitionCount());
        innerCost.setRowCount(totalRowCount);
        innerCost.setRemoteCost(SelectivityUtil.getTotalRemoteCost(innerCost,outerCost,totalRowCount));
        innerCost.setLocalCost(SelectivityUtil.nestedLoopJoinStrategyLocalCost(innerCost,outerCost));
        innerCost.setRowOrdering(outerCost.getRowOrdering());
        innerCost.setSingleScanRowCount(innerCost.getEstimatedRowCount());
    }

    /**
     * Can this join strategy be used on the
     * outermost table of a join.
     *
     * @return Whether or not this join strategy
     * can be used on the outermose table of a join.
     */
    @Override
    protected boolean validForOutermostTable(){
        return true;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    @Override
    public JoinStrategyType getJoinStrategyType() {
        return JoinStrategyType.NESTED_LOOP;
    }

}

