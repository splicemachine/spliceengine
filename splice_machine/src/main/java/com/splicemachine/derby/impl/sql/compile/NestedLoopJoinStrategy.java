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
            boolean genInListVals) throws StandardException{
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
            numArgs=27;
        }else if(bulkFetch>1){
            // Bulk-fetch uses TableScanResultSet arguments plus two
            // additional arguments: 1) bulk fetch size, and 2) whether the
            // table contains LOB columns (used at runtime to decide if
            // bulk fetch is safe DERBY-1511).
            numArgs=27;
        }else{
            numArgs=25;
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
                isolationLevel);

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
//        outerCost.setBase(outerCost.cloneMe());

        /*
         * NestedLoopJoins are very simple. For each outer row, we create a new scan of the inner
         * table, looking for rows which match the join predicates. Therefore, for each outer row,
         * we must pay the local penalty of
         *
         * innerCost.localCost
         *
         * and the remote penalty of
         *
         * innerCost.remoteCost * (number of rows matching join predicates)
         *
         * the number of rows matching the join predicate is the "join selectivity", so a better formulation is:
         *
         * innerScan.outputRows = joinSelectivity*innerCost.outputRows
         * innerScan.localCost = innerCost.localCost
         * innerScan.remoteCost = innerCost.remoteCost*innerCost.outputRows
         * innerScan.heapSize = innerCost.heapSize*joinSelectivity
         *
         * Note,however, that some join predicates may be start and stop predicates, which will reduce
         * the number of rows we have to touch during the inner scan. As a result, we have to keep
         * 2 selectivities: the "output join selectivity" and the "input join selectivity". This adjusts the formulas
         * to be:
         *
         * innerScan.outputRows = outputJoinSelectivity*innerCost.outputRows
         * innerScan.localCost = inputJoinSelectivity*innerCost.localCost
         * innerScan.remoteCost = outputJoinSelectivity*innerCost.remoteCost
         * innerScan.heapSize = outputJoinSelectivity*innerCost.heapSize
         *
         * This the cost made *per outer row*, so our overall cost formula is:
         *
         * totalCost.localCost = outerCost.localCost + outerCost.outputRows*(innerScan.localCost+innerScan.remoteCost)
         * totalCost.remoteCost = outerCost.remoteCost + outerCost.outputRows*(innerScan.remoteCost)
         * totalCost.outputRows = outerCost.outputRows
         * totalCost.heapSize = outerCost.heapSize
         * totalCost.numPartitions = outerCost.numPartitions + innerCost.numPartitions
         *
         * Note that we add in the remote cost of the inner scan twice. This accounts for the fact that we have
         * to read the innerScan's rows over the network twice--once to pull them to the outer table's region,
         * and again to write that data across the network.
         */

        /*
         * If the row count is 1l, then we are either a keyed lookup (a special case handled
         * in FromBaseTable directly), or we are on a table with exactly 1 row returned (which is wild).
         *
         * If we are the latter, then not adjusting for start and stop key selectivity won't matter,
         * and if we are the former, it'll screw up our costing model. So in both cases, don't
         * adjust the selectivity if our estimated row count is 1
         */

        /*
         * if the outer table claims to have no rows return, then
         * we are in a bit of a dilemma. On the one hand, maybe nothing
         * comes back, in which case that's correct. Or maybe there are no
         * statistics, and we are just screwed anyway.
         *
         * Regardless, we don't want to mess up our own statistics by dividing by
         * zero here and so forth, so we choose to accept some error for very small
         * tables by resetting the output row count to 1 if it's less than that already.
         */
        double outerRowCount = outerCost.rowCount();
        if(outerRowCount<1) outerRowCount=1d;

        double innerScanLocalCost = innerCost.localCost();
        double innerScanRemoteCost = innerCost.remoteCost();
        double innerScanHeapSize = innerCost.getEstimatedHeapSize();
        double innerScanOutputRows = innerCost.rowCount();

        double perOuterRowInnerCost = innerScanLocalCost+innerScanRemoteCost;
        perOuterRowInnerCost+=innerCost.partitionCount()*(innerCost.getOpenCost()+innerCost.getCloseCost());

        double totalLocalCost=outerCost.localCost()+outerCost.rowCount()*perOuterRowInnerCost;
        int totalPartitions=outerCost.partitionCount();

        /*
         * unlike other join strategies, NLJ's row count selectivity is determined entirely by
         * the predicates which are pushed to the right hand side. Therefore, the totalOutputRows
         * is actually outerCost.rowCount()*innerScanOutputRows
         */
        double totalOutputRows=outerCost.rowCount()*innerCost.rowCount();
        double totalHeapSize=outerCost.getEstimatedHeapSize()+outerCost.rowCount()*innerScanHeapSize;

        double perRowRemoteCost = outerCost.remoteCost()/outerRowCount;
        if(innerScanOutputRows>0){
            perRowRemoteCost+=innerCost.remoteCost()/innerCost.rowCount();
        }
        double totalRemoteCost=totalOutputRows*perRowRemoteCost;

        innerCost.setEstimatedHeapSize((long)totalHeapSize);
        innerCost.setNumPartitions(totalPartitions);
        innerCost.setRowCount(totalOutputRows);
        innerCost.setRemoteCost(totalRemoteCost);
        innerCost.setLocalCost(totalLocalCost);
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

