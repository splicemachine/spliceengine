/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

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
            String tableVersion,
            boolean pin,
            String delimited,
            String escaped,
            String lines,
            String storedAs,
            String location
            ) throws StandardException{
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
            numArgs=35;
        }else{
            numArgs=33;
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
                tableVersion,
                pin,
                delimited,
                escaped,
                lines,
                storedAs,
                location
                );

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

        innerCost.setEstimatedHeapSize((long) SelectivityUtil.getTotalHeapSize(innerCost, outerCost, totalRowCount));
        innerCost.setNumPartitions(outerCost.partitionCount());
        innerCost.setRowCount(totalRowCount);
        innerCost.setRemoteCost(SelectivityUtil.getTotalRemoteCost(innerCost, outerCost, totalRowCount));
        double joinCost = SelectivityUtil.nestedLoopJoinStrategyLocalCost(innerCost, outerCost);
        innerCost.setLocalCost(joinCost);
        innerCost.setLocalCostPerPartition(joinCost);
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

