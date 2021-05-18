/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.SessionProperties;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import static com.splicemachine.db.impl.sql.compile.JoinNode.INNERJOIN;

public class NestedLoopJoinStrategy extends BaseJoinStrategy{

    public NestedLoopJoinStrategy(){
    }

    @Override
    public boolean feasible(Optimizable innerTable,
                            OptimizablePredicateList predList,
                            Optimizer optimizer,
                            CostEstimate outerCost,
                            boolean wasHinted,
                            boolean skipKeyCheck) throws StandardException{
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

        if (outerCost != null && outerCost.getJoinType() == JoinNode.FULLOUTERJOIN)
            return false;

        if (isJoinWithTriggerRows(innerTable, optimizer))
            return false;

        return innerTable.isMaterializable() || innerTable.supportsMultipleInstantiations();
    }

    // Nested loop join on Spark does not work correctly when using a common
    // Dataset to access the trigger REFERENCING NEW/OLD TABLE rows:
    //         see useCommonDataSet in TriggerNewTransitionRows.
    // The compilation of a trigger is saved as a stored prepared statement in
    // the data dictionary, and reloaded/reused by each new triggering statement.
    // Even if the trigger is compiled to run in OLTP mode, if the triggering
    // statement runs in OLAP, the trigger must run in OLAP too.  Since we
    // cannot tell from the SPSDescriptor whether the trigger was compiled
    // to run on OLTP or OLAP, we would not be able to detect when an
    // OLTP-compiled trigger which uses nested loop join would need to be
    // recompiled as forced-OLAP, and avoid choosing nested loop join.
    // Therefore we must always avoid nested loop join for statement triggers
    // with a REFERENCING clause, even if compiled for OLTP execution.
    private boolean isJoinWithTriggerRows(Optimizable innerTable, Optimizer optimizer) {
        if (!isSingleTableScan(optimizer)) {
            if (innerTable.isTriggerVTI())
                return true;
            ResultSetNode outerTable = optimizer.getOuterTable();
            if (outerTable instanceof Optimizable) {
                Optimizable outerOptimizable = (Optimizable)outerTable;
                if (outerOptimizable.isTriggerVTI())
                    return true;
            }
        }
        return false;
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
            basePredicates.classify(innerTable,innerTable.getCurrentAccessPath(), true);
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
    public String joinResultSetMethodName(){
        return "getNestedLoopJoinResultSet";
    }

    @Override
    public String halfOuterJoinResultSetMethodName(){
        return "getNestedLoopLeftOuterJoinResultSet";
    }

    @Override
    public String fullOuterJoinResultSetMethodName() {
        throw new UnsupportedOperationException("NestedLoop full join not supported currently");
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
            int splits,
            String delimited,
            String escaped,
            String lines,
            String storedAs,
            String location,
            int partitionReferenceItem
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
         * four additional arguments: 1) the list of IN-list values, and 2)
         * a boolean indicating whether or not the IN-list values are already
         * sorted, 3) the in-list column position in the index or primary key,
         * 4) array of types of the in-list columns
         */
        if(genInListVals){
            numArgs=38;
        }else{
            numArgs=34;
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
            if(!genInListVals && !innerTable.hasIndexPrefixIterator()){
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
                splits,
                delimited,
                escaped,
                lines,
                storedAs,
                location,
                partitionReferenceItem
                );

        return numArgs;
    }

    @Override
    public void divideUpPredicateLists(
            Optimizable innerTable,
            JBitSet joinedTableSet,
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

