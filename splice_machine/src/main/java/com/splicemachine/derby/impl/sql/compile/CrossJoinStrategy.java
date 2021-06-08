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
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.logging.log4j.Logger;

public class CrossJoinStrategy extends BaseJoinStrategy {
    private static final Logger LOG=org.apache.logging.log4j.LogManager.getLogger(CrossJoinStrategy.class);

    public CrossJoinStrategy() { }

    /**
     * @see JoinStrategy#joinResultSetMethodName
     */
    @Override
    public String joinResultSetMethodName() {
        return "getCrossJoinResultSet";
    }

    /**
     * @see JoinStrategy#halfOuterJoinResultSetMethodName
     */
    @Override
    public String halfOuterJoinResultSetMethodName() {
        throw new UnsupportedOperationException("Cross join doesn't support half outer join");
    }

    @Override
    public String fullOuterJoinResultSetMethodName() {
        throw new UnsupportedOperationException("Cross full join not supported currently");
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
            boolean genInListVals, String tableVersion,
            int splits,
            String delimited,
            String escaped,
            String lines,
            String storedAs,
            String location,
            int partitionRefItem
    ) throws StandardException {
        ExpressionClassBuilder acb = (ExpressionClassBuilder) acbi;
        int numArgs;
        /* If we're going to generate a list of IN-values for index probing
         * at execution time then we push TableScanResultSet arguments plus
         * four additional arguments: 1) the list of IN-list values, and 2)
         * a boolean indicating whether or not the IN-list values are already
         * sorted, 3) the in-list column position in the index or primary key,
         * 4) array of types of the in-list columns
         */
        if (genInListVals) {
            numArgs = 38;
        }
        else {
            numArgs = 34 ;
        }

        fillInScanArgs1(tc, mb, innerTable, storeRestrictionList, acb, resultRowAllocator);
        if (genInListVals) {
            ((PredicateList) storeRestrictionList).generateInListValues(acb, mb);
        }

        if (SanityManager.DEBUG) {
            /* If we're not generating IN-list values with which to probe
             * the table then storeRestrictionList should not have any
             * IN-list probing predicates.  Make sure that's the case.
             */
            if (!genInListVals) {
                Predicate pred = null;
                for (int i = storeRestrictionList.size() - 1; i >= 0; i--) {
                    pred = (Predicate)storeRestrictionList.getOptPredicate(i);
                    if (pred.isInListProbePredicate()) {
                        SanityManager.THROWASSERT("Found IN-list probing " +
                                "predicate (" + pred.binaryRelOpColRefsToString() +
                                ") when no such predicates were expected.");
                    }
                }
            }
        }

        fillInScanArgs2(mb,innerTable, bulkFetch, colRefItem, indexColItem, lockMode, tableLocked, isolationLevel,tableVersion,
                splits, delimited, escaped, lines, storedAs, location, partitionRefItem);
        return numArgs;
    }


    @Override
    public void divideUpPredicateLists(Optimizable innerTable,
                                       JBitSet joinedTableSet,
                                       OptimizablePredicateList originalRestrictionList,
                                       OptimizablePredicateList storeRestrictionList,
                                       OptimizablePredicateList nonStoreRestrictionList,
                                       OptimizablePredicateList requalificationRestrictionList,
                                       DataDictionary dd) throws StandardException {
       originalRestrictionList.transferPredicates(storeRestrictionList, innerTable.getReferencedTableMap(), innerTable, joinedTableSet);
       originalRestrictionList.copyPredicatesToOtherList(nonStoreRestrictionList);
    }

    @Override
    public boolean doesMaterialization() {
        return false;
    }

    /** @see JoinStrategy#multiplyBaseCostByOuterRows */
    public boolean multiplyBaseCostByOuterRows() {
        return true;
    }

    @Override
    public OptimizablePredicateList getBasePredicates(OptimizablePredicateList predList, OptimizablePredicateList basePredicates, Optimizable innerTable) throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(basePredicates.size() == 0,"The base predicate list should be empty.");
        }

        if (predList != null) {
            predList.transferAllPredicates(basePredicates);
            basePredicates.classify(innerTable, innerTable.getCurrentAccessPath(), false);
        }

        /*
         * We want all the join predicates to be included, so we just pass everything through and filter
         * it out through the actual costing algorithm
         */
        return basePredicates;
    }

    @Override
    public double nonBasePredicateSelectivity(Optimizable innerTable, OptimizablePredicateList predList) throws StandardException {
        return 1.0;
    }

    @Override
    public void putBasePredicates(OptimizablePredicateList predList, OptimizablePredicateList basePredicates) throws StandardException {
        for (int i = basePredicates.size() - 1; i >= 0; i--) {
            OptimizablePredicate pred = basePredicates.getOptPredicate(i);
            predList.addOptPredicate(pred);
            basePredicates.removeOptPredicate(i);
        }
    }

    @Override
    public boolean feasible(Optimizable innerTable,
                            OptimizablePredicateList predList,
                            Optimizer optimizer,
                            CostEstimate outerCost,
                            boolean wasHinted,
                            boolean skipKeyCheck) throws StandardException {
        if (innerTable.indexFriendlyJoinsOnly())
            return false;

        if (optimizer.getJoinPosition() > 0 && innerTable.outerTableOnly())
            return false;

        // cross join strategy cannot be applied to the very left table as it is not a join but just a scan
        if(outerCost.isUninitialized() ||(outerCost.localCost()==0d)) {
            return false;
        }

        // Cross join can't handle IndexLookups on the inner table currently because
        // the join predicates get mapped to the IndexScan instead of the CrossJoin.
        // Broadcast join has a similar restriction.
        if (JoinStrategyUtil.isNonCoveringIndex(innerTable))
                return false;
        
        boolean isSpark = optimizer.isForSpark();
        AccessPath currentAccessPath = innerTable.getCurrentAccessPath();
        boolean isHinted = currentAccessPath.isHintedJoinStrategy();
        boolean isOneRow = ((FromTable)innerTable).isOneRowResultSet();

        // Only use cross join when it is inner join, and not a semi-join
        // Only use cross join when it is on Spark
        return !outerCost.isOuterJoin() && isSpark && (innerTable instanceof FromBaseTable || isHinted) && !isOneRow;
    }

    @Override
    public int maxCapacity(int userSpecifiedCapacity, int maxMemoryPerTable, double perRowUsage) {
        return Integer.MAX_VALUE;
    }

    @Override
    public JoinStrategyType getJoinStrategyType() {
        return JoinStrategyType.CROSS;
    }

    protected boolean validForOutermostTable() {
        return true;
    }

    @Override
    public boolean getBroadcastRight(CostEstimate rightCost) {
        double estimatedRowCount = rightCost.getEstimatedRowCount();
        SConfiguration configuration=EngineDriver.driver().getConfiguration();
        long rowCountThreshold = configuration.getBroadcastRegionRowThreshold();
        return estimatedRowCount < rowCountThreshold;
    }
}

