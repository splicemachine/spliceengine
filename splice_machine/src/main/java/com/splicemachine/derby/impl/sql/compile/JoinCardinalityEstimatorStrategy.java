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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.impl.sql.compile.BaseJoinStrategy;
import org.apache.log4j.Logger;

/**
 * This class is not a read join strategy but used as a join strategy "alternative"
 * by the optimiser to allow calculating the join cardinality consistently for all
 * join strategies. It is important to set this strategy as a start strategy for the
 * optimizer for its side-effect.
 *
 * this class is only relevant for V2 cost estimation model. For more information
 * have a look at <code>V2JoinCardinalityEstimation</code>
 */
public class JoinCardinalityEstimatorStrategy extends BaseJoinStrategy {
    private static final Logger LOG=Logger.getLogger(JoinCardinalityEstimatorStrategy.class);

    public JoinCardinalityEstimatorStrategy() { }

    @Override
    public String joinResultSetMethodName() {
        throw new UnsupportedOperationException("Join Cardinality Estimator doesn't support joinResultSetMethodName");
    }

    @Override
    public String halfOuterJoinResultSetMethodName() {
        throw new UnsupportedOperationException("Join Cardinality Estimator doesn't support halfOuterJoinResultSetMethodName");
    }

    @Override
    public String fullOuterJoinResultSetMethodName() {
        throw new UnsupportedOperationException("Join Cardinality Estimator doesn't support fullOuterJoinResultSetMethodName");
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
        throw new UnsupportedOperationException("Join Cardinality Estimator doesn't support getScanArgs");
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
        return false;
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
        return -1.0d;
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
        return optimizer.getCostModel().toString().equals("v2");
    }

    @Override
    public int maxCapacity(int userSpecifiedCapacity, int maxMemoryPerTable, double perRowUsage) {
        return -1;
    }

    @Override
    public JoinStrategyType getJoinStrategyType() {
        return JoinStrategyType.CARDINALITY_ESTIMATOR;
    }
}

