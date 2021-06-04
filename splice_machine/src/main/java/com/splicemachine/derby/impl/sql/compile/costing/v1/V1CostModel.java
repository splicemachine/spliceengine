/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.compile.costing.v1;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.compile.costing.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.costing.CostModel;
import com.splicemachine.db.iapi.sql.compile.costing.ScanCostEstimator;
import com.splicemachine.db.iapi.sql.compile.costing.SelectivityEstimator;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.compile.ResultColumnList;
import com.splicemachine.derby.impl.sql.compile.costing.StrategyJoinCostEstimation;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class V1CostModel implements CostModel {

    private static Map<JoinStrategy.JoinStrategyType, StrategyJoinCostEstimation> joinCostEstimationMap = new ConcurrentHashMap<>();
    static {
        joinCostEstimationMap.put(JoinStrategy.JoinStrategyType.BROADCAST, new V1BroadcastJoinCostEstimation());
        joinCostEstimationMap.put(JoinStrategy.JoinStrategyType.CROSS, new V1CrossJoinCostEstimation());
        joinCostEstimationMap.put(JoinStrategy.JoinStrategyType.MERGE, new V1MergeJoinCostEstimation());
        joinCostEstimationMap.put(JoinStrategy.JoinStrategyType.MERGE_SORT, new V1MergeSortJoinCostEstimation());
        joinCostEstimationMap.put(JoinStrategy.JoinStrategyType.NESTED_LOOP, new V1NestedLoopJoinCostEstimation());
    }

    private static final SelectivityEstimator selectivityEstimator = new V1SelectivityEstimator();

    @Override
    public void estimateJoinCost(JoinStrategy.JoinStrategyType joinStrategyType,
                                 Optimizable innerTable,
                                 OptimizablePredicateList predList,
                                 ConglomerateDescriptor cd,
                                 CostEstimate outerCost,
                                 Optimizer optimizer,
                                 CostEstimate costEstimate) throws StandardException {
        StrategyJoinCostEstimation joinCostEstimation = joinCostEstimationMap.get(joinStrategyType);
        if(SanityManager.DEBUG) {
            SanityManager.ASSERT(joinCostEstimation != null,
                                 String.format("unexpected missing cost estimation for %s", joinStrategyType.niceName()));
        }
        joinCostEstimation.estimateCost(innerTable, predList, cd, outerCost, costEstimate, optimizer, selectivityEstimator);
    }

    @Override
    public ScanCostEstimator getNewScanCostEstimator(Optimizable baseTable,
                                                     ConglomerateDescriptor cd,
                                                     StoreCostController scc,
                                                     CostEstimate scanCost,
                                                     ResultColumnList resultColumns,
                                                     DataValueDescriptor[] scanRowTemplate,
                                                     BitSet baseColumnsInScan,
                                                     BitSet baseColumnsInLookup,
                                                     int indexLookupBatchRowCount,
                                                     int indexLookupConcurrentBatchesCount,
                                                     boolean forUpdate,
                                                     boolean isOlap,
                                                     HashSet<Integer> usedNoStatsColumnIds) throws StandardException {
        return new V1ScanCostEstimator(baseTable, cd, scc, scanCost, resultColumns, scanRowTemplate,
                                       baseColumnsInScan, baseColumnsInLookup, indexLookupBatchRowCount,
                                       indexLookupConcurrentBatchesCount, forUpdate, isOlap, usedNoStatsColumnIds);
    }

    @Override
    public String toString() {
        return "v1";
    }
}
