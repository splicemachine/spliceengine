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

package com.splicemachine.derby.impl.sql.compile.costing.v2;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.compile.costing.SelectivityEstimator;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.impl.sql.compile.SelectivityUtil;
import com.splicemachine.derby.impl.sql.compile.costing.StrategyJoinCostEstimation;

public class V2JoinCardinalityEstimation implements StrategyJoinCostEstimation {
    @Override
    public void estimateCost(Optimizable innerTable,
                             OptimizablePredicateList predList,
                             ConglomerateDescriptor cd,
                             CostEstimate outerCost,
                             CostEstimate innerCost,
                             Optimizer optimizer,
                             SelectivityEstimator selectivityEstimator) throws StandardException {

        if(outerCost.isUninitialized() ||(outerCost.localCost()==0d && outerCost.getEstimatedRowCount()==1.0)){
            /*
             * Derby calls this method at the end of each table scan, even if it's not a join (or if it's
             * the left side of the join). When this happens, the outer cost is still uninitialized, so there's
             * nothing to do in this method;
             */
            RowOrdering ro = outerCost.getRowOrdering();
            if(ro!=null)
                outerCost.setRowOrdering(ro); //force a cloning
            return;
        }

        /* Use estimateJoinSelectivity() to estimate output row count of this join. This is the routine used
         * by all join strategies except NLJ. Our goal is to make the output row count of this join the same
         * for all join strategies. Since NLJ has its own way of estimating it, we need to make this number
         * available to NLJ.
         */
        innerCost.setBase(innerCost.cloneMe());
        double joinSelectivity = SelectivityUtil.estimateJoinSelectivity(selectivityEstimator, innerTable, cd, predList,
                                                                         (long) innerCost.rowCount(),
                                                                         (long) outerCost.rowCount(), outerCost,
                                                                         SelectivityEstimator.JoinPredicateType.ALL);
        double totalOutputRows = SelectivityUtil.getTotalRows(joinSelectivity, outerCost.rowCount(), innerCost.rowCount());
        outerCost.setJoinSelectionCardinality(totalOutputRows);
        innerCost.setLocalCost(Double.MAX_VALUE);
        innerCost.setRemoteCost(Double.MAX_VALUE);
        innerCost.setLocalCostPerParallelTask(Double.MAX_VALUE);
        innerCost.setRemoteCostPerParallelTask(Double.MAX_VALUE); // prevent the optimiser from choosing this strategy.
    }
}
