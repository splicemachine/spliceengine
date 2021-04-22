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

package com.splicemachine.db.iapi.sql.compile.costing;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;

/**
 * The join cost estimation interface, for a given join strategy,
 * it returns the corresponding cost.
 */
public interface JoinCostEstimationModel {

    /**
     * Get the estimated cost of a specific join strategy.
     * @param joinStrategyType The join strategy type to estimate the cost of
     * @param predList         The predicate list for the join
     * @param innerTable       The inner table to join with
     * @param cd               The conglomerate descriptor (if appropriate) to get
     *                         the cost of
     * @param outerCost        The estimated cost of the part of the plan outer
     *                         to the inner table
     * @param optimizer        The optimizer to use to help estimate the cost
     * @param costEstimate     The estimated cost of doing a single scan of the
     *                         inner table, to be filled in with the cost of
     *                         doing the join.
     */
    void estimateCost(JoinStrategy.JoinStrategyType joinStrategyType,
                      Optimizable innerTable,
                      OptimizablePredicateList predList,
                      ConglomerateDescriptor cd,
                      CostEstimate outerCost,
                      Optimizer optimizer,
                      CostEstimate costEstimate) throws StandardException;
}
