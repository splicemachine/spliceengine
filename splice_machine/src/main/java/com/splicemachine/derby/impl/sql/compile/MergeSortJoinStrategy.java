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
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.compile.costing.CostEstimate;
import com.splicemachine.db.impl.sql.compile.HashableJoinStrategy;

public class MergeSortJoinStrategy extends HashableJoinStrategy {

    public MergeSortJoinStrategy() {
    }

    @Override
	public boolean feasible(Optimizable innerTable,
                            OptimizablePredicateList predList,
                            Optimizer optimizer,
                            CostEstimate outerCost, boolean wasHinted,
                            boolean skipKeyCheck) throws StandardException {
        if (innerTable.indexFriendlyJoinsOnly())
            return false;
		return correlatedSubqueryRestriction(optimizer, predList, outerCost.getJoinType())
                && super.feasible(innerTable, predList, optimizer,outerCost,wasHinted,skipKeyCheck);
	}

    /** @see JoinStrategy#multiplyBaseCostByOuterRows */
    @Override
    public boolean multiplyBaseCostByOuterRows() {
		return true;
	}

    /**
     * @see JoinStrategy#joinResultSetMethodName
     */
    @Override
    public String joinResultSetMethodName() {
        return "getMergeSortJoinResultSet";
    }

    /**
     * @see JoinStrategy#halfOuterJoinResultSetMethodName
     */
    @Override
    public String halfOuterJoinResultSetMethodName() {
        return "getMergeSortLeftOuterJoinResultSet";
    }

    @Override
    public String fullOuterJoinResultSetMethodName() {
        return "getMergeSortFullOuterJoinResultSet";
    }

    @Override
    public JoinStrategyType getJoinStrategyType() {
        return JoinStrategyType.MERGE_SORT;
    }

    // DB-3460: For an outer left join query, sort merge join was ruled out because it did not qualify memory
    // requirement for hash joins. Sort merge join requires substantially less memory than other hash joins, so
    // maxCapacity() is override to return a very large integer to bypass memory check.
    @Override
    public int maxCapacity(int userSpecifiedCapacity,int maxMemoryPerTable,double perRowUsage){
        return Integer.MAX_VALUE;
    }

    private boolean correlatedSubqueryRestriction(Optimizer optimizer,
                                                  OptimizablePredicateList predList,
                                                  int joinType) throws StandardException {
        if (optimizer.isForSpark()
            && (containsCorrelatedSubquery(optimizer.getNonPushablePredicates()) || containsCorrelatedSubquery(predList))) {
            return false;
        }
        return true;
    }

    private boolean containsCorrelatedSubquery(OptimizablePredicateList predList) throws StandardException {
        if(predList == null) {
            return false;
        }
        for (int i = 0; i < predList.size(); i++) {
            if(predList.getOptPredicate(i).hasCorrelatedSubquery()) {
                return true;
            }
        }
        return false;
    }
}
