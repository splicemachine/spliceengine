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
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.impl.sql.compile.ColumnReference;

public interface SelectivityEstimator {
    enum SelectivityJoinType {
        LEFTOUTER, INNER, ANTIJOIN, FULLOUTER
    }

    enum JoinPredicateType {
        MERGE_SEARCH, /* join conditions on index columns that can be used for merge join to search for matching rows */
        HASH_SEARCH,  /* join conditions that can be used for hash-based joins (like broadcast, sortmerge) to search for matching rows, they should be equality join conditions */
        ALL   /* all join conditions, equality or not */
    }

    double defaultJoinSelectivity(long outerRowCount,
                                  long innerRowCount,
                                  SelectivityJoinType selectivityJoinType) throws StandardException;

    double innerJoinSelectivity(Optimizable rightTable,
                                long leftRowCount,
                                long rightRowCount,
                                ColumnReference leftColumn,
                                ColumnReference rightColumn,
                                SelectivityJoinType selectivityJoinType) throws StandardException;

    double leftOuterJoinSelectivity(long leftRowCount,
                                    long rightRowCount,
                                    ColumnReference leftColumn,
                                    ColumnReference rightColumn) throws StandardException;

    double fullOuterJoinSelectivity(long leftRowCount,
                                    long rightRowCount,
                                    ColumnReference leftColumn,
                                    ColumnReference rightColumn) throws StandardException;

    boolean checkJoinSelectivity(double selectivity);
}
