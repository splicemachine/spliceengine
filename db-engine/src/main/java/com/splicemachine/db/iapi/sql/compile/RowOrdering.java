/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.compile.ColumnOrdering;

/**
 * This interface provides a representation of the ordering of rows in a
 * ResultSet.
 */
public interface RowOrdering{
    int ASCENDING=1;
    int DESCENDING=2;
    int DONTCARE=3;

    /**
     * Get an iterable over the Columns. The iteration order is the same as the sort order
     * of this row ordering--for example,  if we have an ORDER BY A desc, B asc, then the first
     * column ordering will be A and the second will be B, even if A's column number comes after B.
     *
     * @return an iterable over the known ordered columns.
     */
    Iterable<ColumnOrdering> orderedColumns();

    /**
     * @param orderPosition The position in the ordering list. For example, for ORDER BY A, B, position
     *                      0 has column A, and position 1 has column B. Note that for an ordering,more than
     *                      one column can be in a single ordering position: for example, in the query
     *                      SELECT * from S, T where S.A = T.B order by T.B, columns S.A and T.B will
     *                      be in the same ordering position because they are equal. Also, constant
     *                      values are considered ordered in all positions (consider SELECT A from
     *                      T WHERE A = 1 ORDER BY A).
     * @param tableNumber the table number of the optimizable containing the column in question
     * @param columnNumber the column number in the table (one-based).
     * @throws StandardException if something goes wrong
     * @return the ColumnOrdering for the specified column, or {@code null} if the specified
     * column is not ordered.
     */
    ColumnOrdering ordering(int orderPosition, int tableNumber, int columnNumber) throws StandardException;

    /**
     * Tell whether this ordering is ordered on the given column in
     * the given position
     *
     * @param direction     One of ASCENDING, DESCENDING, or DONTCARE
     *                      depending on the requirements of the caller.
     *                      An ORDER BY clause cares about direction,
     *                      while DISTINCT and GROUP BY do not.
     * @param orderPosition The position in the ordering list.  For example,
     *                      for ORDER BY A, B, position 0 has column A,
     *                      and position 1 has column B.  Note that for an
     *                      ordering, more than one column can be in a single
     *                      ordering position: for example, in the query
     *                      SELECT * FROM S, T WHERE S.A = T.B ORDER BY T.B
     *                      columns S.A and T.B will be in the same ordering
     *                      positions because they are equal.  Also, constant
     *                      values are considered ordered in all positions
     *                      (consider SELECT A FROM T WHERE A = 1 ORDER BY A).
     * @param tableNumber   The table number of the Optimizable containing
     *                      the column in question
     * @param columnNumber  The column number in the table (one-based).
     * @throws StandardException Thrown on error
     * @return true means this ordering is ordered on the given column
     * in the given position.
     */
    boolean orderedOnColumn(int direction,int orderPosition,int tableNumber,int columnNumber) throws StandardException;

    /**
     * Tell whether this ordering is ordered on the given column.
     * This is similar to the method above, but it checks whether the
     * column is ordered in any position, rather than a specified position.
     * This is useful for operations like DISTINCT and GROUP BY.
     *
     * @param direction    One of ASCENDING, DESCENDING, or DONTCARE
     *                     depending on the requirements of the caller.
     *                     An ORDER BY clause cares about direction,
     *                     while DISTINCT and GROUP BY do not.
     * @param tableNumber  The table number of the Optimizable containing
     *                     the column in question
     * @param columnNumber The column number in the table (one-based).
     * @throws StandardException Thrown on error
     * @return true means this ordering is ordered on the given column
     * in the given position.
     */
    boolean orderedOnColumn(int direction, int tableNumber, int columnNumber) throws StandardException;

    int orderedPositionForColumn(int direction, int tableNumber, int columnNumber) throws StandardException;
    /**
     * Add a column to this RowOrdering in the current order position.
     * This is a no-op if there are any unordered optimizables in the
     * join order (see below).
     *
     * @param direction    One of ASCENDING, DESCENDING, or DONTCARE.
     *                     DONTCARE can be used for things like columns
     *                     with constant value, and for one-row tables.
     * @param tableNumber  The table the column is in.
     * @param columnNumber The column number in the table (one-based)
     */
    void addOrderedColumn(int direction, int tableNumber, int columnNumber);

    /**
     * Move to the next order position for adding ordered columns.
     * This is a no-op if there are any unordered optimizables in the
     * join order (see below).
     *
     * @param direction One of ASCENDING, DESCENDING, or DONTCARE.
     *                  DONTCARE can be used for things like columns
     *                  with constant value, and for one-row tables.
     */
    void nextOrderPosition(int direction);

    /**
     * Tell this row ordering that it is no longer ordered on the given
     * table.  Also, adjust the current order position, if necessary.
     * This only works to remove ordered columns from the end of the
     * ordering.
     *
     * @param tableNumber The number of the table to remove from
     *                    this RowOrdering.
     */
    void removeOptimizable(int tableNumber);

    /**
     * Add an unordered optimizable to this RowOrdering.  This is to
     * solve the following problem:
     * <p/>
     * Suppose we have the query:
     * <p/>
     * select * from r, s, t order by r.a, t.b
     * <p/>
     * Also suppose there are indexes on r.a and t.b.  When the
     * optimizer considers the join order (r, s, t) using the index
     * on r.a, the heap on s, and the index on t.b, the rows from the
     * join order will *NOT* be ordered on t.b, because there is an
     * unordered result set between r and t.  So, when s is added to
     * the partial join order, and we then add table t to the join order,
     * we want to ensure that we don't add column t.b to the RowOrdering.
     */
    void addUnorderedOptimizable(Optimizable optimizable);

    /**
     * Copy the contents of this RowOrdering to the given RowOrdering.
     */
    void copy(RowOrdering copyTo);

    RowOrdering getClone();
}
