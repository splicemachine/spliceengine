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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.util.JBitSet;

/**
 * This interface provides a representation of the required ordering of rows
 * from a ResultSet.  Different operations can require ordering: ORDER BY,
 * DISTINCT, GROUP BY.  Some operations, like ORDER BY, require that the
 * columns be ordered a particular way, while others, like DISTINCT and
 * GROUP BY, reuire only that there be no duplicates in the result.
 */
public interface RequiredRowOrdering
{
	int SORT_REQUIRED = 1;
	int ELIMINATE_DUPS = 2;
	int NOTHING_REQUIRED = 3;

	/**
	 * Tell whether sorting is required for this RequiredRowOrdering,
	 * given a RowOrdering.
	 *
	 * @param rowOrdering	The order of rows in question
	 * @param optimizableList	The current join order being considered by 
	 *    the optimizer. We need to look into this to determine if the outer
	 *    optimizables are single row resultset if the order by column is
	 *    on an inner optimizable and that inner optimizable is not a one
	 *    row resultset. DERBY-3926
	 *
	 * @return	SORT_REQUIRED if sorting is required,
	 *			ELIMINATE_DUPS if no sorting is required but duplicates
	 *							must be eliminated (i.e. the rows are in
	 *							the right order but there may be duplicates),
	 *			NOTHING_REQUIRED is no operation is required
	 *
	 * @exception StandardException		Thrown on error
	 */
	int sortRequired(RowOrdering rowOrdering, OptimizableList optimizableList)  throws StandardException;

	/**
	 * Tell whether sorting is required for this RequiredRowOrdering,
	 * given a RowOrdering representing a partial join order, and
	 * a bit map telling what tables are represented in the join order.
	 * This is useful for reducing the number of cases the optimizer
	 * has to consider.
	 *
	 * @param rowOrdering	The order of rows in the partial join order
	 * @param tableMap		A bit map of the tables in the partial join order
	 * @param optimizableList	The current join order being considered by 
	 *    the optimizer. We need to look into this to determine if the outer
	 *    optimizables are single row resultset if the order by column is
	 *    on an inner optimizable and that inner optimizable is not a one
	 *    row resultset. DERBY-3926
	 *
	 * @return	SORT_REQUIRED if sorting is required,
	 *			ELIMINATE_DUPS if no sorting is required by duplicates
	 *							must be eliminated (i.e. the rows are in
	 *							the right order but there may be duplicates),
	 *			NOTHING_REQUIRED is no operation is required
	 *
	 * @exception StandardException		Thrown on error
	 */
	int sortRequired(RowOrdering rowOrdering,JBitSet tableMap,OptimizableList optimizableList) throws StandardException;

	/**
	 * Estimate the cost of doing a sort for this row ordering, given
	 * the number of rows to be sorted.  This does not take into account
	 * whether the sort is really needed.  It also estimates the number of
	 * result rows.
	 *
	 * @param rowOrdering			The ordering of the input rows
	 *
	 * @exception StandardException		Thrown on error
	 */
	void estimateCost(Optimizer optimizer, RowOrdering rowOrdering, CostEstimate baseCost, CostEstimate sortCost) throws StandardException;

	/**
	 * Indicate that a sort is necessary to fulfill this required ordering.
	 * This method may be called many times during a single optimization.
	 */
	void sortNeeded();

	/**
	 * Indicate that a sort is *NOT* necessary to fulfill this required
	 * ordering.  This method may be called many times during a single
	 * optimization.
	 */
	void sortNotNeeded();

	/**
	 * @return Whether or not a sort is needed.
	 */
	boolean getSortNeeded();
}
