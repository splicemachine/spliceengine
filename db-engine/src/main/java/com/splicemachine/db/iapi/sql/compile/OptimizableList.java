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

import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;

/**
 * OptimizableList provides services for optimizing a list of
 * Optimizables (tables) in a query.
 */

public interface OptimizableList {

	/**
	 *  Return the number of Optimizables in the list.
	 *
	 *  @return integer		The number of Optimizables in the list.
	 */
	public int size();

	/**
	 *  Return the nth Optimizable in the list.
	 *
	 *  @param n				"index" (0 based) into the list.
	 *
	 *  @return Optimizable		The nth Optimizables in the list.
	 */
	public Optimizable getOptimizable(int n);

	/**
	 * Set the nth Optimizable to the specified Optimizable.
	 *
	 *  @param n				"index" (0 based) into the list.
	 *  @param optimizable		New nth Optimizable.
	 */
	public void setOptimizable(int n, Optimizable optimizable);

	/** 
	 * Verify that the Properties list with optimizer overrides, if specified, is valid
	 *
	 * @param dDictionary	The DataDictionary to use.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void verifyProperties(DataDictionary dDictionary) throws StandardException;

	/**
	 * Set the join order for this list of optimizables.  The join order is
	 * represented as an array of integers - each entry in the array stands
	 * for the order of the corresponding element in the list.  For example,
	 * a joinOrder of {2, 0, 1} means that the 3rd Optimizable in the list
	 * (element 2, since we are zero-based) is the first one in the join
	 * order, followed by the 1st element in the list, and finally by the
	 * 2nd element in the list.
	 *
	 * This method shuffles this OptimizableList to match the join order.
	 *
	 * Obviously, the size of the array must equal the number of elements in
	 * the array, and the values in the array must be between 0 and the
	 * number of elements in the array minus 1, and the values in the array
	 * must be unique.
	 */
	public void reOrder(int[] joinOrder);

	/**
	 * user can specify that s/he doesn't want statistics to be considered when
	 * optimizing the query.
	 */
	public boolean useStatistics();

	/**
	 * Tell whether the join order should be optimized.
	 */
	public boolean optimizeJoinOrder();

	/**
	 * Tell whether the join order is legal.
	 */
	public boolean legalJoinOrder(int numTablesInQuery);

	/**
	 * Init the access paths for these optimizables.
	 *
	 * @param optimizer The optimizer being used.
	 */
	public void initAccessPaths(Optimizer optimizer);
}
