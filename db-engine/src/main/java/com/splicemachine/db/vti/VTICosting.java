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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.vti;

import java.sql.SQLException;

/**
  * <P>
  *	VTICosting is the interface that the query optimizer uses
  * to cost Table Functions. The methods on this interface provide the optimizer
  * with the following information:
  * </P>
  *
  * <UL> 
  * <LI> The estimated number of rows returned by the Table Function in a single instantiation.
  * <LI> The estimated cost to instantiate and iterate through the Table Function.
  * <LI> Whether or not the Table Function can be instantiated multiple times within a single query execution.
  * </UL>
  *
  * <P>
  * The optimizer places a Table Function in the join order after making some
  * assumptions:
  * </P>
  *
  * <UL>
  * <LI><B>Cost</B> - The optimizer hard-codes a guess about how expensive
  * it is to materialize a Table Function.
  * </LI>
  * <LI><B>Count</B> - The optimizer also hard-codes a guess about how
  * many rows a Table Function returns.
  * </LI>
  * <LI><B>Repeatability</B> - The optimizer assumes that the same results
  * come back each time you invoke a Table Function.
  * </LI>
  * </Ul>
  *
  * <P>
  * The class which contains your Table Function can override these assumptions
  * and improve the join order as follows:
  * </P>
  *
  * <UL>
  * <LI><B>Implement</B> - The class must implement <a href="./VTICosting.html">VTICosting</a>.
  * </LI>
  * <LI><B>Construct</B> - The class must contain a public, no-arg constructor.
  * </LI>
  * </Ul>
  *
  * <P>
  * The methods in this interface take a <a href="./VTIEnvironment.html">VTIEnvironment</a>
  * argument. This is a state variable created by the optimizer. The methods in
  * this interface can use this state variable to pass information to one
  * another and learn other details of the operating environment.
  * </P>
  *
  *
  * @see com.splicemachine.db.vti.VTIEnvironment
 */
public interface VTICosting
{
	/**
	 * A useful constant: the default estimated number of rows returned by a
	 * Table Function.
	 */
	double defaultEstimatedRowCount		= 10000d;
	/**
	   A useful constant: The default estimated cost of instantiating and
	   iterating throught a Table Function.
	 */
	double defaultEstimatedCost			= 100000d;

	/**
	 *  Get the estimated row count for a single scan of a Table Function.
	 *
	 *  @param vtiEnvironment The state variable for optimizing the Table Function.
	 *
	 *  @return	The estimated row count for a single scan of the Table Function.
	 *
	 *  @exception SQLException thrown if the costing fails.
	 */
	double getEstimatedRowCount(VTIEnvironment vtiEnvironment)
		throws SQLException;

	/**
	 *  Get the estimated cost for a single instantiation of a Table Function.
	 *
	 *  @param vtiEnvironment The state variable for optimizing the Table Function.
	 *
	 *  @return	The estimated cost for a single instantiation of the Table Function.
	 *
	 *  @exception SQLException thrown if the costing fails.
	 */
	double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment)
		throws SQLException;

	/**
		 Find out if the ResultSet of the Table Function can be instantiated multiple times.

		 @param vtiEnvironment The state variable for optimizing the Table Function.

		 @return	True if the ResultSet can be instantiated multiple times, false if
		 can only be instantiated once.

		 @exception SQLException thrown if the costing fails.
	 */
	boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment)
		throws SQLException;
}
