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

package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;

import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;

import com.splicemachine.db.iapi.error.StandardException;

/**
	This is simply the factory for creating an optimizer.
	<p>
	There is expected to be only one of these configured per database.
 */

public interface OptimizerFactory {
	/**
		Module name for the monitor's module locating system.
	 */
	String MODULE = "com.splicemachine.db.iapi.sql.compile.OptimizerFactory";

	/**
	 * Only one optimizer level should exist in the database, however, the
	 * connection may have multiple instances of that optimizer
	 * at a given time.
	 *
	 * @param optimizableList	The list of Optimizables to optimize.
	 * @param predicateList	The list of unassigned OptimizablePredicates.
	 * @param dDictionary	The DataDictionary to use.
	 * @param requiredRowOrdering	The required ordering of the rows to
	 *								come out of the optimized result set
	 * @param numTablesInQuery	The number of tables in the current query
	 * @param lcc			The LanguageConnectionContext
	 *
	 * RESOLVE - We probably want to pass a subquery list, once we define a
	 * new interface for them, so that the Optimizer can out where to attach
	 * the subqueries.
	 *
	 * @exception StandardException		Thrown on error
	 */
	Optimizer getOptimizer(OptimizableList optimizableList,
						   OptimizablePredicateList predicateList,
						   DataDictionary dDictionary,
						   RequiredRowOrdering requiredRowOrdering,
						   int numTablesInQuery,
						   LanguageConnectionContext lcc)
			throws StandardException;


	/**
	 * Return a new CostEstimate.
	 *
	 * @exception StandardException		Thrown on error
	 */
	CostEstimate getCostEstimate()
		throws StandardException;

	/**
	 * Return whether or not the optimizer associated with
	 * this factory supports optimizer trace.
	 *
	 * @return Whether or not the optimizer associated with
	 * this factory supports optimizer trace.
	 */
	boolean supportsOptimizerTrace();

	/**
	 * Return the maxMemoryPerTable setting, this is used in
	 * optimizer, as well as subquery materialization at run time.
	 *
	 * @return	maxMemoryPerTable value
	 */
	int getMaxMemoryPerTable();

	long getDetermineSparkRowThreshold();
}
