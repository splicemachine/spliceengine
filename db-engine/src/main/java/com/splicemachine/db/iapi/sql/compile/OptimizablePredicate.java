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

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.impl.sql.compile.SelectivityUtil;

/**
 * OptimizablePredicate provides services for optimizing predicates in a query.
 */

public interface OptimizablePredicate
{
	/**
	 * Get the map of referenced tables for this OptimizablePredicate.
	 *
	 * @return JBitSet	Referenced table map.
	 */
	JBitSet getReferencedMap();

	/**
	 * Return whether or not an OptimizablePredicate contains a subquery.
	 *
	 * @return boolean	Whether or not an OptimizablePredicate includes a subquery.
	 */
	boolean hasSubquery();

	/**
	 * Return whether or not an OptimizablePredicate contains a method call.
	 *
	 * @return boolean	Whether or not an OptimizablePredicate includes a method call.
	 */
	boolean hasMethodCall();

	/**
	 * Tell the predicate that it is to be used as a column in the start key
	 * value for an index scan.
	 */
	void markStartKey();

	/** Is this predicate a start key? */
	boolean isStartKey();

	/**
	 * Tell the predicate that it is to be used as a column in the stop key
	 * value for an index scan.
	 */
	void markStopKey();

	/** Is this predicate a stop key? */
	boolean isStopKey();

	/**
	 * Tell the predicate that it is to be used as a qualifier in an index
	 * scan.
	 */
	void markQualifier();

	/** Is this predicate a qualifier? */
	boolean isQualifier();

	/**
	 * Is this predicate a comparison with a known constant value?
	 *
	 * @param optTable	The Optimizable that we want to know whether we
	 *					are comparing to a known constant.
	 * @param considerParameters	Whether or not to consider parameters with defaults
	 *								as known constants.
	 */
	boolean compareWithKnownConstant(Optimizable optTable, boolean considerParameters);

	/**
	 * Get an Object representing the known constant value that the given
	 * Optimizable is being compared to.
	 *
	 * @exception StandardException		Thrown on error
	 */
	DataValueDescriptor getCompareValue(Optimizable optTable) 
        throws StandardException;

	/**
	 * Is this predicate an equality comparison with a constant expression?
	 * (IS NULL is considered to be an = comparison with a constant expression).
	 *
	 * @param optTable	The Optimizable for which we want to know whether
	 *					it is being equality-compared to a constant expression.
	 */
	boolean equalsComparisonWithConstantExpression(Optimizable optTable);

	
	/**
	 * Returns if the predicate involves an equal operator on one of the
	 * columns specified in the baseColumnPositions.
	 *
	 * @param 	baseColumnPositions	the column numbers on which the user wants
	 * to check if the equality condition exists.
	 * @param 	optTable the table for which baseColumnPositions are given.

		@return returns the index into baseColumnPositions of the column that has the
		equality operator.
	 */
	int hasEqualOnColumnList(int[] baseColumnPositions,
								 Optimizable optTable)
		throws StandardException;

	/**
	 * Get a (crude) estimate of the selectivity of this predicate.
	 * This is to be used when no better technique is available for
	 * estimating the selectivity - this method's estimate is a hard-
	 * wired number based on the type of predicate and the datatype
	 * (the selectivity of boolean is always 50%).
	 *
	 * @param optTable	The Optimizable that this predicate restricts
	 */
	double selectivity(Optimizable optTable) throws StandardException;

    /**
     *
     * Join Selectivity calculation for an optimizable predicate.
     *
     * @param table
     * @param cd
     * @param innerRowCount
     * @param outerRowCount
     * @param selectivityJoinType
     * @return
     * @throws StandardException
     */
	double joinSelectivity(Optimizable table,ConglomerateDescriptor cd, long innerRowCount, long outerRowCount, SelectivityUtil.SelectivityJoinType selectivityJoinType) throws StandardException;

	double scanSelectivity(Optimizable innerTable) throws StandardException;
	/**
	 * Get the position of the index column that this predicate restricts.
	 * NOTE: This assumes that this predicate is part of an
	 * OptimizablePredicateList, and that classify() has been called
	 * on the OptimizablePredicateList.
	 *
	 * @return The index position that this predicate restricts (zero-based)
	 */
	int getIndexPosition();

    boolean isRowId();

	void markFullJoinPredicate(boolean isForFullJoin);

	boolean isFullJoinPredicate();
}
