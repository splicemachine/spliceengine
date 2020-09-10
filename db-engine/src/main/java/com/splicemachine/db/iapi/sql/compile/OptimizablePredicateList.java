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

import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;

import com.splicemachine.db.iapi.services.compiler.MethodBuilder;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.util.JBitSet;

/**
 * OptimizablePredicateList provides services for optimizing a table in a query.
 * RESOLVE - the methods for this interface need to get defined.
 */

public interface OptimizablePredicateList {
	/**
	 *  Return the number of OptimizablePredicates in the list.
	 *
	 *  @return integer		The number of OptimizablePredicates in the list.
	 */
	int size();

	/**
	 *  Return the nth OptimizablePredicate in the list.
	 *
	 *  @param n				"index" (0 based) into the list.
	 *
	 *  @return OptimizablePredicate		The nth OptimizablePredicate in the list.
	 */
	OptimizablePredicate getOptPredicate(int n);

	/**
	 * Remove the OptimizablePredicate at the specified index (0-based) from the list.
	 *
	 * @param predCtr	The index.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void removeOptPredicate(int predCtr) throws StandardException;

	/**
	 * Add the given OptimizablePredicate to the end of this list.
	 *
	 * @param optPredicate	The predicate to add
	 */
	void addOptPredicate(OptimizablePredicate optPredicate);

	/**
	 * Return true if this predicate list is useful for limiting the scan on
	 * the given table using the given conglomerate.
	 *
	 * @param optTable An Optimizable for the table in question
	 * @param cd A ConglomerateDescriptor for the conglomerate in question
	 *
	 * @return	true if this predicate list can limit the scan
	 * @exception StandardException		Thrown on error
	 */
	boolean useful(Optimizable optTable, ConglomerateDescriptor cd) throws StandardException;

	/**
	 * Determine which predicates in this list are useful for limiting
	 * the scan on the given table using its best conglomerate.  Remove
	 * those predicates from this list and push them down to the given
	 * Optimizable table.  The predicates are pushed down in the order of
	 * the index columns that they qualify.  Also, the predicates are
	 * "marked" as start predicates, stop predicates, or qualifier
	 * predicates.  Finally, the start and stop operators are set in
	 * the given Optimizable.
	 *
	 * @param optTable	An Optimizable for the table in question
	 *
	 * @exception StandardException		Thrown on error
	 */
	void pushUsefulPredicates(Optimizable optTable) throws StandardException;

	/**
	 * Classify the predicates in this list according to the given
	 * table and conglomerate.  Each predicate can be a start key, stop key,
	 * and/or qualifier, or it can be none of the above.  This method
	 * also orders the predicates to match the order of the columns
	 * in a keyed conglomerate.  No ordering is done for heaps.
	 *
	 * @param optTable	The Optimizable table for which to classify
	 *					the predicates in this list.
	 * @param cd	The ConglomerateDescriptor for which to classify
	 *				the predicates in this list.
	 * @param considerJoinPredicateAsKey For nestedloop join, it should be set to true, as join predicate can
	 *              serve as key for the scan of the right table, for hashable join, it should be set to false
	 *
	 * @exception StandardException		Thrown on error
	 */
	void classify(Optimizable optTable, ConglomerateDescriptor cd, boolean considerJoinPredicateAsKey) throws StandardException;

	/**
	 * Mark all of the predicates as Qualifiers and set the numberOfQualifiers
	 * to reflect this.  This is useful for hash joins where all of the
	 * predicates in the list to be evaluated during the probe into the
	 * hash table on a next are qualifiers.
	 */
	void markAllPredicatesQualifiers();
	
	/**
	 * Check into the predicate list if the passed column has an equijoin 
	 * predicate on it.
	 * 
	 * @param optTable
	 * @param columnNumber
	 * @param isNullOkay
	 * @return the position of the predicate in the list which corresponds to 
	 *   the equijoin. If no quijoin predicate found, then the return value 
	 *   will be -1
	 * @throws StandardException
	 */
	int hasEqualityPredicateOnOrderedColumn(Optimizable optTable,int columnNumber,boolean isNullOkay) throws StandardException;

	/**
	 * Is there an optimizable equality predicate on the specified column?
	 *
	 * @param optTable		The optimizable the column comes from.
	 * @param columnNumber	The column number within the base table.
	 * @param isNullOkay	boolean, whether or not the IS NULL operator
	 *						satisfies the search
	 *
	 * @return Whether or not there is an optimizable equality predicate on the specified column.
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean hasOptimizableEqualityPredicate(Optimizable optTable,
											int columnNumber,
											boolean isNullOkay) throws StandardException;

	OptimizablePredicate getOptimizableEqualityPredicate(Optimizable optTable,
											int columnNumber,
											boolean isNullOkay) throws StandardException;

	/**
	 * Is there an optimizable equijoin on the specified column?
	 *
	 * @param optTable		The optimizable the column comes from.
	 * @param columnNumber	The column number within the base table.
	 *
	 * @return Whether or not there is an optimizable equijoin on the specified column.
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean hasOptimizableEquijoin(Optimizable optTable, int columnNumber) throws StandardException;
									

	/**
	 * Find the optimizable equality predicate on the specified column and make
	 * it the first predicate in this list.  This is useful for hash joins where
	 * Qualifier[0] is assumed to be on the hash key.
	 *
	 * @param optTable		The optimizable the column comes from.
	 * @param columnNumber	The column number within the base table.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void putOptimizableEqualityPredicateFirst(Optimizable optTable, int columnNumber) throws StandardException;

	/**
	 * Transfer the predicates whose referenced set is contained by the
	 * specified referencedTableMap from this list to the other list.
	 * This is useful when splitting out a set of predicates from a larger
	 * set, like when generating a HashScanResultSet.
	 *
	 * @param otherList				The predicateList to xfer to
	 * @param referencedTableMap	The table map to check against
	 * @param table					The table to order the new predicates
	 *								against
	 *
	 * @exception StandardException		Thrown on error
	 */
	void transferPredicates(OptimizablePredicateList otherList,
							JBitSet referencedTableMap,
							Optimizable table,
							JBitSet joinedTableSet) throws StandardException;


	/**
	 * Transfer all the predicates from this list to the given list.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void transferAllPredicates(OptimizablePredicateList otherList) throws StandardException;

	/**
	 * Non-destructive copy of all of the predicates from this list to the
	 * other list.
	 *
	 * This is useful when splitting out a set of predicates from a larger
	 * set, like when generating a HashScanResultSet.
	 *
	 * @param otherList				The predicateList to xfer to
	 *
	 * @exception StandardException		Thrown on error
	 */
	void copyPredicatesToOtherList(OptimizablePredicateList otherList) throws StandardException;

	/**
	 * Sets the given list to have the same elements as this one, and
	 * the same properties as this one (number of qualifiers and start
	 * and stop predicates.
	 *
	 * @param otherList		The list to set the same as this one.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void setPredicatesAndProperties(OptimizablePredicateList otherList) throws StandardException;

	/**
	 * Return whether or not the specified entry in the list is a redundant
	 * predicate. This is useful for selectivity calculations because we
	 * do not want redundant predicates included in the selectivity calculation.
	 *
	 * @param predNum	The entry in the list
	 *
	 * @return Whether or not the specified entry in the list is a redundant predicate.
	 */
	boolean isRedundantPredicate(int predNum);

	/**
	 * Get the start operator for the given Optimizable for a heap or
	 * index scan.
	 */
	int startOperator(Optimizable optTable);

	/**
	 * Get the stop operator for the given Optimizable for a heap or
	 * index scan.
	 */
	int stopOperator(Optimizable optTable);

	/**
	 * Generate the qualifiers for a scan.  This method generates an array
	 * of Qualifiers, and fills them in with calls to the factory method
	 * for generating Qualifiers in the constructor for the activation.
	 * It stores the array of Qualifiers in a field in the activation, and
	 * returns a reference to that field.
	 *
	 * If there are no qualifiers, it initializes the array of Qualifiers
	 * to null.
	 *
	 * @param acb	The ExpressionClassBuilderInterface for the class we are building
	 * @param mb	The method the generated code is going into
	 * @param optTable	The Optimizable table the Qualifiers are on
	 * @param absolute	Generate absolute column positions if true,
	 *					else relative column positions (within the underlying
	 *					row)
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	void generateQualifiers(ExpressionClassBuilderInterface acb,
							MethodBuilder mb,
							Optimizable optTable,
							boolean absolute) throws StandardException;

	/**
	 * Generate the start key for a heap or index scan.
	 *
	 * @param acb	The ExpressionClassBuilderInterface for the class we're building
	 * @param mb	The method the generated code is to go into
	 * @param optTable	The Optimizable table the start key is for
	 *
	 * @exception StandardException		Thrown on error
	 */
	void generateStartKey(ExpressionClassBuilderInterface acb,
						  MethodBuilder mb,
						  Optimizable optTable) throws StandardException;

	/**
	 * Generate the stop key for a heap or index scan.
	 *
	 * @param acb	The ExpressionClassBuilderInterface for the class we're building
	 * @param mb	the method the generated code is to go into
	 * @param optTable	The Optimizable table the stop key is for
	 *
	 * @exception StandardException		Thrown on error
	 */
	void generateStopKey(ExpressionClassBuilderInterface acb,
								MethodBuilder mb,
								Optimizable optTable)
				throws StandardException;

	/**
	 * Can we use the same key for both the start and stop key.
	 * This is possible when doing an exact match on an index
	 * where there are no other sargable predicates.
	 *
	 * @return Whether or not we can use the same key for both the start and stop key.
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean sameStartStopPosition() throws StandardException;

	/**
	 * Walk through the predicates in this list and make any adjustments
	 * that are required to allow for proper handling of an ORDER BY
	 * clause.
	 */
	void adjustForSortElimination(RequiredRowOrdering ordering) throws StandardException;

    boolean isRowIdScan();

	/**
	 *
	 * Evaluates whether the index conglomerate can be scanned by the predicates.
	 *
	 * @param cd
	 * @return
	 * @throws StandardException
     */
	boolean canSupportIndexExcludedNulls(int tableNumber, ConglomerateDescriptor cd, TableDescriptor td) throws StandardException;

	/**
	 *
	 * Evaluates whether the index conglomerate can be scanned by the predicates.
	 *
	 * @param cd
	 * @return
	 * @throws StandardException
	 */
	boolean canSupportIndexExcludedDefaults(int tableNumber, ConglomerateDescriptor cd, TableDescriptor td) throws StandardException;

}
