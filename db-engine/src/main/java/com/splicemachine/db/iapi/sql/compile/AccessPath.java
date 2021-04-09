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
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.compile.FirstColumnOfIndexStats;
import com.splicemachine.db.impl.sql.compile.FromTable;
import com.splicemachine.db.impl.sql.compile.Predicate;
import com.splicemachine.db.impl.sql.compile.ResultSetNode;

/**
 * AccessPath represents a proposed access path for an Optimizable.
 * An Optimizable may have more than one proposed AccessPath.
 */

public interface AccessPath {
	/**
	 * Set the conglomerate descriptor for this access path.
	 *
	 * @param cd	A ConglomerateDescriptor
	 */
	void setConglomerateDescriptor(ConglomerateDescriptor cd);

	/**
	 * Get whatever was last set as the conglomerate descriptor.
	 * Returns null if nothing was set since the last call to startOptimizing()
	 */
	ConglomerateDescriptor getConglomerateDescriptor();

	/**
	 * Set the given cost estimate in this AccessPath.  Generally, this will
	 * be the CostEstimate for the plan currently under consideration.
	 */
	void setCostEstimate(CostEstimate costEstimate);

	/**
	 * Get the cost estimate for this AccessPath.  This is the last one
	 * set by setCostEstimate.
	 */
	CostEstimate getCostEstimate();

	/**
	 * Set whether or not to consider a covering index scan on the optimizable.
	 */
	void setCoveringIndexScan(boolean coveringIndexScan);

	/**
	 * Return whether or not the optimizer is considering a covering index
	 * scan on this AccessPath. 
	 *
	 * @return boolean Whether or not the optimizer chose a covering index scan.
	 */
	boolean getCoveringIndexScan();

	/**
	 * Set whether or not to consider a non-matching index scan on this AccessPath.
	 */
	void setNonMatchingIndexScan(boolean nonMatchingIndexScan);

	/**
	 * Return whether or not the optimizer is considering a non-matching
	 * index scan on this AccessPath. We expect to call this during
	 * generation, after access path selection is complete.
	 *
	 * @return boolean		Whether or not the optimizer is considering
	 *						a non-matching index scan.
	 */
	boolean getNonMatchingIndexScan();

	/**
	 * Remember the given join strategy
	 *
	 * @param joinStrategy	The best join strategy
	 */
	void setJoinStrategy(JoinStrategy joinStrategy);

	/**
	 * Get the join strategy, as set by setJoinStrategy().
	 */
	JoinStrategy getJoinStrategy();

	/**
	 * Set the lock mode
	 */
	void setLockMode(int lockMode);

	/**
	 * Get the lock mode, as last set in setLockMode().
	 */
	int getLockMode();

	/**
	 * Copy all information from the given AccessPath to this one.
	 */
	void copy(AccessPath copyFrom);

	/**
	 * Get the optimizer associated with this access path.
	 *
	 * @return	The optimizer associated with this access path.
	 */
	Optimizer getOptimizer();
	
	/**
	 * Sets the "name" of the access path. if the access path represents an
	 * index then set the name to the name of the index. if it is an index
	 * created for a constraint, use the constraint name. This is called only
	 * for base tables.
	 * 
	 * @param 	td		TableDescriptor of the base table.
	 * @param 	dd		Datadictionary.
	 *
	 * @exception StandardException 	on error.
	 */
	void initializeAccessPathName(DataDictionary dd,TableDescriptor td) throws StandardException;

	/**
	 * Inform the access path that the join strategy was chosen by the user through the use of hints. This
	 * way the individual join strategies can be informed that that user believes the join strategy to be feasible,
	 * and not to check physical restrictions (such as memory size limits etc.)
	 *
	 * @param isHintedJoinStrategy {@code true} if the join strategy was hinted, {@code false} otherwise.
	 */
	void setHintedJoinStrategy(boolean isHintedJoinStrategy);

	/**
	 * @return {@code true} if the join strategy in this access path was selected by the user through hints,
	 * {@code false} otherwise
	 */
	boolean isHintedJoinStrategy();

	/**
	 * Check whether memory usage is under the system limit in the presence of consecutive joins
	 * @param memoryAlreadyConsumed the memory consumed before the join specified in the current access path
	 */
	boolean isJoinPathMemoryUsageUnderLimit(double memoryAlreadyConsumed);

	/**
	 * @return {@code true} if the join strategy in this access path is hashable and is allowed to have a missing hash key,
	 * {@code false} otherwise
	 */
	boolean isMissingHashKeyOK();

	/**
	 * Mark in the access path that the join strategy chosen is hashable, but there are no equijoin conditions
	 * to use for generating a hash key.
	 *
	 * @param missingHashKeyOK {@code true} if the join strategy was is hashable with no hash key, {@code false} otherwise.
	 */
	void setMissingHashKeyOK(boolean missingHashKeyOK);

	boolean getSpecialMaxScan();
	void setSpecialMaxScan(boolean value);


	/**
	 * Check whether the current access path uses a primary key or index to scan
	 * a subset of the rows, but one or more leading index fields has no predicate
	 * specified to generate a start key.  If non-zero, the access path represents
	 * an IndexPrefixIteratorMode scan, where the values in the first column of the
	 * index are iterated through in separate scans, and the remainder predicates
	 * are used to complete the start key.
	 */
	int getNumUnusedLeadingIndexFields();

	/**
	 * If non-zero, this access path represents an IndexPrefixIteratorMode scan.
	 */
    void setNumUnusedLeadingIndexFields(int numUnusedLeadingIndexFields);

    FirstColumnOfIndexStats getFirstColumnStats();

	/**
	 * Store in the access path the predicate used to enable unioned index scans.
	 */
    void setUisPredicate(Predicate uisPredicate);

	/**
	 * The predicate used to enable unioned index scans, if any.
	 */
    Predicate getUisPredicate();

	/**
	 * Store in the access path the RowId predicate used to join back
	 * to the base table for unioned index scan query plans.
	 */
    void setUisRowIdPredicate(Predicate uisPredicate);

	/**
	 * The RowId predicate used to join back
	 * to the base table for unioned index scan query plans.
	 */
    Predicate getUisRowIdPredicate();

	/**
	 * The tree of UnionNodes that combines results of the OR'ed index scans,
	 * for a unioned index scans access path.
	 */
    FromTable getUnionOfIndexes();

	/**
	 * Store in the access path the tree of UnionNodes that combines results
	 * of OR'ed index scans for a unioned index scans access path.
	 */
    void setUnionOfIndexes(FromTable unionOfIndexes);

	/**
	 * The entire optimized result set tree of the unioned index scans
	 * plus the rowid join back to the base table to get the full rows.
	 */
    ResultSetNode getUisRowIdJoinBackToBaseTableResultSet();

	/**
	 * Store in the access path the entire optimized result set tree of
	 * the unioned index scans plus the rowid join back to the base table
	 * to get the full rows.
	 */
    void setUisRowIdJoinBackToBaseTableResultSet(ResultSetNode uisRowIdJoinBackToBaseTableResultSet);
}
