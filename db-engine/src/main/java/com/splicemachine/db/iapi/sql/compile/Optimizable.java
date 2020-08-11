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
import com.splicemachine.db.iapi.util.JBitSet;
import java.util.Properties;

/**
 * Optimizable provides services for optimizing a table in a query.
 */

public interface Optimizable {

    /**
     * Choose the next access path to evaluate for this Optimizable.
     *
     * @param optimizer    Optimizer to use.
     * @param predList    The predicate list for this optimizable.
     *                    The optimizer always passes null, and it is up
     *                    to the optimizable object to pass along its
     *                    own predicate list, if appropriate, when delegating
     *                    this method.
     * @param rowOrdering    The row ordering for all the outer tables in
     *                        the join order.  This method will add the ordering
     *                        of the next access path to the given RowOrdering.
     *
     * @return    true means another access path was chosen, false means
     *            no more access paths to evaluate.
     *
     * @exception StandardException        Thrown on error
     */
    boolean nextAccessPath(Optimizer optimizer,
                            OptimizablePredicateList predList,
                            RowOrdering rowOrdering) throws StandardException;

    /**
     * Choose the best access path for this Optimizable.
     *
     * @param optimizer     Optimizer to use.
     * @param predList      The predicate list to optimize against
     * @param outerCost     The CostEstimate for the outer tables in the join order,
     *                      telling how many times this Optimizable will be scanned.
     * @param rowOrdering   The row ordering for all the tables in the
     *                      join order, including this one.
     *
     * @return The optimizer's estimated cost of the best access path.
     *
     * @exception StandardException        Thrown on error
     */
    CostEstimate optimizeIt(Optimizer optimizer,
                            OptimizablePredicateList predList,
                            CostEstimate outerCost,
                            RowOrdering rowOrdering) throws StandardException;

    /**
     * Get the current access path under consideration for this Optimizable
     */
    AccessPath getCurrentAccessPath();

    /**
     * Get the best access path for this Optimizable.
     */
    AccessPath getBestAccessPath();

    /**
     * Get the best sort-avoidance path for this Optimizable.
     */
    AccessPath getBestSortAvoidancePath();

    /**
     * Get the best access path overall for this Optimizable.
     */
    AccessPath getTrulyTheBestAccessPath();

    /**
     * Mark this optimizable so that its sort avoidance path will be
     * considered.
     */
    void rememberSortAvoidancePath();

    /**
     * Check whether this optimizable's sort avoidance path should
     * be considered.
     */
    boolean considerSortAvoidancePath();

    /**
     * Remember the current join strategy as the best one so far in this
     * join order.
     */
    void rememberJoinStrategyAsBest(AccessPath ap);

    /**
     * Get the table descriptor for this table (if any).  Only base tables
     * have table descriptors - for the rest of the optimizables, this
     * method returns null.
     */
    TableDescriptor getTableDescriptor();

    /**
     * Get the map of referenced tables for this Optimizable.
     *
     * @return JBitSet    Referenced table map.
     */
    JBitSet getReferencedTableMap();

    /**
     * Push an OptimizablePredicate down, if this node accepts it.
     *
     * @param optimizablePredicate    OptimizablePredicate to push down.
     *
     * @return Whether or not the predicate was pushed down.
     *
     * @exception StandardException        Thrown on error
     */
    boolean pushOptPredicate(OptimizablePredicate optimizablePredicate) throws StandardException;

    /**
     * Pull all the OptimizablePredicates from this Optimizable and put them
     * in the given OptimizablePredicateList.
     *
     * @param optimizablePredicates        The list to put the pulled predicates
     *                                    in.
     *
     * @exception StandardException        Thrown on error
     */
    void pullOptPredicates(OptimizablePredicateList optimizablePredicates) throws StandardException;

    void pullOptPostJoinPredicates(OptimizablePredicateList optimizablePredicates) throws StandardException;

    /**
     * Modify the access path for this Optimizable, as necessary.  This includes
     * things like adding a result set to translate from index rows to base rows
     *
     * @param outerTables    Bit map of the tables that are outer to this one
     *                        in the join order.
     *
     * @return    The (potentially new) Optimizable at the top of the tree.
     *
     * @exception StandardException        Thrown on error
     */
    Optimizable modifyAccessPath(JBitSet outerTables) throws StandardException;

    /**
     * Return whether or not this is a covering index.  We expect to call this
     * during generation, after access path selection is complete.
     *
     * @param cd            ConglomerateDesriptor for index to consider
     *
     * @return boolean        Whether or not this is a covering index.
     *
     * @exception StandardException        Thrown on error
     */
    boolean isCoveringIndex(ConglomerateDescriptor cd) throws StandardException;

    /**
     * Get the Properties list, if any, associated with this optimizable.
     *
     * @return The Properties list, if any, associated with this optimizable.
     */
    Properties getProperties();

    /**
     * Set the Properties list for this optimizalbe.
     *
     * @param tableProperties The Properties list for this optimizable.
     */
    void setProperties(Properties tableProperties);

    /**
     * Verify that the Properties list with optimizer overrides, if specified, is valid
     *
     * @param dDictionary    The DataDictionary  to use.
     *
     * @exception StandardException        Thrown on error
     */
    void verifyProperties(DataDictionary dDictionary) throws StandardException;

    /**
     * Get the (exposed) name of this Optimizable
     *
     * @return    The name of this Optimizable.
     * @exception StandardException        Thrown on error
     */
    String getName() throws StandardException;

    /**
     * Get the table name of this Optimizable.  Only base tables have
     * table names (by the time we use this method, all views will have
     * been resolved).
     */
    String getBaseTableName();

    /**
     *  Convert an absolute to a relative 0-based column position.
     *  This is useful when generating qualifiers for partial rows
     *  from the store.
     *
     * @param absolutePosition    The absolute 0-based column position for the column.
     *
     *  @return The relative 0-based column position for the column.
     */
    int convertAbsoluteToRelativeColumnPosition(int absolutePosition);

    /**
     * When remembering "truly the best" access path for an Optimizable, we
     * have to keep track of which OptimizerImpl the "truly the best" access
     * is for.  In most queries there will only be one OptimizerImpl in
     * question, but in cases where there are nested subqueries, there will be
     * one OptimizerImpl for every level of nesting, and each OptimizerImpl
     * might have its own idea of what this Optimizable's "truly the best path"
     * access path really is.  In addition, there could be Optimizables
     * above this Optimizable that might need to override the best path
     * chosen during optimization.  So whenever we save a "truly the best" path,
     * we take note of which Optimizer/Optimizable told us to do so.  Then
     * as each level of subquery finishes optimization, the corresponding
     * OptimizerImpl/Optimizable can load its preferred access path into this
     * Optimizable's trulyTheBestAccessPath field and pass it up the tree, until
     * eventually the outer-most OptimizerImpl can choose to either use the best
     * path that it received from below (by calling "rememberAsBest()") or else
     * use the path that it found to be "best" for itself.
     *
     * This method is what allows us to keep track of which OptimizerImpl or
     * Optimizable saved which "best plan", and allows us to load the
     * appropriate plans after each round of optimization.
     *
     * @param action Indicates whether we're adding, loading, or removing
     *  a best plan for the OptimizerImpl/Optimizable.
     * @param planKey Object to use as the map key when adding/looking up
     *  a plan.  If it is an instance of OptimizerImpl then it corresponds
     *  to an outer query; otherwise it's some Optimizable above this
     *  Optimizable that could potentially reject plans chosen by the
     *  OptimizerImpl to which this Optimizable belongs.
     */
    void updateBestPlanMap(short action,Object planKey) throws StandardException;

    /**
     * Remember the current access path as the best one (so far).
     *
     * @param planType    The type of plan (one of Optimizer.NORMAL_PLAN
     *                    or Optimizer.SORT_AVOIDANCE_PLAN)
     * @param optimizer The OptimizerImpl that is telling this Optimizable
     *    to remember its current path as "truly the best".
     *
     * @exception StandardException thrown on error.
     */
    void rememberAsBest(int planType,Optimizer optimizer) throws StandardException;

    boolean bestPathPicksSortMergeJoin(int planType);

    /**
     * Begin the optimization process for this Optimizable.  This can be
     * called many times for an Optimizable while optimizing a query -
     * it will typically be called every time the Optimizable is placed
     * in a potential join order.
     */
    void startOptimizing(Optimizer optimizer,RowOrdering rowOrdering);

    /**
     * Estimate the cost of scanning this Optimizable using the given
     * predicate list with the given conglomerate.  It is assumed that the
     * predicate list has already been classified.  This cost estimate is
     * just for one scan, not for the life of the query.
     *
     * @see OptimizablePredicateList#classify
     *
     * @param    predList           The predicate list to optimize against
     * @param    cd                 The conglomerate descriptor to get the cost of
     * @param    outerCost          The estimated cost of the part of the plan outer
     *                              to this optimizable.
     * @param    optimizer          The optimizer to use to help estimate the cost
     * @param    rowOrdering        The row ordering for all the tables in the
     *                              join order, including this one.
     *
     * @return    The estimated cost of doing the scan
     *
     * @exception StandardException        Thrown on error
     */
    CostEstimate estimateCost(OptimizablePredicateList predList,
                                ConglomerateDescriptor cd,
                                CostEstimate outerCost,
                                Optimizer optimizer,
                                RowOrdering rowOrdering)
                    throws StandardException;

    /** Tell whether this Optimizable represents a base table */
    boolean isBaseTable();

    /** Tell whether this Optimizable is materializable
     *
     * @exception StandardException        Thrown on error
     */
    boolean isMaterializable() throws StandardException;

    /** Tell whether this Optimizable can be instantiated multiple times */
    boolean supportsMultipleInstantiations();

    /** Tell whether this Optimizable has any large object (LOB) columns. */
    boolean hasLargeObjectColumns();

    /** Get this Optimizable's result set number */
    int getResultSetNumber();

    /** Get this Optimizable's table number */
    int getTableNumber();

    /** Return true if this Optimizable has a table number */
    boolean hasTableNumber();

    /** Return true if this is the target table of an update */
    boolean forUpdate();

    /** Return the hash key column numbers, for hash join strategy */
    int[] hashKeyColumns();

    /** Set the hash key column numbers, for hash join strategy */
    void setHashKeyColumns(int[] columnNumbers);

    /**
     * Is the current proposed join strategy for this optimizable feasible
     * given the predicate list?
     *
     * @param predList    The predicate list that has been pushed down to
     *                    this optimizable
     * @param optimizer    The optimizer to use.
     *
     * @param outerCost the cost of the outer table scan
     * @return    true means feasible
     *
     * @exception StandardException        Thrown on error
     */
    boolean feasibleJoinStrategy(OptimizablePredicateList predList,
                                 Optimizer optimizer,
                                 CostEstimate outerCost) throws StandardException;


    /**
     * Can this Optimizable appear at the current location in the join order.
     * In other words, have the Optimizable's dependencies been satisfied?
     *
     * @param assignedTableMap    The tables that have been placed so far in the join order.
     *
     * @return    Where or not this Optimizable can appear at the current location in the join order.
     */
    boolean legalJoinOrder(JBitSet assignedTableMap);

    /**
     * Get the DataDictionary from this Optimizable.  This is useful for code generation
     * because we need to get the constraint name if scanning a back index so that
     * RunTimeStatistics can display the correct info.
     *
     * @return The DataDictionary to use.
     *
     * @exception StandardException        Thrown on error
     */
    DataDictionary getDataDictionary() throws StandardException;

    /**
     * Is the optimizable the target table of an update or delete?
     *
     * @return Whether or not the optimizable the target table of an update or delete.
     */
    boolean isTargetTable();

    /**
     * Get the number of the number of columns returned by this Optimizable.
     *
     * @return The number of the number of columns returned by this Optimizable.
     */
    int getNumColumnsReturned();

    /**
     * Will the optimizable return at most 1 row per scan?
     *
     * @return Whether or not the optimizable will return at most 1 row per scan?
     *
     * @exception StandardException        Thrown on error
     */
    boolean isOneRowScan() throws StandardException;

    /**
     * Init the access paths for this optimizable.
     *
     * @param optimizer The optimizer being used.
     */
    void initAccessPaths(Optimizer optimizer);

    /**
     * Reset access paths
     */
    void resetAccessPaths();

    /**
     * Does this optimizable have a uniqueness condition on the
     * given predicate list, and if so, how many unique keys will be
     * returned per scan.
     *
     * @param predList        The predicate list to check
     *
     * @return    <= 0 means there is no uniqueness condition
     *            > 0 means there is a uniqueness condition,
     *            and the return value is the number of rows per scan.
     *
     * @exception StandardException        Thrown on error
     */
    double uniqueJoin(OptimizablePredicateList predList) throws StandardException;

    /**
     * get the current optimizable's memory usage if its best join plan is a broadcast join
     */
    double getMemoryUsage4BroadcastJoin();

    void setAccumulatedCost(CostEstimate cost);
    void setAccumulatedCostForSortAvoidancePlan(CostEstimate cost);
    CostEstimate getAccumulatedCost();
    CostEstimate getAccumulatedCostforSortAvoidancePlan();

    default boolean hasJoinPredicatePushedDownFromOuter() {
        return false;
    }

    default void setHasJoinPredicatePushedDownFromOuter(boolean value) {
    }
}
