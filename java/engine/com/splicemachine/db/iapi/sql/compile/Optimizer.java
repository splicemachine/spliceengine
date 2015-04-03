/*

   Derby - Class com.splicemachine.db.iapi.sql.compile.Optimizer

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.store.access.AggregateCostController;
import com.splicemachine.db.iapi.store.access.SortCostController;
import com.splicemachine.db.impl.sql.compile.AggregateNode;
import com.splicemachine.db.impl.sql.compile.GroupByList;
import com.splicemachine.db.impl.sql.compile.OrderByList;

import java.util.List;

/**
 * Optimizer provides services for optimizing a query.
 * RESOLVE:
 * o  Need to figure out what to do about subqueries, figuring out
 * their attachment points and how to communicate them back to the
 * caller.
 */

public interface Optimizer{
    /**
     * Module name for the monitor's module locating system.
     */
    String MODULE="com.splicemachine.db.iapi.sql.compile.Optimizer";

    /**
     * Property name for controlling whether to do join order optimization.
     */
    String JOIN_ORDER_OPTIMIZATION="derby.optimizer.optimizeJoinOrder";

    /**
     * Property name for controlling whether to do rule-based optimization,
     * as opposed to cost-based optimization.
     */
    String RULE_BASED_OPTIMIZATION=
            "derby.optimizer.ruleBasedOptimization";

    /**
     * Property name for controlling whether the optimizer ever times out
     * while optimizing a query and goes with the best plan so far.
     */
    String NO_TIMEOUT="derby.optimizer.noTimeout";

    /**
     * Property name for controlling the maximum size of memory (in KB)
     * the optimizer can use for each table.  If an access path takes
     * memory larger than that size for a table, the access path is skipped.
     * Default is 1024 (KB).
     */
    String MAX_MEMORY_PER_TABLE="derby.language.maxMemoryPerTable";

    /**
     * Maximum size of dynamically created materialized rows. Caching large results
     * use lot of memory and can cause stack overflow. See DERBY-634
     */
    int MAX_DYNAMIC_MATERIALIZED_ROWS=512;

    /**
     * Property name for disabling statistics use for all queries.
     */
    String USE_STATISTICS="derby.language.useStatistics";

    /**
     * Indicates a "normal" plan that is not optimized to do sort avoidance
     */
    int NORMAL_PLAN=1;

    /**
     * Indicates a sort-avoidance plan
     */
    int SORT_AVOIDANCE_PLAN=2;

    /**
     * Iterate through the permutations, returning false when the permutations
     * are exhausted.
     * NOTE - Implementers are responsible for hiding tree pruning of permutations
     * behind this method call.
     *
     * @return boolean    True - An optimizable permutation remains.
     * False - Permutations are exhausted.
     * @throws StandardException Thrown on error
     */
    boolean getNextPermutation() throws StandardException;

    /**
     * Iterate through the "decorated permutations", returning false when they
     * are exhausted.
     * NOTE - Implementers are responsible for hiding tree pruning of access
     * methods behind this method call.
     *
     * @return boolean    True - An optimizable decorated permutation remains.
     * False - Decorated permutations are exhausted.
     * @throws StandardException Thrown on error
     */
    boolean getNextDecoratedPermutation() throws StandardException;

    /**
     * Cost the current permutation.
     * Caller is responsible for pushing all predicates which can be evaluated
     * prior to costing.
     *
     * @throws StandardException Thrown on error
     */
    void costPermutation() throws StandardException;

    /**
     * Cost the current Optimizable with the specified OPL.
     * Caller is responsible for pushing all predicates which can be evaluated
     * prior to costing.
     *
     * @param optimizable The Optimizable
     * @param td          TableDescriptor of the Optimizable
     * @param cd          The ConglomerateDescriptor for the conglom to cost
     *                    (This should change to an object to represent
     *                    access paths, but for now this is OK).
     * @param predList    The OptimizablePredicateList to apply
     * @param outerCost   The cost of the tables outer to the one being
     *                    optimizer - tells how many outer rows there are.
     * @throws StandardException Thrown on error
     */
    void costOptimizable(Optimizable optimizable,
                         TableDescriptor td,
                         ConglomerateDescriptor cd,
                         OptimizablePredicateList predList,
                         CostEstimate outerCost) throws StandardException;

    /**
     * Consider the cost of the given optimizable.  This method is like
     * costOptimizable, above, but it is used when the Optimizable does
     * not need help from the optimizer in costing the Optimizable (in practice,
     * all Optimizables except FromBaseTable use this method.
     * <p/>
     * Caller is responsible for pushing all predicates which can be evaluated
     * prior to costing.
     *
     * @param optimizable   The Optimizable
     * @param predList      The OptimizablePredicateList to apply
     * @param estimatedCost The estimated cost of the given optimizable
     * @param outerCost     The cost of the tables outer to the one being
     *                      optimizer - tells how many outer rows there are.
     * @throws StandardException Thrown on error
     */
    void considerCost(Optimizable optimizable,
                      OptimizablePredicateList predList,
                      CostEstimate estimatedCost,
                      CostEstimate outerCost) throws StandardException;

    /**
     * Return the DataDictionary that the Optimizer is using.
     * This is useful when an Optimizable needs to call optimize() on
     * a child ResultSetNode.
     *
     * @return DataDictionary    DataDictionary that the Optimizer is using.
     */
    DataDictionary getDataDictionary();

    /**
     * Modify the access path for each Optimizable, as necessary.  This includes
     * things like adding result sets to translate from index rows to base rows.
     *
     * @throws StandardException Thrown on error
     */
    void modifyAccessPaths() throws StandardException;

    /**
     * Get a new CostEstimate object
     */
    CostEstimate newCostEstimate();

    AggregateCostController newAggregateCostController(GroupByList groupingList, List<AggregateNode> aggregateVector);

    SortCostController newSortCostController(OrderByList orderByList);

    /**
     * Get the estimated cost of the optimized query
     */
    CostEstimate getOptimizedCost();

    /**
     * Get the final estimated cost of the optimized query.  This
     * should be the cost that corresponds to the best overall join
     * order chosen by the optimizer, and thus this method should
     * only be called after optimization is complete (i.e. when
     * modifying access paths).
     */
    CostEstimate getFinalCost();

    /**
     * Prepare for another round of optimization.
     * <p/>
     * This method is called before every "round" of optimization, where
     * we define a "round" to be the period between the last time a call to
     * getOptimizer() (on either a ResultSetNode or an OptimizerFactory)
     * returned _this_ Optimizer and the time a call to this Optimizer's
     * getNextPermutation() method returns FALSE.  Any re-initialization
     * of state that is required before each round should be done in this
     * method.
     */
    void prepForNextRound();

    /**
     * Set the estimated number of outer rows - good for optimizing nested
     * optimizables like subqueries and join nodes.
     */
    void setOuterRows(double outerRowCount);

    /**
     * Get the number of join strategies supported by this optimizer.
     */
    int getNumberOfJoinStrategies();

    /**
     * Get the maximum number of estimated rows touched in a table before
     * we decide to open the table with table locking (as opposed to row
     * locking.
     */
    int tableLockThreshold();

    /**
     * Gets the names of available join strategies.
     */
    String[] getJoinStrategyNames();

    /**
     * Gets a join strategy by number (zero-based).
     */
    JoinStrategy getJoinStrategy(int whichStrategy);

    /**
     * Gets a join strategy by name.  Returns null if not found.
     * The look-up is case-insensitive.
     */
    JoinStrategy getJoinStrategy(String whichStrategy);

    /**
     * @return an Optimizer tracer for logging information about the optimizer
     */
    OptimizerTrace tracer();

    /**
     * Get the level of this optimizer.
     *
     * @return The level of this optimizer.
     */
    int getLevel();

    /**
     * Tells whether any of the tables outer to the current one
     * has a uniqueness condition on the given predicate list,
     * and if so, how many times each unique key can be seen by
     * the current table.
     *
     * @param predList The predicate list to check
     * @throws StandardException Thrown on error
     * @return    <= 0 means there is no uniqueness condition
     * > 0 means there is a uniqueness condition on an
     * outer table, and the return value is the reciprocal of
     * the maximum number of times the optimizer estimates that each
     * unique key will be returned. For example, 0.5 means the
     * optimizer thinks each distinct join key will be returned
     * at most twice.
     */
    double uniqueJoinWithOuterTable(OptimizablePredicateList predList) throws StandardException;

    /**
     * If statistics should be considered by the optimizer while optimizing
     * a query. The user may disable the use of statistics by setting the
     * property db.optimizer.useStatistics or by using the property
     * useStatistics in a query.
     *
     * @see #USE_STATISTICS
     */
    boolean useStatistics();

    /**
     * @return the maximum number of bytes to be used per table.
     */
    int getMaxMemoryPerTable();

    OptimizableList getOptimizableList();

    void updateBestPlanMaps(short action,Object planKey) throws StandardException;

    void addScopedPredicatesToList(OptimizablePredicateList predList) throws StandardException;
}
