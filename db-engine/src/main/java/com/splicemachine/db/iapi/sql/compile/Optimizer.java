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
     * move to the next <em>legal</em> join order in the table.
     *
     * Making sense of this call relies on understanding the performance implications
     * of joining multiple tables together. To do this, we start with an example:
     *
     * Suppose we have three tables {@code A},{@code B},and {@code C}. {@code A} has
     * 1 millions rows, {@code B} has 500,000 rows, and {@code C} has 100,000 rows. Now
     * suppose we are performing something like {@code A inner join B inner join C}, where {@code A join B}
     * will output 500K rows, and {@code B join C} will output 50K.
     *
     * We have two choices of the order of operations: {@code A join B join C} or {@code C join B join A}. If
     * we choose the first case, we will first generate an intermediate data set of {@code A join B} = 500K rows,
     * then we will filter that result set down to 50K when we perform the {@code B join C} component.
     *
     * Alternatively, we could perform the {@code C join B} component first, which will generate only 50K intermediate
     * rows. Applying the join with {@code A }afterwards, we generate {@code C join B join A} which will stll output
     * 50K rows. All things being equal, we would expect the second to be much faster (since it won't move as
     * much data around).
     *
     * Of course, all things are not equal--it may be that the access path of one table (index choice) forces
     * or allows different join strategies to be applied. Thus, in order to choose our best overall query
     * plan, we'll need to try different join orders.
     *
     * This method is responsible for generating the next join order. When all join orders are exhausted (or
     * some other consideration is taken into account), this method will return false, which indicates that no
     * more join strategies can be considered.
     *
     * Some Notes:
     *
     * 1. Not all Join permutations are legal (for example, if you are using a column which is generated
     * from the joined output results of something else as a join clause later). Implementations of this method
     * need to ensure that only <em>legal</em> join orders are allowed when called.
     *
     * 2. The method signature necessarily implies state--It is necessary for the underlying implementation
     * to store all relevant information for each permutation of the join order internally, for use within
     * the next level of optimization.
     *
     * @return boolean true  - An optimizable permutation remains.
     *                 false - Permutations are exhausted.
     * @throws StandardException Thrown on error
     */
    boolean nextJoinOrder() throws StandardException;

    /**
     * Iterate through the "decorated permutations", returning false when they
     * are exhausted.
     * NOTE - Implementers are responsible for hiding tree pruning of access
     * methods behind this method call.
     *
     * @return boolean True - An optimizable decorated permutation remains.
     *                 False - Decorated permutations are exhausted.
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

    AccessPath newAccessPath();

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
     *
     * Set Row Ordering on the Outermost Cost Estimate
     *
     * @param rowOrdering
     */
    void setOuterRowOrdering(RowOrdering rowOrdering);


    /**
     *
     * Transfer current outer cost to the optimizers current outer cost.
     *
     */
    void transferOuterCost(CostEstimate currentOuterCostEstimate);

    /**
     *
     * Set on the outer information to mark as an outer join for cost estimates.
     *
     * @param isOuterJoin
     */

    void setIsOuterJoin(boolean isOuterJoin);

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

    /**
     * DB-2877/DB-2001. Sometimes, we can call modifyAccessPaths() and it will explode if there is no best plan. Other
     * times (as with a subselect), we are unable to call modifyAccessPaths() because the inner subselect was unable
     * to find a best plan; the result of this is that we are stuck in an infinite loop and cannot modify our access
     * paths.
     *
     * The intent of this method is to ensure that we can verify a best plan is found at the end of optimization
     * loops, and therefore prevent infinite loops from occurring.
     */
    void verifyBestPlanFound() throws StandardException;
}
