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

import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;

import com.splicemachine.db.iapi.store.access.TransactionController;

import com.splicemachine.db.iapi.services.compiler.MethodBuilder;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * A JoinStrategy represents a strategy like nested loop, hash join,
 * merge join, etc.  It tells the optimizer whether the strategy is
 * feasible in a given situation, how much the strategy costs, whether
 * the strategy requires the data from the source result sets to be ordered,
 * etc.
 */

public interface JoinStrategy {

    enum JoinStrategyType {
        NESTED_LOOP ("NESTED_LOOP",0,"NestedLoop",true),
		MERGE_SORT ("MERGE_SORT",1,"MergeSort",false),
        BROADCAST ("BROADCAST",2,"Broadcast",false),
        MERGE ("MERGE",3, "Merge",false),
		HALF_MERGE_SORT ("HALF_MERGE_SORT", 4, "HalfMergeSort", false),
        CROSS ("CROSS", 5, "Cross", false);
        private final String name;
        private final int strategyId;
        private final String niceName;
        private final boolean allowsJoinPredicatePushdown;
        JoinStrategyType(String name, int strategyId, String niceName, boolean allowsJoinPredicatePushdown) {
            this.name = name;
            this.strategyId = strategyId;
            this.niceName = niceName;
            this.allowsJoinPredicatePushdown = allowsJoinPredicatePushdown;
        }
        public String getName() { return name;}
        public int getStrategyId() { return strategyId;}
        public String niceName() { return niceName;}
        public boolean isAllowsJoinPredicatePushdown() {
            return allowsJoinPredicatePushdown;
        }
    }

	/**
	 * Is this join strategy feasible under the circumstances?
	 *
	 * @param innerTable    The inner table of the join
	 * @param predList        The predicateList for the join
	 * @param optimizer        The optimizer to use
	 * @param outerCost the cost to scan the outer table
	 * @param wasHinted {@code true} if the join strategy was chosen by the user as a hint, {@code false} otherwise
     * @param skipKeyCheck {@code true} if any checks for equality join conditions on which a hash key can be built should be bypassed, {@code false} otherwise
	 * @return	true means the strategy is feasible, false means it isn't
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean feasible(Optimizable innerTable,
					 OptimizablePredicateList predList,
					 Optimizer optimizer,
					 CostEstimate outerCost,
					 boolean wasHinted,
                     boolean skipKeyCheck) throws StandardException;

	/**
	 * Is it OK to use bulk fetch with this join strategy?
	 */
	boolean bulkFetchOK();

	/**
	 * Should we just ignore bulk fetch with this join strategy?
	 */
	boolean ignoreBulkFetch();

	/**
	 * Returns true if the base cost of scanning the conglomerate should be
	 * multiplied by the number of outer rows.
	 */
	boolean multiplyBaseCostByOuterRows();

	/**
	 * Get the base predicates for this join strategy.  The base predicates
	 * are the ones that can be used while scanning the table.  For some
	 * join strategies (for example, nested loop), all predicates are base
	 * predicates.  For other join strategies (for example, hash join),
	 * the base predicates are those that involve comparisons with constant
	 * expressions.
	 *
	 * Also, order the base predicates according to the order in the
	 * proposed conglomerate descriptor for the inner table.
	 *
	 * @param predList	The predicate list to pull from.
	 * @param basePredicates	The list to put the base predicates in.
	 * @param innerTable	The inner table of the join
	 *
	 * @return	The base predicate list.  If no predicates are pulled,
	 *			it may return the source predList without doing anything.
	 *
	 * @exception StandardException		Thrown on error
	 */
	OptimizablePredicateList getBasePredicates(
								OptimizablePredicateList predList,
								OptimizablePredicateList basePredicates,
								Optimizable innerTable) throws StandardException;

	/**
	 * Get the extra selectivity of the non-base predicates (those that were
	 * left in the predicate list by getBasePredicates() that are not
	 * applied to the scan of the base conglomerate.
	 *
	 * NOTE: For some types of join strategy, it may not remove any predicates
	 * from the original predicate list.  The join strategy is expected to
	 * know when it does this, and to return 1.0 as the extra selectivity
	 * in these cases.
	 *
	 * @param innerTable	The inner table of the join.
	 * @param predList	The original predicate list that was passed to
	 *					getBasePredicates(), from which some base predicates
	 *					may have been pulled.
	 *
	 * @return	The extra selectivity due to non-base predicates
	 */
	double nonBasePredicateSelectivity(Optimizable innerTable,
									   OptimizablePredicateList predList) throws StandardException;

	/**
	 * Put back and base predicates that were removed from the list by
	 * getBasePredicates (see above).
	 *
	 * NOTE: Those join strategies that treat all predicates as base
	 *		 predicates may treat the get and put methods as no-ops.
	 *
	 * @param predList	The list of predicates to put the base predicates
	 *					back in.
	 * @param basePredicates	The base predicates to put back in the list.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void putBasePredicates(OptimizablePredicateList predList,
							OptimizablePredicateList basePredicates)
					throws StandardException;
	/**
	 * Get the estimated cost for the join.
	 *
	 * @param predList		The predicate list for the join
	 * @param innerTable	The inner table to join with
	 * @param cd			The conglomerate descriptor (if appropriate) to get
	 *						the cost of
	 * @param outerCost		The estimated cost of the part of the plan outer
	 *						to the inner table
	 * @param optimizer		The optimizer to use to help estimate the cost
	 * @param costEstimate	The estimated cost of doing a single scan of the
	 *						inner table, to be filled in with the cost of
	 *						doing the join.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void estimateCost(Optimizable innerTable,
						OptimizablePredicateList predList,
						ConglomerateDescriptor cd,
						CostEstimate outerCost,
						Optimizer optimizer,
						CostEstimate costEstimate)
		throws StandardException;

	/*
	 * Get the output sort order of the join strategy. In most cases, it will inherit the join
	 * order of the outer table, but in some cases it might be different, and we'll want to be careful.
	 */
//	void setOutputSortOrder(Optimizable innerTable,
//								   OptimizablePredicateList predicateList,
//								   CostEstimate outerCost,
//								   Optimizer optimizer,
//								   CostEstimate innerCost);
    /**
     * @param userSpecifiedCapacity the capacity as specified by the user (bytes per table)
     * @param maxMemoryPerTable maximum number of bytes per table
     * @param perRowUsage number of bytes per row
     *
     * @return The maximum number of rows that can be handled by this join strategy
     */
	int maxCapacity(int userSpecifiedCapacity,
					int maxMemoryPerTable,
					double perRowUsage);
    
	/** Get the name of this join strategy */
	String getName();

	/**
	 * Get the name of the result set method for base table scans
	 *
	 * @param multiprobe True means we are probing the inner table for rows
	 *  matching a specified list of values.
	 */
	String resultSetMethodName(boolean multiprobe);

	/**
	 * Get the name of the join result set method for the join
	 */
	String joinResultSetMethodName();

	/**
	 * Get the name of the join result set method for the half outerjoin
	 */
	String halfOuterJoinResultSetMethodName();

	/**
	 * Get the appropriate arguments to the scan for this type of join.
	 *
	 * @param tc	The TransactionController
	 * @param mb	The method to generate the arguments in
	 * @param innerTable	The inner table of the join
	 * @param storeRestrictionList	The predicate list to be evaluated in the
	 *								store
	 * @param nonStoreRestrictionList	The predicate list to be evaluated
	 *									outside of the store
	 * @param acb	The expression class builder for the activation class
	 *				we're building
	 * @param bulkFetch	The amount of bulk fetch to do
	 * @param resultRowAllocator	A completed method to allocate the result row
	 * @param colRefItem	The item number of the column reference bit map
	 * @param lockMode		The lock mode to use when scanning the table
	 *						(see TransactionController).
	 * @param tableLocked	Whether or not the table is marked (in sys.systables)
	 *						as always using table locking
	 * @param isolationLevel		Isolation level specified (or not) for scans
	 * @param maxMemoryPerTable	Max memory per table
	 * @param genInListVals Whether or not we are going to generate IN-list
	 *  values with which to probe the inner table.
	 *
	 * @return	Count of the expressions pushed to use as the parameters to the
	 *			result set for the inner table
	 *
	 * @exception StandardException		Thrown on error
	 */
	int getScanArgs(TransactionController tc,
							MethodBuilder mb,
							Optimizable innerTable,
							OptimizablePredicateList storeRestrictionList,
							OptimizablePredicateList nonStoreRestrictionList,
							ExpressionClassBuilderInterface acb,
							int bulkFetch,
							MethodBuilder resultRowAllocator,
							int colRefItem,
							int indexColItem,
							int lockMode,
							boolean tableLocked,
							int isolationLevel,
							int maxMemoryPerTable,
							boolean genInListVals,
                            String tableVersion,
							boolean pin,
							int splits,
							String delimited,
							String escaped,
							String lines,
							String storedAs,
							String location,
							int partitionRefItem)
					throws StandardException;

	/**
	 * Divide up the predicates into different lists for different phases
	 * of the operation. When this method is called, all of the predicates
	 * will be in restrictionList.  The effect of this method is to
	 * remove all of the predicates from restrictionList except those that
	 * will be pushed down to the store as start/stop predicates or
	 * Qualifiers.  The remaining predicates will be put into
	 * nonBaseTableRestrictionList.
	 *
	 * All predicate lists will be ordered as necessary for use with
	 * the conglomerate.
	 *
	 * Some operations (like hash join) materialize results, and so
	 * require requalification of rows when doing a non-covering index
	 * scan.  The predicates to use for requalification are copied into
	 * baseTableRestrictionList.
	 *
	 * @param innerTable	The inner table of the join
	 * @param originalRestrictionList	Initially contains all predicates.
	 *									This method removes predicates from
	 *									this list and moves them to other
	 *									lists, as appropriate.
	 * @param storeRestrictionList	To be filled in with predicates to
	 *								be pushed down to store.
	 * @param nonStoreRestrictionList	To be filled in with predicates
	 *									that are not pushed down to the
	 *									store.
	 * @param requalificationRestrictionList	Copy of predicates used to
	 *											re-qualify rows, if necessary.
	 * @param dd			The DataDictionary
	 *
	 * @exception StandardException		Thrown on error
	 */
	void divideUpPredicateLists(
						Optimizable innerTable,
						OptimizablePredicateList originalRestrictionList,
						OptimizablePredicateList storeRestrictionList,
						OptimizablePredicateList nonStoreRestrictionList,
						OptimizablePredicateList requalificationRestrictionList,
						DataDictionary			 dd)
				throws StandardException;

	/**
	 * Is this a form of hash join?
	 *
	 * @return Whether or not this strategy is a form of hash join.
	 */
	boolean isHashJoin();

	/**
	 * Is materialization built in to the join strategy?
	 *
	 * @return Whether or not materialization is built in to the join strategy
	 */
	boolean doesMaterialization();

    /**
     *
     * Defers to the join strategy type to determine if the join Predicates can be pushed down.
     *
     * @return
     */
    boolean allowsJoinPredicatePushdown();

	/**
	 * Sets the row ordering on the cost estimate. The row ordering which is set is the
	 * <em>output</em> ordering after the join.
	 *
	 * @param costEstimate the cost estimate to set the ordering on.
	 */
	void setRowOrdering(CostEstimate costEstimate);
    /**
     *
     * Grab the JoinStrategyType to allow for non reflection based testing of types.
     *
     * @return
     */
	JoinStrategyType getJoinStrategyType();

	/**
	 * Check whether memory usage is under the system limit in the presence of consecutive joins, specifically,
	 * consecutive broadcast joins.
	 * @param totalMemoryConsumed the memory consumed by consecutive joins in bytes
	 */
	boolean isMemoryUsageUnderLimit(double totalMemoryConsumed);

}
