/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.compile;

import java.util.Properties;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.JoinStrategy;
import com.splicemachine.db.iapi.sql.compile.OptimizableList;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicateList;
import com.splicemachine.db.iapi.sql.compile.Optimizer;
import com.splicemachine.db.iapi.sql.compile.OptimizerFactory;
import com.splicemachine.db.iapi.sql.compile.RequiredRowOrdering;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.impl.sql.compile.OptimizerFactoryImpl;

public class SpliceLevel2OptimizerFactoryImpl extends OptimizerFactoryImpl {

	public void boot(boolean create, Properties startParams) throws StandardException {
		super.boot(create, startParams);
	}


	/**
	 * @see OptimizerFactory#supportsOptimizerTrace
	 */
	public boolean supportsOptimizerTrace() {
		return true;
	}

	public SpliceLevel2OptimizerFactoryImpl()  {
		
	}
	@Override
	public Optimizer getOptimizer(OptimizableList optimizableList,
			  OptimizablePredicateList predList,
			  DataDictionary dDictionary,
			  RequiredRowOrdering requiredRowOrdering,
			  int numTablesInQuery,
			  LanguageConnectionContext lcc) throws StandardException {
	/* Get/set up the array of join strategies.
	* See comment in boot().  If joinStrategySet
	* is null, then we may do needless allocations
	* in a multi-user environment if multiple
	* users find it null on entry.  However, 
	* assignment of array is atomic, so system
	* will be consistent even in rare case
	* where users get different arrays.
	*/
		if (joinStrategySet == null) { // Do not change order...
			joinStrategySet = new JoinStrategy[]{
					new NestedLoopJoinStrategy(),
					new MergeSortJoinStrategy(),
					new BroadcastJoinStrategy(),
					new MergeJoinStrategy(),
//					new HalfMergeSortJoinStrategy(),
			};
		}

return getOptimizerImpl(optimizableList,
		predList,
		dDictionary,
		requiredRowOrdering,
		numTablesInQuery,
		lcc);
}


	protected Optimizer getOptimizerImpl(
							  OptimizableList optimizableList,
							  OptimizablePredicateList predList,
							  DataDictionary dDictionary,
							  RequiredRowOrdering requiredRowOrdering,
							  int numTablesInQuery,
							  LanguageConnectionContext lcc) throws StandardException {

	return new SpliceLevel2OptimizerImpl(
						optimizableList,
						predList,
						dDictionary,
						ruleBasedOptimization,
						noTimeout,
						useStatistics,
						maxMemoryPerTable,
						joinStrategySet,
						lcc.getLockEscalationThreshold(),
						requiredRowOrdering,
						numTablesInQuery,
						lcc);
}

	/**
	 * @see OptimizerFactory#getCostEstimate
	 *
	 * @exception StandardException		Thrown on error
	 */
	public CostEstimate getCostEstimate() throws StandardException {
		return new SimpleCostEstimate();
	}
}


