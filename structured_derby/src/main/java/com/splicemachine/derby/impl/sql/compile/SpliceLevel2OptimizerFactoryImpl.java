package com.splicemachine.derby.impl.sql.compile;

import java.util.Properties;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.OptimizableList;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.OptimizerFactory;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.impl.sql.compile.Level2CostEstimateImpl;
import org.apache.derby.impl.sql.compile.OptimizerFactoryImpl;

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
		return new Level2CostEstimateImpl();
	}
}


