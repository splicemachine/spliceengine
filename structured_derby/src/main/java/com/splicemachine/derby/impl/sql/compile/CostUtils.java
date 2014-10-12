package com.splicemachine.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.Optimizer;

public class CostUtils {
	public static boolean isThisBaseTable(Optimizer optimizer) {
		SpliceLevel2OptimizerImpl opt = (SpliceLevel2OptimizerImpl) optimizer;
		return (opt.joinPosition == 0 && opt.outermostCostEstimate.getEstimatedCost() == 0.0 && opt.outermostCostEstimate.getEstimatedRowCount() == 1)?true:false;
	}	
}
