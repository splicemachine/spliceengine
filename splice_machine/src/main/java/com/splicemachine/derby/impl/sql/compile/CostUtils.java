package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.sql.compile.Optimizer;

import static com.google.common.base.Preconditions.checkArgument;

public class CostUtils {

    public static boolean isThisBaseTable(Optimizer optimizer) {
        SpliceLevel2OptimizerImpl opt = (SpliceLevel2OptimizerImpl) optimizer;
        boolean outermostCostEstimateZero = (Math.abs(opt.outermostCostEstimate.getEstimatedCost() - 0d) < 1e-5);
        return (opt.joinPosition == 0 && outermostCostEstimateZero && opt.outermostCostEstimate.getEstimatedRowCount() == 1);
    }

    /**
     * In cost calculations we don't want to overflow and a have large positive row count turn into a negative count.
     * This method assumes that for our purposes any count greater than Long.MAX_VALUE can be approximated as
     * Long.MAX_VALUE.We don't have the same consideration for cost as double overflow results in a value of
     * Double.POSITIVE_INFINITY which behaves correctly (to my knowledge) in our cost calculations.
     */
    public static long add(long count1, long count2) {
        checkArgument(count1 >= 0);
        checkArgument(count2 >= 0);
        long sum = count1 + count2;
        return (sum >= 0L) ? sum : Long.MAX_VALUE;
    }

}
