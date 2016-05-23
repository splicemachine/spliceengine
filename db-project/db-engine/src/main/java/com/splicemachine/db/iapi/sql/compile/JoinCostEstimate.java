package com.splicemachine.db.iapi.sql.compile;

/**
 * @author Scott Fines
 *         Date: 5/22/15
 */
public interface JoinCostEstimate extends CostEstimate{

    CostEstimate leftCost();

    CostEstimate rightCost();
}
