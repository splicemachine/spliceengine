package com.splicemachine.db.iapi.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;

/**
 * @author Scott Fines
 *         Date: 3/25/15
 */
public interface AggregateCostController{

    CostEstimate estimateAggregateCost(CostEstimate baseCost) throws StandardException;
}
