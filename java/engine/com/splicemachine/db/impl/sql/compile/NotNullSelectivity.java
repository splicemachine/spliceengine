package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.StoreCostController;

/**
 * Created by jleach on 8/8/15.
 */
public class NotNullSelectivity extends AbstractSelectivityHolder {
    private StoreCostController storeCost;
    public NotNullSelectivity(StoreCostController storeCost, int colNum, QualifierPhase phase){
        super(colNum,phase);
        this.storeCost = storeCost;
    }

    public double getSelectivity() throws StandardException {
        if (selectivity == -1.0d)
            selectivity = 1.0d-storeCost.nullSelectivity(colNum);
        return selectivity;
    }
}
