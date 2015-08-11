package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * Created by jleach on 8/8/15.
 */
public class NotEqualsSelectivity extends AbstractSelectivityHolder {
    private DataValueDescriptor value;
    private StoreCostController storeCost;
    public NotEqualsSelectivity(StoreCostController storeCost, int colNum, QualifierPhase phase, DataValueDescriptor value){
        super(colNum,phase);
        this.value = value;
        this.storeCost = storeCost;
    }

    public double getSelectivity() throws StandardException {
        if (selectivity == -1.0d)
            selectivity = 1-storeCost.getSelectivity(colNum,value,true,value,false);
        return selectivity;
    }
}
