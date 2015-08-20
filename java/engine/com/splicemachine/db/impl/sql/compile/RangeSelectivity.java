package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 *
 * Range Selectivity Holder, allows one to modify the start or stop based on steps in the optimizer.
 *
 */
public class RangeSelectivity extends AbstractSelectivityHolder {
    public DataValueDescriptor start;
    public boolean includeStart;
    public DataValueDescriptor stop;
    public boolean includeStop;
    public StoreCostController storeCost;

    public RangeSelectivity(StoreCostController storeCost,DataValueDescriptor start, DataValueDescriptor stop,boolean includeStart, boolean includeStop,
                            int colNum, QualifierPhase phase){
        super(colNum,phase);
        this.start = start;
        this.stop = stop;
        this.includeStart = includeStart;
        this.includeStop = includeStop;
        this.storeCost = storeCost;
    }

    public double getSelectivity()  throws StandardException {
        if (selectivity==-1.0d)
            selectivity = storeCost.getSelectivity(colNum,start,includeStart,stop,includeStop);
        return selectivity;
    }
    public boolean isRangeSelectivity() {
        return true;
    }
}

