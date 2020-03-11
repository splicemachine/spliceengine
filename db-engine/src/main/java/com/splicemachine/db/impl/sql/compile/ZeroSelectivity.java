package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * Created by yxia on 3/11/20.
 */
public class ZeroSelectivity extends AbstractSelectivityHolder {
    public ZeroSelectivity(int colNum, QualifierPhase phase) {
        super(colNum, phase);
        selectivity = 0.0;
    }

    public double getSelectivity()  throws StandardException {
        return selectivity;
    }
    public boolean isRangeSelectivity() {
        return false;
    }
}
