package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * Created by jleach on 8/8/15.
 */
public class ConstantSelectivity extends AbstractSelectivityHolder {
    public ConstantSelectivity(double selectivity, int colNum, QualifierPhase phase){
        super(colNum,phase);
        this.selectivity = selectivity;
    }

    public double getSelectivity() throws StandardException {
        return selectivity;
    }
}

