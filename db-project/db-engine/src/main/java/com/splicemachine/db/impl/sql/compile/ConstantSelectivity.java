package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;

/**
 *
 * Constant class used for testing purposes only currently.  This can always be added when you have a fixed selectivity.
 *
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

