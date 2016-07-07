package com.splicemachine.olap;

import com.splicemachine.derby.iapi.sql.olap.AbstractOlapResult;

/**
 * Created by dgomezferro on 3/17/16.
 */
public class FailedOlapResult extends AbstractOlapResult {
    public FailedOlapResult() {
    }

    public FailedOlapResult(Throwable t) {
        this.throwable = t;
    }

    @Override
    public boolean isSuccess(){
        return false;
    }
}
