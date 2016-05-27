package com.splicemachine.olap;

import com.splicemachine.derby.iapi.sql.olap.OlapResult;

/**
 * Created by dgomezferro on 3/16/16.
 */
public abstract class AbstractOlapResult implements OlapResult {
    protected Throwable throwable;

    @Override
    public Throwable getThrowable() {
        return throwable;
    }

    @Override
    public boolean isSuccess(){
        return false;
    }
}
