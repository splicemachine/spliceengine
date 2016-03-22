package com.splicemachine.olap;

import com.splicemachine.derby.iapi.sql.olap.OlapResult;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by dgomezferro on 3/16/16.
 */
public abstract class AbstractOlapResult implements OlapResult {
    private short callerId;
    protected Throwable throwable;

    @Override
    public short getCallerId() {
        return callerId;
    }

    @Override
    public void setCallerId(short callerId) {
        this.callerId = callerId;
    }

    @Override
    public Throwable getThrowable() {
        return throwable;
    }
}
