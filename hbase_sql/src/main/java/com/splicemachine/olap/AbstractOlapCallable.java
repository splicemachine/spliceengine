package com.splicemachine.olap;

import com.splicemachine.derby.iapi.sql.olap.OlapCallable;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by dgomezferro on 3/16/16.
 */
public abstract class AbstractOlapCallable<R extends OlapResult> implements OlapCallable<R> {
    private short callerId;

    @Override
    public void setCallerId(short callerId) {
        this.callerId = callerId;
    }

    @Override
    public short getCallerId() {
        return callerId;
    }
}
