package com.splicemachine.olap;

import com.splicemachine.derby.iapi.sql.olap.OlapResult;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by dgomezferro on 3/16/16.
 */
public abstract class AbstractOlapResult implements OlapResult, Externalizable {
    private short callerId;

    @Override
    public short getCallerId() {
        return callerId;
    }

    @Override
    public void setCallerId(short callerId) {
        this.callerId = callerId;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeShort(callerId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.callerId = in.readShort();
    }
}
