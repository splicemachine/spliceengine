package com.splicemachine.derby.ddl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.pipeline.ddl.TentativeDDLDesc;

public class ClearStatsCacheDDLDesc implements TentativeDDLDesc {
    private long[] conglomerateIds;

    public ClearStatsCacheDDLDesc() {
    }

    public ClearStatsCacheDDLDesc(long[] conglomIds) {
        this.conglomerateIds = conglomIds;
    }

    @Override
    public long getBaseConglomerateNumber() {
        return -1l;
    }

    @Override
    public long getConglomerateNumber() {
        return -1l;
    }

    public long[] getConglomerateIds() {
        return conglomerateIds;
    }
    
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(conglomerateIds);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.conglomerateIds = (long[])in.readObject();
    }
}
