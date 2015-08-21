package com.splicemachine.derby.ddl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.pipeline.ddl.TentativeDDLDesc;

public class ClearStatsCacheDDLDesc implements TentativeDDLDesc {
    private long[] conglomerateIds;
    private String tableName;

    public ClearStatsCacheDDLDesc() {
    }

    public ClearStatsCacheDDLDesc(long[] conglomIds, String tableName) {
        this.conglomerateIds = conglomIds;
        this.tableName = tableName;
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
    
    public String getTableName() {
    	return tableName;
    }
    
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(conglomerateIds);
        out.writeUTF(tableName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.conglomerateIds = (long[])in.readObject();
        this.tableName = (String)in.readUTF();
    }
}
