package com.splicemachine.hbase.writer;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class BulkWriteResult implements Externalizable {
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public Map<Integer, WriteResult> getFailedRows() {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    public List<Integer> getNotRunRows() {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }
}
