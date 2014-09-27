package com.splicemachine.derby.ddl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Date: 9/15/14
 */
public class NoTentativeDDLChange implements TentativeDDLDesc {
    public static NoTentativeDDLChange INSTANCE = new NoTentativeDDLChange();
    /*Serialization constructor, don't use*/
    public NoTentativeDDLChange() { }

    @Override
    public long getBaseConglomerateNumber() {
        return -1l;
    }

    @Override
    public long getConglomerateNumber() {
        return -1l;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        //nothing to write
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        //nothing to read
    }
}
