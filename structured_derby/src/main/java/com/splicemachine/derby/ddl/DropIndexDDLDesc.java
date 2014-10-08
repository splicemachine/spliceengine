package com.splicemachine.derby.ddl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Date: 10/7/14
 */
public class DropIndexDDLDesc implements TentativeDDLDesc{
    private long conglomerateId;
    private long baseConglomerateId;

    public DropIndexDDLDesc() {
    }

    public DropIndexDDLDesc(long indexConglomId, long tableConglomId) {

        this.conglomerateId = indexConglomId;
        this.baseConglomerateId = tableConglomId;
    }

    @Override public long getBaseConglomerateNumber() { return conglomerateId; }
    @Override public long getConglomerateNumber() { return baseConglomerateId; }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(conglomerateId);
        out.writeLong(baseConglomerateId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.conglomerateId = in.readLong();
        this.baseConglomerateId = in.readLong();
    }
}
