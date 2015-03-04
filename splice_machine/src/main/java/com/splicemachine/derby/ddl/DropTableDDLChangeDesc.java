package com.splicemachine.derby.ddl;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.pipeline.ddl.TentativeDDLDesc;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class DropTableDDLChangeDesc implements TentativeDDLDesc{

    private long conglomerateNumber;
    private UUID tableId;

    public DropTableDDLChangeDesc() {
    }

    /**
     * Provide the conglomerateNumber of the table being dropped.
     */
    public DropTableDDLChangeDesc(long conglomerateNumber, UUID tableId) {
        this.conglomerateNumber = conglomerateNumber;
        this.tableId = tableId;
    }

    @Override
    public long getBaseConglomerateNumber() {
        return 0L;
    }

    @Override
    public long getConglomerateNumber() {
        return conglomerateNumber;
    }

    public UUID getTableId() {
        return tableId;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(conglomerateNumber);
        out.writeObject(tableId);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        conglomerateNumber = in.readLong();
        tableId = (UUID)in.readObject();

    }

    @Override
    public String toString() {
        return "DropTableDDLChangeDesc{" +
                ", conglomerateNumber=" + conglomerateNumber +
                ", tableId=" + tableId +
                '}';
    }
}
