package com.splicemachine.derby.ddl;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.pipeline.ddl.TentativeDDLDesc;

/**
 * Add column descriptor
 */
public class TentativeAddColumnDesc implements TentativeDDLDesc, Externalizable{
    private UUID tableId;
    private long newConglomId;
    private long oldConglomId;
    private ColumnInfo[] columnInfos;

    public TentativeAddColumnDesc() {}

    public TentativeAddColumnDesc(UUID tableId,
                                  long newConglomId,
                                  long oldConglomId,
                                  ColumnInfo[] columnInfos) {
        this.tableId = tableId;
        this.newConglomId = newConglomId;
        this.oldConglomId = oldConglomId;
        this.columnInfos = columnInfos;
    }

    @Override
    public long getBaseConglomerateNumber() {
        return oldConglomId;
    }

    @Override
    public long getConglomerateNumber() {
        return newConglomId;
    }

    public UUID getTableId() {
        return tableId;
    }

    public ColumnInfo[] getColumnInfos() {
        return columnInfos;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(tableId);
        out.writeLong(newConglomId);
        out.writeLong(oldConglomId);
        out.writeInt(columnInfos.length);
        for (ColumnInfo col:columnInfos) {
            out.writeObject(col);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tableId = (UUID) in.readObject();
        newConglomId = in.readLong();
        oldConglomId = in.readLong();
        int size = in.readInt();
        columnInfos = new ColumnInfo[size];
        for (int i = 0; i < size; ++i) {
            columnInfos[i] = (ColumnInfo)in.readObject();
        }
    }
}
