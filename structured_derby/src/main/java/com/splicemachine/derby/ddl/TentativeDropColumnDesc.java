package com.splicemachine.derby.ddl;

import org.apache.derby.impl.sql.execute.ColumnInfo;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.derby.catalog.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 2/10/14
 * Time: 10:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class TentativeDropColumnDesc implements TentativeDDLDesc, Externalizable{
    private UUID tableId;
    private long conglomerateNumber;
    private long baseConglomerateNumber;
    private int droppedColumnPosition;
    private ColumnInfo[] columnInfos;

    public TentativeDropColumnDesc() {}

    public TentativeDropColumnDesc(UUID tableId,
                                   long conglomerateNumber,
                                   long baseConglomerateNumber,
                                   ColumnInfo[] columnInfos,
                                   int droppedColumnPosition) {
        this.tableId = tableId;
        this.conglomerateNumber = conglomerateNumber;
        this.baseConglomerateNumber = baseConglomerateNumber;
        this.columnInfos = columnInfos;
        this.droppedColumnPosition = droppedColumnPosition;

    }

    @Override
    public long getBaseConglomerateNumber() {
        return baseConglomerateNumber;
    }

    @Override
    public long getConglomerateNumber() {
        return conglomerateNumber;
    }

    public UUID getTableId() {
        return tableId;
    }

    public ColumnInfo[] getColumnInfos() {
        return columnInfos;
    }

    public int getDroppedColumnPosition() {
        return droppedColumnPosition;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(tableId);
        out.writeLong(conglomerateNumber);
        out.writeLong(baseConglomerateNumber);
        int size = columnInfos.length;
        out.writeInt(size);
        for (ColumnInfo col:columnInfos) {
            out.writeObject(col);
        }
        out.writeInt(droppedColumnPosition);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tableId = (UUID) in.readObject();
        conglomerateNumber = in.readLong();
        baseConglomerateNumber = in.readLong();
        int size = in.readInt();
        columnInfos = new ColumnInfo[size];
        for (int i = 0; i < size; ++i) {
            columnInfos[i] = (ColumnInfo)in.readObject();
        }
        droppedColumnPosition = in.readInt();
    }
}
