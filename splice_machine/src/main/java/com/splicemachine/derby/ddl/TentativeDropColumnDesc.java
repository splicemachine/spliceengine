package com.splicemachine.derby.ddl;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.derby.impl.sql.execute.altertable.DropColumnRowTransformer;
import com.splicemachine.pipeline.api.RowTransformer;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.ddl.TransformingDDLDescriptor;
import com.splicemachine.pipeline.writehandler.altertable.AlterTableInterceptWriteHandler;

/**
 * Drop column.
 */
public class TentativeDropColumnDesc implements TransformingDDLDescriptor, Externalizable{
    private long conglomerateNumber;
    private long baseConglomerateNumber;
    private String tableVersion;
    private int[] oldColumnOrdering;
    private int[] newColumnOrdering;
    private ColumnInfo[] columnInfos;
    private int droppedColumnPosition;

    public TentativeDropColumnDesc() {}

    public TentativeDropColumnDesc(long baseConglomerateNumber,
                                   long conglomerateNumber,
                                   String tableVersion,
                                   int[] oldColumnOrdering,
                                   int[] newColumnOrdering,
                                   ColumnInfo[] columnInfos,
                                   int droppedColumnPosition) {
        this.conglomerateNumber = conglomerateNumber;
        this.baseConglomerateNumber = baseConglomerateNumber;
        this.tableVersion = tableVersion;
        this.oldColumnOrdering = oldColumnOrdering;
        this.newColumnOrdering = newColumnOrdering;
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

    @Override
    public RowTransformer createRowTransformer() throws IOException {
        return DropColumnRowTransformer.create(tableVersion,
                                               oldColumnOrdering, newColumnOrdering,
                                               columnInfos, droppedColumnPosition);
    }

    @Override
    public WriteHandler createWriteHandler(RowTransformer transformer) throws IOException {
        return new AlterTableInterceptWriteHandler(transformer, Bytes.toBytes(String.valueOf(conglomerateNumber)));
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(conglomerateNumber);
        out.writeLong(baseConglomerateNumber);
        out.writeInt(droppedColumnPosition);
        out.writeObject(tableVersion);
        ArrayUtil.writeIntArray(out, oldColumnOrdering);
        ArrayUtil.writeIntArray(out, newColumnOrdering);
        int size = columnInfos.length;
        out.writeInt(size);
        for (ColumnInfo col:columnInfos) {
            out.writeObject(col);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        conglomerateNumber = in.readLong();
        baseConglomerateNumber = in.readLong();
        droppedColumnPosition = in.readInt();
        tableVersion = (String) in.readObject();
        oldColumnOrdering = ArrayUtil.readIntArray(in);
        newColumnOrdering = ArrayUtil.readIntArray(in);
        int size = in.readInt();
        columnInfos = new ColumnInfo[size];
        for (int i = 0; i < size; ++i) {
            columnInfos[i] = (ColumnInfo)in.readObject();
        }
    }
}
