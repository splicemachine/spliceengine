package com.splicemachine.derby.ddl;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.derby.impl.sql.execute.altertable.AddColumnRowTransformer;
import com.splicemachine.pipeline.api.RowTransformer;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.ddl.TentativeDDLDesc;
import com.splicemachine.pipeline.ddl.TransformingDDLDescriptor;
import com.splicemachine.pipeline.writehandler.altertable.AlterTableInterceptWriteHandler;

/**
 * Add column descriptor
 */
public class TentativeAddColumnDesc implements TransformingDDLDescriptor, Externalizable{
    private String tableVersion;
    private long newConglomId;
    private long oldConglomId;
    private int[] columnOrdering;
    private ColumnInfo[] columnInfos;

    public TentativeAddColumnDesc() {}

    public TentativeAddColumnDesc(String tableVersion,
                                  long newConglomId,
                                  long oldConglomId,
                                  int[] columnOrdering,
                                  ColumnInfo[] columnInfos) {
        this.tableVersion = tableVersion;
        this.newConglomId = newConglomId;
        this.oldConglomId = oldConglomId;
        this.columnOrdering = columnOrdering;
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

    @Override
    public RowTransformer createRowTransformer() throws IOException {
        return AddColumnRowTransformer.create(tableVersion, columnOrdering, columnInfos);
    }

    @Override
    public WriteHandler createWriteHandler(RowTransformer transformer) throws IOException {
        return new AlterTableInterceptWriteHandler(transformer, Bytes.toBytes(String.valueOf(newConglomId)));

    }

    public ColumnInfo[] getColumnInfos() {
        return columnInfos;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(tableVersion);
        out.writeLong(newConglomId);
        out.writeLong(oldConglomId);
        ArrayUtil.writeIntArray(out, columnOrdering);
        out.writeInt(columnInfos.length);
        for (ColumnInfo col:columnInfos) {
            out.writeObject(col);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tableVersion = (String) in.readObject();
        newConglomId = in.readLong();
        oldConglomId = in.readLong();
        columnOrdering = ArrayUtil.readIntArray(in);
        int size = in.readInt();
        columnInfos = new ColumnInfo[size];
        for (int i = 0; i < size; ++i) {
            columnInfos[i] = (ColumnInfo)in.readObject();
        }
    }
}
