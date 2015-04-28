package com.splicemachine.derby.ddl;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.pipeline.api.RowTransformer;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.ddl.TransformingDDLDescriptor;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.writehandler.altertable.AlterTableInterceptWriteHandler;

/**
 * Add column descriptor
 */
public class TentativeAddColumnDesc extends AlterTableDDLDescriptor implements TransformingDDLDescriptor, Externalizable{
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
        return create(tableVersion, columnOrdering, columnInfos);
    }

    @Override
    public WriteHandler createWriteHandler(RowTransformer transformer) throws IOException {
        return new AlterTableInterceptWriteHandler(transformer, Bytes.toBytes(String.valueOf(newConglomId)));

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

    private static RowTransformer create(String tableVersion,
                                        int[] sourceKeyOrdering,
                                        ColumnInfo[] columnInfos) throws IOException {

        // template rows
        ExecRow srcRow = new ValueRow(columnInfos.length-1);
        ExecRow templateRow = new ValueRow(columnInfos.length);
        // columnMapping of srcRow cols to templateRow cols - will NOT be 1-1.
        // templateRow MAY have new default value, so we're not including it in columnMapping.
        int[] columnMapping = new int[columnInfos.length-1];

        try {
            for (int i=0; i<columnInfos.length-1; i++) {
                int columnPosition = i+1;
                srcRow.setColumn(columnPosition, columnInfos[i].dataType.getNull());
                templateRow.setColumn(columnPosition, columnInfos[i].dataType.getNull());
                columnMapping[i] = columnPosition;
            }
            // set default value if given
            DataValueDescriptor newColDefaultValue = columnInfos[columnInfos.length-1].defaultValue;
            templateRow.setColumn(columnInfos.length,
                                (newColDefaultValue != null ?
                                    newColDefaultValue :
                                    columnInfos[columnInfos.length - 1].dataType.getNull()));
        } catch (StandardException e) {
            throw Exceptions.getIOException(e);
        }

        // create the row transformer
        return createRowTransformer(tableVersion,
                                    sourceKeyOrdering,
                                    sourceKeyOrdering,
                                    columnMapping,
                                    srcRow,
                                    templateRow);
    }

}
