package com.splicemachine.derby.ddl;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.pipeline.api.RowTransformer;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.ddl.TransformingDDLDescriptor;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.writehandler.altertable.AlterTableInterceptWriteHandler;

/**
 * Drop column.
 */
public class TentativeDropColumnDesc extends AlterTableDDLDescriptor implements TransformingDDLDescriptor, Externalizable{
    private long conglomerateNumber;
    private long baseConglomerateNumber;
    private String tableVersion;
    private int[] srcColumnOrdering;
    private int[] targetColumnOrdering;
    private ColumnInfo[] columnInfos;
    private int droppedColumnPosition;

    public TentativeDropColumnDesc() {}

    public TentativeDropColumnDesc(long baseConglomerateNumber,
                                   long conglomerateNumber,
                                   String tableVersion,
                                   int[] srcColumnOrdering,
                                   int[] targetColumnOrdering,
                                   ColumnInfo[] columnInfos,
                                   int droppedColumnPosition) {
        this.conglomerateNumber = conglomerateNumber;
        this.baseConglomerateNumber = baseConglomerateNumber;
        this.tableVersion = tableVersion;
        this.srcColumnOrdering = srcColumnOrdering;
        this.targetColumnOrdering = targetColumnOrdering;
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
        return create(tableVersion, srcColumnOrdering, targetColumnOrdering, columnInfos, droppedColumnPosition);
    }

    @Override
    public WriteHandler createWriteHandler(RowTransformer transformer) throws IOException {
        return new AlterTableInterceptWriteHandler(transformer, Bytes.toBytes(String.valueOf(conglomerateNumber)));
    }

    @Override
    public TableScannerBuilder setScannerBuilderProperties(TableScannerBuilder builder) throws IOException {
        ExecRow templateRow = createSourceTemplate();
        int nColumns = templateRow.nColumns();
        int[] baseColumnOrder = getRowDecodingMap(nColumns);
        int[] keyColumnEncodingOrder = srcColumnOrdering;
        FormatableBitSet accessedPKColumns = getAccessedKeyColumns(keyColumnEncodingOrder);

        builder.template(templateRow).tableVersion(tableVersion)
               .rowDecodingMap(baseColumnOrder).keyColumnEncodingOrder(keyColumnEncodingOrder)
               .keyColumnSortOrder(getKeyColumnSortOrder(nColumns))
               .keyColumnTypes(getKeyColumnTypes(templateRow, keyColumnEncodingOrder))
               .accessedKeyColumns(getAccessedKeyColumns(keyColumnEncodingOrder))
               .keyDecodingMap(getKeyDecodingMap(accessedPKColumns, baseColumnOrder, keyColumnEncodingOrder));

        return builder;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(conglomerateNumber);
        out.writeLong(baseConglomerateNumber);
        out.writeInt(droppedColumnPosition);
        out.writeObject(tableVersion);
        ArrayUtil.writeIntArray(out, srcColumnOrdering);
        ArrayUtil.writeIntArray(out, targetColumnOrdering);
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
        srcColumnOrdering = ArrayUtil.readIntArray(in);
        targetColumnOrdering = ArrayUtil.readIntArray(in);
        int size = in.readInt();
        columnInfos = new ColumnInfo[size];
        for (int i = 0; i < size; ++i) {
            columnInfos[i] = (ColumnInfo)in.readObject();
        }
    }

    private ExecRow createSourceTemplate() throws IOException {
        ExecRow srcRow = new ValueRow(columnInfos.length);
        try {
            for (int i=0; i<columnInfos.length; i++) {
                srcRow.setColumn(i+1, columnInfos[i].dataType.getNull());
            }
        } catch (StandardException e) {
            throw Exceptions.getIOException(e);
        }
        return srcRow;
    }

    private static RowTransformer create(String tableVersion,
                                        int[] sourceKeyOrdering,
                                        int[] targetKeyOrdering,
                                        ColumnInfo[] columnInfos,
                                        int droppedColumnPosition) throws IOException {

        // template rows
        ExecRow srcRow = new ValueRow(columnInfos.length);
        ExecRow templateRow = new ValueRow(columnInfos.length-1);
        int[] columnMapping = new int[columnInfos.length];

        try {
            int i = 1;
            int j =1;
            for (ColumnInfo col : columnInfos){
                srcRow.setColumn(i, col.dataType.getNull());
                if (i != droppedColumnPosition){
                    columnMapping[i-1] = j;
                    templateRow.setColumn(j++, col.dataType.getNull());
                }
                ++i;
            }
        } catch (StandardException e) {
            throw Exceptions.getIOException(e);
        }

        // create the row transformer
        return createRowTransformer(tableVersion,
                                    sourceKeyOrdering,
                                    targetKeyOrdering,
                                    columnMapping,
                                    srcRow,
                                    templateRow);
    }
}
