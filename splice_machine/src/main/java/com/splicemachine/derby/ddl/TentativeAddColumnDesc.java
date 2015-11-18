package com.splicemachine.derby.ddl;

import java.io.IOException;
import com.splicemachine.ddl.DDLMessage;
import org.apache.hadoop.hbase.util.Bytes;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.pipeline.api.RowTransformer;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.ddl.TransformingDDLDescriptor;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.writehandler.altertable.AlterTableInterceptWriteHandler;
import org.sparkproject.guava.primitives.Ints;

/**
 * Add column descriptor
 */
public class TentativeAddColumnDesc extends AlterTableDDLDescriptor implements TransformingDDLDescriptor {
    private String tableVersion;
    private long newConglomId;
    private long oldConglomId;
    private int[] columnOrdering;
    private ColumnInfo[] columnInfos;

    public TentativeAddColumnDesc(DDLMessage.TentativeAddColumn addColumn) {
        this.tableVersion = addColumn.getTableVersion();
        this.newConglomId = addColumn.getNewConglomId();
        this.oldConglomId = addColumn.getOldConglomId();
        this.columnOrdering = Ints.toArray(addColumn.getColumnOrderingList());
        this.columnInfos = DDLUtils.deserializeColumnInfoArray(addColumn.getColumnInfo().toByteArray());
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
    public RowTransformer createRowTransformer(KeyEncoder keyEncoder) throws IOException {
        return create(tableVersion, columnOrdering, columnInfos, keyEncoder);
    }

    @Override
    public RowTransformer createRowTransformer() throws IOException {
        return create(tableVersion, columnOrdering, columnInfos, null);
    }

    @Override
    public WriteHandler createWriteHandler(RowTransformer transformer) throws IOException {
        return new AlterTableInterceptWriteHandler(transformer, Bytes.toBytes(String.valueOf(newConglomId)));

    }

    @Override
    public TableScannerBuilder setScannerBuilderProperties(TableScannerBuilder builder) throws IOException {
        ExecRow templateRow = createSourceTemplate();
        int nColumns = templateRow.nColumns();
        int[] baseColumnOrder = getRowDecodingMap(nColumns);
        int[] keyColumnEncodingOrder = columnOrdering;
        FormatableBitSet accessedPKColumns = getAccessedKeyColumns(keyColumnEncodingOrder);

        builder.template(templateRow).tableVersion(tableVersion)
                .execRowTypeFormatIds(getFormatIds(templateRow))
               .rowDecodingMap(baseColumnOrder).keyColumnEncodingOrder(keyColumnEncodingOrder)
               .keyColumnSortOrder(getKeyColumnSortOrder(nColumns))
               .keyColumnTypes(getKeyColumnTypes(templateRow, keyColumnEncodingOrder))
               .accessedKeyColumns(getAccessedKeyColumns(keyColumnEncodingOrder))
               .keyDecodingMap(getKeyDecodingMap(accessedPKColumns, baseColumnOrder, keyColumnEncodingOrder));

        return builder;
    }

    private ExecRow createSourceTemplate() throws IOException {
        ExecRow srcRow = new ValueRow(columnInfos.length-1);
        try {
            for (int i=0; i<columnInfos.length-1; i++) {
                srcRow.setColumn(i+1, columnInfos[i].dataType.getNull());
            }
        } catch (StandardException e) {
            throw Exceptions.getIOException(e);
        }
        return srcRow;
    }

    private static RowTransformer create(String tableVersion,
                                         int[] sourceKeyOrdering,
                                         ColumnInfo[] columnInfos,
                                         KeyEncoder keyEncoder) throws IOException {

        // template rows
        ExecRow srcRow = new ValueRow(columnInfos.length-1);
        ExecRow targetRow = new ValueRow(columnInfos.length);
        // columnMapping of srcRow cols to templateRow cols - will NOT be 1-1.
        // templateRow MAY have new default value, so we're not including it in columnMapping.
        int[] columnMapping = new int[columnInfos.length-1];

        try {
            for (int i=0; i<columnInfos.length-1; i++) {
                int columnPosition = i+1;
                srcRow.setColumn(columnPosition, columnInfos[i].dataType.getNull());
                targetRow.setColumn(columnPosition, columnInfos[i].dataType.getNull());
                columnMapping[i] = columnPosition;
            }
            // set default value if given
            DataValueDescriptor newColDefaultValue = columnInfos[columnInfos.length-1].defaultValue;
            targetRow.setColumn(columnInfos.length,
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
                                    targetRow,
                                    keyEncoder);
    }

}
