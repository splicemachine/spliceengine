package com.splicemachine.derby.ddl;

import java.io.IOException;
import com.splicemachine.ddl.DDLMessage;
import org.apache.hadoop.hbase.util.Bytes;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
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
 * Drop column.
 */
public class TentativeDropColumnDesc extends AlterTableDDLDescriptor implements TransformingDDLDescriptor {
    private long conglomerateNumber;
    private long baseConglomerateNumber;
    private String tableVersion;
    private int[] srcColumnOrdering;
    private int[] targetColumnOrdering;
    private ColumnInfo[] columnInfos;
    private int droppedColumnPosition;

    public TentativeDropColumnDesc(DDLMessage.TentativeDropColumn tentativeDropColumn) {
        this.conglomerateNumber = tentativeDropColumn.getNewConglomId();
        this.baseConglomerateNumber = tentativeDropColumn.getOldConglomId();
        this.tableVersion = tentativeDropColumn.getTableVersion();
        this.srcColumnOrdering = Ints.toArray(tentativeDropColumn.getOldColumnOrderingList());
        this.targetColumnOrdering = Ints.toArray(tentativeDropColumn.getNewColumnOrderingList());
        this.columnInfos = DDLUtils.deserializeColumnInfoArray(tentativeDropColumn.getColumnInfos().toByteArray());;
        this.droppedColumnPosition = tentativeDropColumn.getDroppedColumnPosition();

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
    public RowTransformer createRowTransformer(KeyEncoder keyEncoder) throws IOException {
        return create(tableVersion, srcColumnOrdering, targetColumnOrdering, columnInfos, droppedColumnPosition, keyEncoder);
    }

    @Override
    public RowTransformer createRowTransformer() throws IOException {
        return create(tableVersion, srcColumnOrdering, targetColumnOrdering, columnInfos, droppedColumnPosition, null);
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
                .execRowTypeFormatIds(getFormatIds(templateRow))
               .rowDecodingMap(baseColumnOrder).keyColumnEncodingOrder(keyColumnEncodingOrder)
               .keyColumnSortOrder(getKeyColumnSortOrder(nColumns))
               .keyColumnTypes(getKeyColumnTypes(templateRow, keyColumnEncodingOrder))
               .accessedKeyColumns(getAccessedKeyColumns(keyColumnEncodingOrder))
               .keyDecodingMap(getKeyDecodingMap(accessedPKColumns, baseColumnOrder, keyColumnEncodingOrder));

        return builder;
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
                                         int droppedColumnPosition, KeyEncoder keyEncoder) throws IOException {

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
                                    templateRow,
                                    keyEncoder);
    }
}
