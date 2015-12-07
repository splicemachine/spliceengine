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
 * Tentative constraint descriptor. Serves for both add and drop constraint currently.
 */
public class TentativeAddConstraintDesc extends AlterTableDDLDescriptor implements TransformingDDLDescriptor {
    private String tableVersion;
    private long newConglomId;
    private long oldConglomId;
    private long indexConglomerateId;
    private int[] srcColumnOrdering;
    private int[] targetColumnOrdering;
    private ColumnInfo[] columnInfos;

    public TentativeAddConstraintDesc(DDLMessage.TentativeAddConstraint tentativeAddConstraint) {
        this.tableVersion = tentativeAddConstraint.getTableVersion();
        this.newConglomId = tentativeAddConstraint.getNewConglomId();
        this.oldConglomId = tentativeAddConstraint.getOldConglomId();
        this.indexConglomerateId = tentativeAddConstraint.getIndexConglomerateId();
        this.srcColumnOrdering = Ints.toArray(tentativeAddConstraint.getSrcColumnOrderingList());
        this.targetColumnOrdering = Ints.toArray(tentativeAddConstraint.getTargetColumnOrderingList());
        this.columnInfos = DDLUtils.deserializeColumnInfoArray(tentativeAddConstraint.getColumnInfos().toByteArray());
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
        return create(tableVersion, srcColumnOrdering, targetColumnOrdering, columnInfos, keyEncoder);
    }

    @Override
    public RowTransformer createRowTransformer() throws IOException {
        return create(tableVersion, srcColumnOrdering, targetColumnOrdering, columnInfos, null);
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

    public long getIndexConglomerateId() {
        return indexConglomerateId;
    }

    private ExecRow createSourceTemplate() throws IOException {
        int rowWidth = columnInfos.length;
        ExecRow srcRow = new ValueRow(rowWidth);
        try {
            for (int i=0; i<rowWidth; i++) {
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
                                         ColumnInfo[] columnInfos, KeyEncoder keyEncoder) throws IOException {
        // template rows : size is same when not dropping/adding columns
        ExecRow srcRow = new ValueRow(columnInfos.length);
        ExecRow templateRow = new ValueRow(columnInfos.length);
        int[] columnMapping = new int[columnInfos.length];

        // Set the types on each column
        try {
            for (int i=0; i<columnInfos.length; i++) {
                int columnPosition = i+1;
                srcRow.setColumn(columnPosition, columnInfos[i].dataType.getNull());
                templateRow.setColumn(columnPosition, columnInfos[i].dataType.getNull());
                columnMapping[i] = columnPosition;
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
