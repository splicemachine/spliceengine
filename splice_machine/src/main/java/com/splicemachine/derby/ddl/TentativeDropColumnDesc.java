/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.ddl;

import java.io.IOException;

import splice.com.google.common.primitives.Ints;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.pipeline.RowTransformer;
import com.splicemachine.pipeline.altertable.AlterTableInterceptWriteHandler;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.writehandler.WriteHandler;
import com.splicemachine.primitives.Bytes;

/**
 * Drop column.
 */
public class TentativeDropColumnDesc extends AlterTableDDLDescriptor implements TransformingDDLDescriptor{
    private long conglomerateNumber;
    private long baseConglomerateNumber;
    private String tableVersion;
    private int[] srcColumnOrdering;
    private int[] targetColumnOrdering;
    private ColumnInfo[] columnInfos;
    private int droppedColumnPosition;

    public TentativeDropColumnDesc(DDLMessage.TentativeDropColumn tentativeDropColumn,PipelineExceptionFactory exceptionFactory) {
        super(exceptionFactory);
        this.conglomerateNumber = tentativeDropColumn.getNewConglomId();
        this.baseConglomerateNumber = tentativeDropColumn.getOldConglomId();
        this.tableVersion = tentativeDropColumn.getTableVersion();
        this.srcColumnOrdering = Ints.toArray(tentativeDropColumn.getOldColumnOrderingList());
        this.targetColumnOrdering = Ints.toArray(tentativeDropColumn.getNewColumnOrderingList());
        this.columnInfos = DDLUtils.deserializeColumnInfoArray(tentativeDropColumn.getColumnInfos().toByteArray());
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
                .template(templateRow)
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
            throw exceptionFactory.doNotRetry(e);
        }
        return srcRow;
    }

    private RowTransformer create(String tableVersion,
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
            throw exceptionFactory.doNotRetry(e);
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
