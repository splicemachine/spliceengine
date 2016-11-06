/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.ddl;

import java.io.IOException;

import org.spark_project.guava.primitives.Ints;
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

    public TentativeAddConstraintDesc(DDLMessage.TentativeAddConstraint tentativeAddConstraint,
                                      PipelineExceptionFactory exceptionFactory) {
        super(exceptionFactory);
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
                .template(templateRow)
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
            throw exceptionFactory.doNotRetry(e);
        }
        return srcRow;
    }

    private RowTransformer create(String tableVersion,
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
