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
import java.io.ObjectInput;
import java.io.ObjectOutput;
import splice.com.google.common.primitives.Ints;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
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
public class TentativeDropPKConstraintDesc extends AlterTableDDLDescriptor implements TransformingDDLDescriptor {
    private String tableVersion;
    private long newConglomId;
    private long oldConglomId;
    private int[] srcColumnOrdering;
    private int[] targetColumnOrdering;
    private ColumnInfo[] columnInfos;

    public TentativeDropPKConstraintDesc(DDLMessage.TentativeDropPKConstraint tentativeDropPKConstraint,
                                         PipelineExceptionFactory exceptionFactory) {
        super(exceptionFactory);
        this.tableVersion = tentativeDropPKConstraint.getTableVersion();
        this.newConglomId = tentativeDropPKConstraint.getNewConglomId();
        this.oldConglomId = tentativeDropPKConstraint.getOldConglomId();
        this.srcColumnOrdering = Ints.toArray(tentativeDropPKConstraint.getSrcColumnOrderingList());
        this.targetColumnOrdering = Ints.toArray(tentativeDropPKConstraint.getTargetColumnOrderingList());
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

        builder.template(templateRow)
                .tableVersion(tableVersion)
               .rowDecodingMap(baseColumnOrder).keyColumnEncodingOrder(keyColumnEncodingOrder)
               .keyColumnSortOrder(getKeyColumnSortOrder(nColumns))
               .keyColumnTypes(getKeyColumnTypes(templateRow, keyColumnEncodingOrder))
               .accessedKeyColumns(getAccessedKeyColumns(keyColumnEncodingOrder))
               .keyDecodingMap(getKeyDecodingMap(accessedPKColumns, baseColumnOrder, keyColumnEncodingOrder));

        return builder;
    }

    //Todo -sf- we can remove this once we are comfortable not making it externalizable
//    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(tableVersion);
        out.writeLong(newConglomId);
        out.writeLong(oldConglomId);
        ArrayUtil.writeIntArray(out, srcColumnOrdering);
        ArrayUtil.writeIntArray(out, targetColumnOrdering);
        out.writeInt(columnInfos.length);
        for (ColumnInfo col:columnInfos) {
            out.writeObject(col);
        }
    }

//    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tableVersion = (String) in.readObject();
        newConglomId = in.readLong();
        oldConglomId = in.readLong();
        srcColumnOrdering = ArrayUtil.readIntArray(in);
        targetColumnOrdering = ArrayUtil.readIntArray(in);
        int size = in.readInt();
        columnInfos = new ColumnInfo[size];
        for (int i = 0; i < size; ++i) {
            columnInfos[i] = (ColumnInfo)in.readObject();
        }
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
                                         ColumnInfo[] columnInfos,
                                         KeyEncoder keyEncoder) throws IOException {
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
