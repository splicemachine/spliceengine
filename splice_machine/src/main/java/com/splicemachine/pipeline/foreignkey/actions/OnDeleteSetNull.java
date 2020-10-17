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

package com.splicemachine.pipeline.foreignkey.actions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.derby.stream.output.update.NonPkRowHash;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.foreignkey.ForeignKeyViolationProcessor;
import com.splicemachine.si.api.data.TxnOperationFactory;

import java.util.Arrays;

public class OnDeleteSetNull extends OnDeleteAbstractAction {

    private final boolean isSelfReferencing;

    public OnDeleteSetNull(Long backingIndexConglomId,
                           DDLMessage.FKConstraintInfo constraintInfo,
                           WriteContext writeContext,
                           TxnOperationFactory txnOperationFactory,
                           ForeignKeyViolationProcessor violationProcessor) throws Exception {
        super(backingIndexConglomId, constraintInfo, writeContext, txnOperationFactory, violationProcessor);
        isSelfReferencing = childBaseTableConglomId == constraintInfo.getParentTableConglomerate();
    }

    private KVPair constructUpdateToNull(byte[] rowId ) throws StandardException {
        int colCount = constraintInfo.getTable().getFormatIdsCount();
        int[] keyColumns = constraintInfo.getColumnIndicesList().stream().mapToInt(i -> i).toArray();
        int[] oneBased = new int[colCount + 1];
        for (int i = 0; i < colCount; ++i) {
            oneBased[i + 1] = i;
        }
        FormatableBitSet heapSet = new FormatableBitSet(oneBased.length);
        ExecRow execRow = WriteReadUtils.getExecRowFromTypeFormatIds(constraintInfo.getTable().getFormatIdsList().stream().mapToInt(i -> i).toArray());
        for (int keyColumn : keyColumns) {
            execRow.setColumn(keyColumn, execRow.getColumn(keyColumn).getNewNull());
            heapSet.set(keyColumn);
        }
        DescriptorSerializer[] serializers = VersionedSerializers.forVersion(constraintInfo.getTable().getTableVersion(), true).getSerializers(execRow);
        EntryDataHash entryEncoder = new NonPkRowHash(oneBased, null, serializers, heapSet);
        ValueRow rowToEncode = new ValueRow(execRow.getRowArray().length);
        rowToEncode.setRowArray(execRow.getRowArray());
        entryEncoder.setRow(rowToEncode);
        byte[] value = entryEncoder.encode();
        return new KVPair(rowId, value, KVPair.Type.UPDATE);
    }

    @Override
    protected WriteResult handleExistingRow(byte[] indexRowId, byte[] sourceRowKey) throws Exception {
        byte[] baseTableRowId = toChildBaseRowId(indexRowId, constraintInfo);
        if(isSelfReferencing && Arrays.equals(sourceRowKey, baseTableRowId)) {
            return WriteResult.success(); // do not add an update mutation since this row will be deleted anyway.
        }
        KVPair pair = constructUpdateToNull(baseTableRowId);
        pipelineBuffer.add(pair);
        mutationBuffer.putIfAbsent(pair, pair);
        return WriteResult.success();
    }
}
