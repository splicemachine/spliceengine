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

public class OnDeleteSetNull extends OnDeleteAbstractAction {
    public OnDeleteSetNull(Long childBaseTableConglomId,
                           Long backingIndexConglomId,
                           DDLMessage.FKConstraintInfo constraintInfo,
                           WriteContext writeContext,
                           TxnOperationFactory txnOperationFactory,
                           ForeignKeyViolationProcessor violationProcessor) throws Exception {
        super(childBaseTableConglomId, backingIndexConglomId, constraintInfo, writeContext, txnOperationFactory, violationProcessor);
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
    protected WriteResult handleExistingRow(byte[] indexRowId) throws Exception {
        byte[] baseTableRowId = new byte[0];
        try {
            baseTableRowId = toChildBaseRowId(indexRowId, constraintInfo);
        } catch (StandardException e) {
            e.printStackTrace();
        }
        KVPair pair = constructUpdateToNull(baseTableRowId);
        pipelineBuffer.add(pair);
        mutationBuffer.putIfAbsent(pair, pair);
        return WriteResult.success();
    }
}
