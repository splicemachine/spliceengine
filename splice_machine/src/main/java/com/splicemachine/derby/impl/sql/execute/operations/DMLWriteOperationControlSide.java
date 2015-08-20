package com.splicemachine.derby.impl.sql.execute.operations;

import java.util.concurrent.Callable;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.StandardIterators;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.job.JobResults;
import com.splicemachine.job.SimpleJobResults;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.impl.WriteCoordinator;

/**
 * Control side shuffle for DML write operations.
 * <p/>
 * For UPDATE/DELETE/INSERT where the source operation is providing us a small number of rows read them directly from
 * the source's row provider and write to destination table.  The sub-tree under this operation is still
 * serialized and attached to the scan, but we avoid the task framework.  UPDATES and DELETES are approximately
 * 3 times faster this way, in the context of our local TPCC benchmark.
 */
class DMLWriteOperationControlSide {

    private final DMLWriteOperation dmlOperation;
    private final SpliceOperation source;
    private final WriteCoordinator writeCoordinator;
    private final KVPair.Type dataType;
    private final boolean isUpdate;
    private final TriggerHandler triggerHandler;

    public DMLWriteOperationControlSide(DMLWriteOperation dmlOperation) {
        this.dmlOperation = dmlOperation;
        this.source = dmlOperation.getSource();
        this.writeCoordinator = SpliceDriver.driver().getTableWriter();
        this.isUpdate = dmlOperation instanceof UpdateOperation;
        this.dataType = isUpdate ? KVPair.Type.UPDATE : (dmlOperation instanceof InsertOperation) ? KVPair.Type.INSERT : KVPair.Type.DELETE;
        this.triggerHandler = dmlOperation.getTriggerHandler();
    }

    public JobResults controlSideShuffle(SpliceRuntimeContext runtimeContext) throws Exception {
        //
        // WriteBuffer for destination table.
        //
        RecordingCallBuffer<KVPair> writeBuffer = writeCoordinator.writeBuffer(dmlOperation.getDestinationTable(), runtimeContext.getTxn(), runtimeContext);
        writeBuffer = dmlOperation.transformWriteBuffer(writeBuffer);
        Callable<Void> flushCallback = TriggerHandler.flushCallback(writeBuffer);

        //
        // PairEncoder: For local ExecRow -> KVPair -> CallBuffer
        //
        KeyEncoder keyEncoder = dmlOperation.getKeyEncoder(runtimeContext);
        DataHash<ExecRow> rowHash = dmlOperation.getRowHash(runtimeContext);
        PairEncoder pairEncoder = new PairEncoder(keyEncoder, rowHash, dataType);

        //
        // PairDecoder: for RowProvider/Scan: KeyValue -> ExecRow translation.
        //
        PairDecoder pairDecoder = OperationUtils.getPairDecoder(source, runtimeContext);
        RowProvider rowProvider = source.getMapRowProvider(source, pairDecoder, runtimeContext);

        StandardIterator<ExecRow> iterator = StandardIterators.wrap(rowProvider);
        //
        // For the update operation we wrap the source rows in an iterator that will skip no-op rows.
        //
        if (isUpdate) {
            iterator = ((UpdateOperation) dmlOperation).buildNoOpRowSkipper(iterator);
        }

        long rowsSunk = 0;
        try {
            iterator.open();
            ExecRow execRow;
            while ((execRow = iterator.next(runtimeContext)) != null) {
                TriggerHandler.fireBeforeRowTriggers(triggerHandler, execRow);
                dmlOperation.setCurrentRow(execRow);

                dmlOperation.evaluateGenerationClauses(execRow);

                KVPair kvPair = pairEncoder.encode(execRow);
                writeBuffer.add(kvPair);
                rowsSunk++;
                if (isUpdate) {
                    rowsSunk += ((UpdateOperationNoOpRowSkipper) iterator).getSkippedRows();
                }
                TriggerHandler.fireAfterRowTriggers(triggerHandler, execRow, flushCallback);
            }

            // The last call of iterator.next() may have skipped a zillion rows and then returned null
            if (isUpdate) {
                rowsSunk += ((UpdateOperationNoOpRowSkipper) iterator).getSkippedRows();
            }

            TriggerHandler.firePendingAfterTriggers(triggerHandler, flushCallback);

            dmlOperation.setRowsSunk(rowsSunk);

        } finally {
            iterator.close();
            writeBuffer.close();
            pairEncoder.close();
            rowHash.close();
            keyEncoder.close();
        }

        return new SimpleJobResults(new EmptyJobStats(), null);
    }


}
