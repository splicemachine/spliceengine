package com.splicemachine.derby.impl.sql.execute.operations;

import static com.google.common.base.Throwables.propagateIfInstanceOf;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.writeconfiguration.ForwardingWriteConfiguration;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.sql.execute.TaskIdStack;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.derby.utils.marshall.SpreadBucket;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.Timer;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.ActiveWriteTxn;
import com.splicemachine.uuid.Snowflake;

/**
 * OperationSink instance which writes to Hbase tables (temp tables, or non-temp in the case of DML operations).
 */
public class TableOperationSink implements OperationSink {

    private final WriteCoordinator writeCoordinator;
    private final SinkingOperation operation;
    private final byte[] taskId;
    private final Timer totalTimer;
    private final TxnView txn;
    private final byte[] destinationTable;
    private final TableOperationSinkTrace traceMetricRecorder;
    private final boolean destinationIsTemp;
    private final boolean sourceIsDMLOp;
    private final TriggerHandler triggerHandler;

    private long rowsRead;
    private long rowsWritten;
    private Timer writeTimer;

    public TableOperationSink(byte[] taskId,
                              SinkingOperation operation,
                              WriteCoordinator writeCoordinator,
                              TxnView txn,
                              long statementId,
                              long waitTimeNs,
                              byte[] destinationTable) {
        this.writeCoordinator = writeCoordinator;
        this.taskId = taskId;
        this.operation = operation;
        this.txn = txn;
        this.destinationTable = destinationTable;
        //we always record this time information, because it's cheap relative to the per-row timing
        this.totalTimer = Metrics.newTimer();
        this.traceMetricRecorder = new TableOperationSinkTrace(operation, taskId, statementId, waitTimeNs, txn);
        this.destinationIsTemp = Bytes.equals(destinationTable, SpliceConstants.TEMP_TABLE_BYTES);
        this.sourceIsDMLOp = (this.operation instanceof DMLWriteOperation);
        this.triggerHandler = sourceIsDMLOp ? ((DMLWriteOperation) operation).getTriggerHandler() : null;
    }

    /**
     * Entry point to this class.  Inits PairEncoder and CallBuffer and sinks rows from source operation.
     */
    @Override
    public TaskStats sink(SpliceRuntimeContext context) throws Exception {
        //add ourselves to the task id list
        TaskIdStack.pushTaskId(taskId);

        writeTimer = context.newTimer();

        PairEncoder encoder = initPairEncoder(context);
        try {

            RecordingCallBuffer<KVPair> writeBuffer = intCallBuffer(context);

            totalTimer.startTiming();
            sinkRows(context, encoder, writeBuffer);

            writeTimer.startTiming();
            writeBuffer.flushBuffer();
            writeBuffer.close();
            writeTimer.stopTiming();

            //stop timing events. We do this inside of the try block because we don't care if the task fails for some reason
            totalTimer.stopTiming();
            traceMetricRecorder.recordTraceMetrics(context, writeTimer, writeBuffer.getWriteStats());

        } catch (Exception e) {
            propagateIfInstanceOf(Throwables.getRootCause(e), InterruptedException.class);
            throw e;
        } finally {
            Closeables.closeQuietly(encoder);
            TaskIdStack.popTaskId();
            operation.close();
        }

        // Increment reported number of rows by row count of DML operation filtered rows.
        if (sourceIsDMLOp) {
            rowsWritten += ((DMLWriteOperation) operation).getFilteredRows();
        }

        final boolean[] usedTempBuckets = destinationIsTemp ? ((TempPairEncoder) encoder).getUsedTempBuckets() : null;
        return new TaskStats(totalTimer.getTime().getWallClockTime(), rowsRead, rowsWritten, usedTempBuckets);
    }

    /**
     * The caller has initialized everything we need.  Now just iterate over rows from the source and send them to
     * the destination table.
     */
    private void sinkRows(SpliceRuntimeContext context, PairEncoder encoder, RecordingCallBuffer<KVPair> writeBuffer) throws Exception {

        /* After triggers can only be fired when rows have been flushed (and constraints checked).  We let the trigger
         * handler class contain the logic for how often the flush happens, but we have to provide it with a callback
         * for causing the flush.  The CallBuffer may have already flushed before the trigger handler calls flush, in
         * which case this callback will do nothing.  What is important is that this callback will cause constraint
         * violation exceptions to be thrown (when applicable) before we fire after triggers. */
        Callable<Void> flushCallback = triggerHandler == null ? null : TriggerHandler.flushCallback(writeBuffer);

        ExecRow row;
        while ((row = operation.getNextSinkRow(context)) != null) {
            rowsRead++;
            SpliceBaseOperation.checkInterrupt(rowsRead, SpliceConstants.interruptLoopCheck);

            TriggerHandler.fireBeforeRowTriggers(triggerHandler, row);

            if (sourceIsDMLOp) {
                ((DMLWriteOperation) operation).evaluateGenerationClauses(row);
            }

            writeTimer.startTiming();
            KVPair kvPair = encoder.encode(row);
            writeBuffer.add(kvPair);

            TriggerHandler.fireAfterRowTriggers(triggerHandler, row, flushCallback);

            writeTimer.tick(1);
            rowsWritten++;
        }

        TriggerHandler.firePendingAfterTriggers(triggerHandler, flushCallback);
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // methods used in initialization are below
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * init RecordingCallBuffer
     */
    private RecordingCallBuffer<KVPair> intCallBuffer(final SpliceRuntimeContext context) throws StandardException {
        TxnView txn = this.txn;
        WriteConfiguration configuration = writeCoordinator.defaultWriteConfiguration();
        if (destinationIsTemp) {
            /* We are writing to the TEMP Table. The timestamp has a useful meaning in the TEMP table, which is that
             * it should be the longified version of the job id (to facilitate dropping data from TEMP efficiently.
             * However, timestamps can't be negative, so we just  take the time portion of the uuid out and stringify */
            long txnId = Snowflake.timestampFromUUID(Bytes.toLong(operation.getUniqueSequenceId()));
            txn = new ActiveWriteTxn(txnId, txnId, Txn.ROOT_TRANSACTION);
            configuration = new ForwardingWriteConfiguration(configuration){
                @Override
                public WriteResponse globalError(Throwable t) throws ExecutionException{
                    /*
                     * When writing to TEMP, it's okay if we re-write our own rows a few times, which means that
                     * we are able to retry in the event of CallTimeout and SocketTimeout exceptions.
                     */
                    if(t instanceof CallTimeoutException ||
                            t instanceof SocketTimeoutException)
                        return WriteResponse.RETRY;
                    else
                        return super.globalError(t);
                }

                @Override public MetricFactory getMetricFactory(){ return context; }
            };
        }else if(context.isActive()){
            configuration = new ForwardingWriteConfiguration(configuration){
                @Override
                public MetricFactory getMetricFactory(){
                    return context;
                }
            };
        }
        RecordingCallBuffer<KVPair> bufferToTransform = writeCoordinator.writeBuffer(destinationTable, txn,configuration);
        return operation.transformWriteBuffer(bufferToTransform);
    }

    /**
     * init PairEncoder
     */
    private PairEncoder initPairEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        KeyEncoder keyEncoder = operation.getKeyEncoder(spliceRuntimeContext);
        DataHash rowHash = operation.getRowHash(spliceRuntimeContext);
        KVPair.Type dataType = operation instanceof UpdateOperation ? KVPair.Type.UPDATE : KVPair.Type.INSERT;
        dataType = operation instanceof DeleteOperation ? KVPair.Type.DELETE : dataType;

        if (destinationIsTemp) {
            return new TempPairEncoder(keyEncoder, rowHash, dataType);
        } else {
            return new PairEncoder(keyEncoder, rowHash, dataType);
        }
    }

    private static class TempPairEncoder extends PairEncoder {
        private final boolean[] usedTempBuckets;
        private final SpreadBucket tempSpread;

        public TempPairEncoder(KeyEncoder keyEncoder, DataHash rowHash, KVPair.Type dataType) {
            super(keyEncoder, rowHash, dataType);
            this.tempSpread = SpliceDriver.driver().getTempTable().getCurrentSpread();
            this.usedTempBuckets = new boolean[tempSpread.getNumBuckets()];
        }

        @Override
        public KVPair encode(ExecRow execRow) throws StandardException, IOException {
            byte[] key = keyEncoder.getKey(execRow);
            usedTempBuckets[tempSpread.bucketIndex(key[0])] = true;
            rowEncoder.setRow(execRow);
            byte[] row = rowEncoder.encode();
            return new KVPair(key, row, pairType);
        }

        public boolean[] getUsedTempBuckets() {
            return usedTempBuckets;
        }
    }

}