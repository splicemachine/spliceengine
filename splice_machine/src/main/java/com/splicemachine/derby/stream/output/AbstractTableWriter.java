package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.si.api.txn.TxnView;

import java.util.concurrent.Callable;

/**
 * Created by jleach on 5/20/15.
 */
public abstract class AbstractTableWriter<T> implements AutoCloseable, TableWriter<T> {
    protected TxnView txn;
    protected byte[] destinationTable;
    protected long heapConglom;
    protected  TriggerHandler triggerHandler;
    protected Callable<Void> flushCallback;
    protected RecordingCallBuffer<KVPair> writeBuffer;
    protected WriteCoordinator writeCoordinator = SpliceDriver.driver().getTableWriter();
    protected DMLWriteOperation operation;

    public AbstractTableWriter (TxnView txn, long heapConglom) {
        this.txn = txn;
        this.heapConglom = heapConglom;
        destinationTable = Long.toString(heapConglom).getBytes();
    }

    @Override
    public void open(TriggerHandler triggerHandler, SpliceOperation operation) throws StandardException {
        writeCoordinator = SpliceDriver.driver().getTableWriter();
        this.triggerHandler = triggerHandler;
        this.operation = (DMLWriteOperation) operation;
    }

    @Override
    public void setTxn(TxnView txn) {
        this.txn = txn;
    }

    @Override
    public TxnView getTxn() {
        return txn;
    }

    @Override
    public byte[] getDestinationTable() {
        return destinationTable;
    }

    protected void beforeRow(ExecRow row) throws StandardException {
        TriggerHandler.fireBeforeRowTriggers(triggerHandler, row);
        if (operation!=null)
            operation.evaluateGenerationClauses(row);
    }

    public void close() throws StandardException {
        try {
            TriggerHandler.firePendingAfterTriggers(triggerHandler, flushCallback);
            writeBuffer.flushBuffer();
            writeBuffer.close();
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    };
}
