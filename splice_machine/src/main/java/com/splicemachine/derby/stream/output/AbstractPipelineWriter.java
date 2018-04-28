/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.Callable;

/**
 * Created by jleach on 5/20/15.
 */
public abstract class AbstractPipelineWriter<T> implements AutoCloseable, TableWriter<T> {
    protected TxnView txn;
    protected byte[] token;
    protected byte[] destinationTable;
    protected long heapConglom;
    protected  TriggerHandler triggerHandler;
    protected Callable<Void> flushCallback;
    protected RecordingCallBuffer<KVPair> writeBuffer;
    protected WriteCoordinator writeCoordinator;
    protected DMLWriteOperation operation;
    protected OperationContext operationContext;

    public AbstractPipelineWriter(TxnView txn, byte[] token, long heapConglom, OperationContext operationContext) {
        this.txn = txn;
        this.token = token;
        this.heapConglom = heapConglom;
        this.destinationTable = Bytes.toBytes(Long.toString(heapConglom));
        this.operationContext = operationContext;
        if (operationContext != null) {
            this.operation = (DMLWriteOperation) operationContext.getOperation();
        }
    }

    @Override
    public void open(TriggerHandler triggerHandler, SpliceOperation operation) throws StandardException {
        writeCoordinator = PipelineDriver.driver().writeCoordinator();
        this.triggerHandler = triggerHandler;
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
    public byte[] getToken() {
        return token;
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public byte[] getDestinationTable() {
        return destinationTable;
    }

    protected void beforeRow(ExecRow row) throws StandardException {
        TriggerHandler.fireBeforeRowTriggers(triggerHandler, row);
        if (operation != null)
            operation.evaluateGenerationClauses(row);
    }

    public void close() throws StandardException {

        try {
            TriggerHandler.firePendingAfterTriggers(triggerHandler, flushCallback);
            if (writeBuffer != null) {
                writeBuffer.flushBuffer();
                writeBuffer.close();
                WriteStats ws = writeBuffer.getWriteStats();
                operationContext.recordPipelineWrites(ws.getWrittenCounter());
                operationContext.recordRetry(ws.getRetryCounter());
                operationContext.recordThrownErrorRows(ws.getThrownErrorsRows());
                operationContext.recordRetriedRows(ws.getRetriedRows());
                operationContext.recordPartialRows(ws.getPartialRows());
                operationContext.recordPartialThrownErrorRows(ws.getPartialThrownErrorRows());
                operationContext.recordPartialRetriedRows(ws.getPartialRetriedRows());
                operationContext.recordPartialIgnoredRows(ws.getPartialIgnoredRows());
                operationContext.recordPartialWrite(ws.getPartialWrite());
                operationContext.recordIgnoredRows(ws.getIgnoredRows());
                operationContext.recordCatchThrownRows(ws.getCatchThrownRows());
                operationContext.recordCatchRetriedRows(ws.getCatchRetriedRows());
                operationContext.recordRegionTooBusy(ws.getRegionTooBusy());
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw Exceptions.parseException(e);
        }

    };

    @Override
    public OperationContext getOperationContext() {
        return operationContext;
    }
}
