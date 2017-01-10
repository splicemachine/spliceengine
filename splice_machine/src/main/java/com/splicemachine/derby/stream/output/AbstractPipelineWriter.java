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

package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.storage.Record;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.concurrent.Callable;

/**
 * Created by jleach on 5/20/15.
 */
public abstract class AbstractPipelineWriter<T> implements AutoCloseable, TableWriter<T> {
    protected Txn txn;
    protected byte[] destinationTable;
    protected long heapConglom;
    protected  TriggerHandler triggerHandler;
    protected Callable<Void> flushCallback;
    protected RecordingCallBuffer<Record> writeBuffer;
    protected WriteCoordinator writeCoordinator;
    protected DMLWriteOperation operation;
    protected OperationContext operationContext;

    public AbstractPipelineWriter(Txn txn,long heapConglom,OperationContext operationContext) {
        this.txn = txn;
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
    public void setTxn(Txn txn) {
        this.txn = txn;
    }

    @Override
    public Txn getTxn() {
        return txn;
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
