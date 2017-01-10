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

package com.splicemachine.derby.stream.output.direct;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.config.ForwardingWriteConfiguration;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.pipeline.utils.PipelineUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.storage.Record;
import java.io.IOException;
import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 1/13/16
 */
public class DirectPipelineWriter implements TableWriter<Record>,AutoCloseable{
    private final long destConglomerate;
    private Txn txn;
    private final OperationContext opCtx;
    private final boolean skipIndex;

    private RecordingCallBuffer<Record> writeBuffer;

    public DirectPipelineWriter(long destConglomerate,Txn txn,OperationContext opCtx,boolean skipIndex){
        this.destConglomerate=destConglomerate;
        this.txn=txn;
        this.opCtx=opCtx;
        this.skipIndex=skipIndex;
    }

    @Override
    public void open() throws StandardException{
        WriteCoordinator wc = PipelineDriver.driver().writeCoordinator();
        WriteConfiguration writeConfiguration = new DirectWriteConfiguration(wc.defaultWriteConfiguration());
        try{
            this.writeBuffer = wc.writeBuffer(
                    Bytes.toBytes(Long.toString(destConglomerate)),
                    txn,
                    PipelineUtils.noOpFlushHook,
                    writeConfiguration,
                    Metrics.basicMetricFactory());
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void open(TriggerHandler triggerHandler,SpliceOperation dmlWriteOperation) throws StandardException{
        open();
    }

    @Override
    public void write(Record row) throws StandardException{
        try{
            writeBuffer.add(row);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void write(Iterator<Record> rows) throws StandardException{
        while(rows.hasNext()){
            write(rows.next());
        }
    }

    @Override
    public void close() throws StandardException{
        try{
            writeBuffer.flushBufferAndWait();
            writeBuffer.close();
            WriteStats ws = writeBuffer.getWriteStats();
            if (opCtx != null) {
                opCtx.recordPipelineWrites(ws.getWrittenCounter());
                opCtx.recordRetry(ws.getRetryCounter());
                opCtx.recordThrownErrorRows(ws.getThrownErrorsRows());
                opCtx.recordRetriedRows(ws.getRetriedRows());
                opCtx.recordPartialRows(ws.getPartialRows());
                opCtx.recordPartialThrownErrorRows(ws.getPartialThrownErrorRows());
                opCtx.recordPartialRetriedRows(ws.getPartialRetriedRows());
                opCtx.recordPartialIgnoredRows(ws.getPartialIgnoredRows());
                opCtx.recordPartialWrite(ws.getPartialWrite());
                opCtx.recordIgnoredRows(ws.getIgnoredRows());
                opCtx.recordCatchThrownRows(ws.getCatchThrownRows());
                opCtx.recordCatchRetriedRows(ws.getCatchRetriedRows());
                opCtx.recordRegionTooBusy(ws.getRegionTooBusy());
            }
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void setTxn(Txn txn){
        this.txn = txn;
    }

    @Override
    public Txn getTxn(){
        return txn;
    }

    @Override
    public byte[] getDestinationTable(){
        return Bytes.toBytes(Long.toString(destConglomerate));
    }

    @Override
    public OperationContext getOperationContext() {
        return opCtx;
    }

    public static class DirectWriteConfiguration extends ForwardingWriteConfiguration {
        protected DirectWriteConfiguration(WriteConfiguration delegate) {
            super(delegate);
        }

        @Override
        public MetricFactory getMetricFactory() {
            return Metrics.basicMetricFactory();
        }
    }
}
