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

package com.splicemachine.derby.stream.output.direct;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.kvpair.KVPair;
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
import com.splicemachine.si.api.txn.TxnView;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 1/13/16
 */
public class DirectPipelineWriter implements TableWriter<KVPair>,AutoCloseable{
    private final long destConglomerate;
    private final byte[] token;
    private TxnView txn;
    private final OperationContext opCtx;
    private final boolean skipIndex;
    protected String tableVersion;

    private RecordingCallBuffer<KVPair> writeBuffer;

    public DirectPipelineWriter(long destConglomerate, TxnView txn, byte[] token, OperationContext opCtx, boolean skipIndex, String tableVersion){
        this.destConglomerate=destConglomerate;
        this.txn=txn;
        this.token=token;
        this.opCtx=opCtx;
        this.skipIndex=skipIndex;
        this.tableVersion = tableVersion;
    }

    @Override
    public void open() throws StandardException{
        WriteCoordinator wc = PipelineDriver.driver().writeCoordinator();
        WriteConfiguration writeConfiguration = new DirectWriteConfiguration(wc.defaultWriteConfiguration());
        try{
            this.writeBuffer = wc.writeBuffer(
                    Bytes.toBytes(Long.toString(destConglomerate)),
                    txn, null,
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
    public void write(KVPair row) throws StandardException{
        try{
            if (row != null)
                writeBuffer.add(row);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void write(Iterator<KVPair> rows) throws StandardException{
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
    public void setTxn(TxnView txn){
        this.txn = txn;
    }

    @Override
    public TxnView getTxn(){
        return txn;
    }

    @Override
    public byte[] getToken() {
        return token;
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
