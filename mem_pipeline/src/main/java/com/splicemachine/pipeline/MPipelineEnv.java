/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.pipeline;

import com.splicemachine.access.api.*;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.pipeline.api.BulkWriterFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.PipelineMeter;
import com.splicemachine.pipeline.api.WritePipelineFactory;
import com.splicemachine.pipeline.context.NoOpPipelineMeter;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.pipeline.mem.DirectBulkWriterFactory;
import com.splicemachine.pipeline.mem.DirectPipelineExceptionFactory;
import com.splicemachine.pipeline.traffic.AtomicSpliceWriteControl;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.readresolve.KeyedReadResolver;
import com.splicemachine.si.api.rollforward.RollForward;
import com.splicemachine.si.api.server.ClusterHealth;
import com.splicemachine.si.api.txn.KeepAliveScheduler;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.driver.SIEnvironment;
import com.splicemachine.si.impl.store.IgnoreTxnSupplier;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.PartitionInfoCache;
import com.splicemachine.timestamp.api.TimestampSource;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class MPipelineEnv  implements PipelineEnvironment{
    private SIEnvironment siEnv;
    private BulkWriterFactory writerFactory;
    private ContextFactoryDriver ctxFactoryDriver;
    private OldestActiveTransactionTaskFactory oldestActiveTransactionTaskFactory;
    private final WritePipelineFactory pipelineFactory= new MappedPipelineFactory();

    public MPipelineEnv(SIEnvironment siEnv) throws IOException{
        super();
        this.siEnv=siEnv;
        this.writerFactory = new DirectBulkWriterFactory(new MappedPipelineFactory(),
                new AtomicSpliceWriteControl(Integer.MAX_VALUE,Integer.MAX_VALUE,Integer.MAX_VALUE,Integer.MAX_VALUE),
                pipelineExceptionFactory(),pipelineMeter());
        this.ctxFactoryDriver = ContextFactoryDriverService.loadDriver();
        this.oldestActiveTransactionTaskFactory = new OldestActiveTransactionTaskFactory() {
            @Override
            public GetOldestActiveTransactionTask get(String hostName, int port, long startupTimestamp) throws IOException {
                throw new UnsupportedOperationException("Operation not supported in Mem profile");
            }
        };
    }

    @Override
    public OperationFactory baseOperationFactory(){
        return siEnv.baseOperationFactory();
    }

    @Override
    public PartitionFactory tableFactory(){
        return siEnv.tableFactory();
    }

    @Override
    public ExceptionFactory exceptionFactory(){
        return siEnv.exceptionFactory();
    }

    @Override
    public OldestActiveTransactionTaskFactory oldestActiveTransactionTaskFactory(){
        return oldestActiveTransactionTaskFactory;
    }

    @Override
    public SConfiguration configuration(){
        return siEnv.configuration();
    }

    @Override
    public TxnStore txnStore(){
        return siEnv.txnStore();
    }

    @Override
    public OperationStatusFactory statusFactory(){
        return siEnv.statusFactory();
    }

    @Override
    public TimestampSource timestampSource(){
        return siEnv.timestampSource();
    }

    @Override
    public TxnSupplier txnSupplier(){
        return siEnv.txnSupplier();
    }

    @Override
    public IgnoreTxnSupplier ignoreTxnSupplier(){
        return siEnv.ignoreTxnSupplier();
    }

    @Override
    public RollForward rollForward(){
        return siEnv.rollForward();
    }

    @Override
    public TxnOperationFactory operationFactory(){
        return siEnv.operationFactory();
    }

    @Override
    public SIDriver getSIDriver(){
        return siEnv.getSIDriver();
    }

    @Override
    public PartitionInfoCache partitionInfoCache(){
        return siEnv.partitionInfoCache();
    }

    @Override
    public KeepAliveScheduler keepAliveScheduler(){
        return siEnv.keepAliveScheduler();
    }

    @Override
    public DataFilterFactory filterFactory(){
        return siEnv.filterFactory();
    }

    @Override
    public Clock systemClock(){
        return siEnv.systemClock();
    }

    @Override
    public KeyedReadResolver keyedReadResolver(){
        return siEnv.keyedReadResolver();
    }

    @Override
    public PipelineExceptionFactory pipelineExceptionFactory(){
        return DirectPipelineExceptionFactory.INSTANCE;
    }

    @Override
    public PipelineDriver getPipelineDriver(){
        return PipelineDriver.driver();
    }

    @Override
    public ContextFactoryDriver contextFactoryDriver(){
        return ctxFactoryDriver;
    }

    @Override
    public PipelineCompressor pipelineCompressor(){
        return null;
    }

    @Override
    public BulkWriterFactory writerFactory(){
        return writerFactory;
    }

    @Override
    public PipelineMeter pipelineMeter(){
        return NoOpPipelineMeter.INSTANCE;
    }

    @Override
    public WritePipelineFactory pipelineFactory(){
        return pipelineFactory;
    }

    @Override
    public SnowflakeFactory snowflakeFactory() {
        return siEnv.snowflakeFactory();
    }

    @Override
    public ClusterHealth clusterHealthFactory() {
        return siEnv.clusterHealthFactory();
    }

    @Override
    public FilesystemAdmin filesystemAdmin() {
        return siEnv.filesystemAdmin();
    }

    @Override
    public DistributedFileSystem fileSystem(String path) throws IOException, URISyntaxException {
        return siEnv.fileSystem(path);
    }
}
