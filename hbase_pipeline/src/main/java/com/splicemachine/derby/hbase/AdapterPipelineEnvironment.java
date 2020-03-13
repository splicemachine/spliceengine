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
 *
 */

package com.splicemachine.derby.hbase;

import com.splicemachine.access.api.*;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.ipc.ChannelFactoryService;
import com.splicemachine.pipeline.MappedPipelineFactory;
import com.splicemachine.pipeline.PartitionWritePipeline;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.PipelineEnvironment;
import com.splicemachine.pipeline.SnappyPipelineCompressor;
import com.splicemachine.pipeline.api.BulkWriterFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.PipelineMeter;
import com.splicemachine.pipeline.api.WritePipelineFactory;
import com.splicemachine.ipc.RpcChannelFactory;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.pipeline.utils.SimplePipelineCompressor;
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
import com.splicemachine.si.data.hbase.coprocessor.AdapterSIEnvironment;
import com.splicemachine.si.data.hbase.coprocessor.HOldestActiveTransactionTaskFactory;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.driver.SIEnvironment;
import com.splicemachine.si.impl.store.IgnoreTxnSupplier;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.PartitionInfoCache;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.utils.kryo.KryoPool;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public class AdapterPipelineEnvironment implements PipelineEnvironment{
    private static volatile AdapterPipelineEnvironment INSTANCE;

    private final SIEnvironment delegate;
    private final PipelineExceptionFactory pipelineExceptionFactory;
    private final OldestActiveTransactionTaskFactory oldestActiveTransactionTaskFactory;
    private final ContextFactoryDriver contextFactoryLoader;
    private final SConfiguration pipelineConfiguration;
    private final PipelineCompressor compressor;
    private final BulkWriterFactory writerFactory;
    private final PipelineMeter meter = new CountingPipelineMeter();
    private final WritePipelineFactory pipelineFactory;

    public static AdapterPipelineEnvironment loadEnvironment(Clock systemClock, ContextFactoryDriver ctxFactoryLoader, DataSource connectionPool) throws IOException{
        AdapterPipelineEnvironment env = INSTANCE;
        if(env==null){
            synchronized(AdapterPipelineEnvironment.class){
                env = INSTANCE;
                if(env==null){
                    SIEnvironment siEnv = AdapterSIEnvironment.loadEnvironment(systemClock,ZkUtils.getRecoverableZooKeeper(),connectionPool);
                    env = new AdapterPipelineEnvironment(siEnv,ctxFactoryLoader,HPipelineExceptionFactory.INSTANCE);
                    PipelineDriver.loadDriver(env);
                    INSTANCE = env;
                }
            }
        }
        return env;
    }

    private AdapterPipelineEnvironment(SIEnvironment env,
                                       ContextFactoryDriver ctxFactoryLoader,
                                       PipelineExceptionFactory pef){
        this.delegate = env;
        this.pipelineExceptionFactory = pef;
        this.oldestActiveTransactionTaskFactory = new HOldestActiveTransactionTaskFactory();
        this.contextFactoryLoader = ctxFactoryLoader;
        this.pipelineConfiguration = env.configuration();
        this.pipelineFactory = new AvailablePipelineFactory();

        KryoPool kryoPool=new KryoPool(pipelineConfiguration.getPipelineKryoPoolSize());
        kryoPool.setKryoRegistry(new PipelineKryoRegistry());
        this.compressor = new SnappyPipelineCompressor(new SimplePipelineCompressor(kryoPool,env.getSIDriver().getOperationFactory()));

        RpcChannelFactory channelFactory = ChannelFactoryService.loadChannelFactory(this.pipelineConfiguration);
        this.writerFactory = new CoprocessorWriterFactory(compressor,partitionInfoCache(),pipelineExceptionFactory,channelFactory,
                HBaseTableInfoFactory.getInstance(configuration()));
    }

    @Override
    public Clock systemClock(){
        return delegate.systemClock();
    }

    @Override
    public KeyedReadResolver keyedReadResolver(){
        return delegate.keyedReadResolver();
    }

    @Override public PartitionFactory tableFactory(){ return delegate.tableFactory(); }
    @Override public ExceptionFactory exceptionFactory(){ return delegate.exceptionFactory(); }

    @Override
    public OldestActiveTransactionTaskFactory oldestActiveTransactionTaskFactory(){
        return oldestActiveTransactionTaskFactory;
    }

    @Override public TxnStore txnStore(){ return delegate.txnStore(); }
    @Override public OperationStatusFactory statusFactory(){ return delegate.statusFactory(); }
    @Override public TimestampSource timestampSource(){ return delegate.timestampSource(); }
    @Override public TxnSupplier txnSupplier(){ return delegate.txnSupplier(); }
    @Override public IgnoreTxnSupplier ignoreTxnSupplier(){ return delegate.ignoreTxnSupplier(); }
    @Override public RollForward rollForward(){ return delegate.rollForward(); }
    @Override public TxnOperationFactory operationFactory(){ return delegate.operationFactory(); }
    @Override public SIDriver getSIDriver(){ return delegate.getSIDriver(); }

    @Override
    public SConfiguration configuration(){
        return pipelineConfiguration;
    }

    @Override
    public PipelineExceptionFactory pipelineExceptionFactory(){
        return pipelineExceptionFactory;
    }

    @Override
    public PipelineDriver getPipelineDriver(){
        return PipelineDriver.driver();
    }

    @Override
    public ContextFactoryDriver contextFactoryDriver(){
        return contextFactoryLoader;
    }

    @Override
    public PipelineCompressor pipelineCompressor(){
        return compressor;
    }

    @Override
    public PartitionInfoCache partitionInfoCache(){
        return delegate.partitionInfoCache();
    }

    @Override
    public KeepAliveScheduler keepAliveScheduler(){
        return delegate.keepAliveScheduler();
    }

    @Override
    public DataFilterFactory filterFactory(){
        return delegate.filterFactory();
    }

    @Override
    public BulkWriterFactory writerFactory(){
        return writerFactory;
    }

    @Override
    public PipelineMeter pipelineMeter(){
        return meter;
    }

    @Override
    public WritePipelineFactory pipelineFactory(){
        return pipelineFactory;
    }

    /**
     *
     * Retrieve the appropriate filesystem based on the scheme.  If not scheme provided,
     * it will use the filesystem from the configuration.
     *
     * @param path
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    @Override
    public DistributedFileSystem fileSystem(String path) throws IOException, URISyntaxException {
        return delegate.fileSystem(path);
    }

    @Override
    public OperationFactory baseOperationFactory(){
        return delegate.baseOperationFactory();
    }


    @Override
    public SnowflakeFactory snowflakeFactory() {
        return delegate.snowflakeFactory();
    }

    @Override
    public ClusterHealth clusterHealthFactory() {
        return delegate.clusterHealthFactory();
    }

    @Override
    public FilesystemAdmin filesystemAdmin() {
        return delegate.filesystemAdmin();
    }

    private static class AvailablePipelineFactory implements WritePipelineFactory{
        /*
         * As it turns out, a Region cannot be considered to be "online" until it has been
         * added to the HBase network logic.
         *
         * For most coprocessors, this isn't an important distinction, since the coprocessor is
         * attached to the network logic, so requests can't be made unless it is through the stack.
         *
         * However, the BulkWrite pipeline sends multiple regions in a single request. Therefore, it is
         * possible for us to send a request to a region that isn't yet added to the network stack. This in turn
         * causes a race condition to occur when performing splits--specifically, a bulk write may advance the MVCC
         * write point for a newly opened daughter region before it is finished fully initializing. When that happens,
         * the daughter region will "fail after the point-of-no-return" and terminate the RegionServer JVM.
         *
         * Clearly, we don't want that to happen. In order to avoid it, we have to be sure that the region
         * is in fact fully constructed and ready to accept writes; this means that we have to check that
         * the RegionServerServices is aware of the region.
         *
         * In a perfect world, we would check this once and be done. However, doing so would mean implicitly
         * introducing the possibility that we make decisions on outdated information; i.e. we would be re-introducing
         * the race condition. We simply have to pay the price of an extra concurrent map lookup on each
         * bulk write request.
         */
        private final MappedPipelineFactory delegate = new MappedPipelineFactory();

        @Override
        public PartitionWritePipeline getPipeline(String partitionName){
            PartitionWritePipeline pipeline=delegate.getPipeline(partitionName);
            if(pipeline!=null && pipeline.getRegionCoprocessorEnvironment().isAvailable())
                return pipeline;
            else
                return null;
        }

        @Override
        public void registerPipeline(String name,PartitionWritePipeline writePipeline){
            delegate.registerPipeline(name,writePipeline);
        }

        @Override
        public void deregisterPipeline(String partitionName){
            delegate.deregisterPipeline(partitionName);
        }
    }
}
