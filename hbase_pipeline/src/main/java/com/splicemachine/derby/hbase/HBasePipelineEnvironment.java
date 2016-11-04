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

package com.splicemachine.derby.hbase;

import java.io.IOException;
import java.net.URISyntaxException;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.SnowflakeFactory;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.pipeline.MappedPipelineFactory;
import com.splicemachine.pipeline.PartitionWritePipeline;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.PipelineEnvironment;
import com.splicemachine.pipeline.api.BulkWriterFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.PipelineMeter;
import com.splicemachine.pipeline.api.WritePipelineFactory;
import com.splicemachine.pipeline.client.RpcChannelFactory;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.pipeline.utils.SimplePipelineCompressor;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.readresolve.KeyedReadResolver;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.txn.KeepAliveScheduler;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.driver.SIEnvironment;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.PartitionInfoCache;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.utils.kryo.KryoPool;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public class HBasePipelineEnvironment implements PipelineEnvironment{
    private static volatile HBasePipelineEnvironment INSTANCE;

    private final SIEnvironment delegate;
    private final PipelineExceptionFactory pipelineExceptionFactory;
    private final ContextFactoryDriver contextFactoryLoader;
    private final SConfiguration pipelineConfiguration;
    private final PipelineCompressor compressor;
    private final BulkWriterFactory writerFactory;
    private final PipelineMeter meter = new CountingPipelineMeter();
    private final WritePipelineFactory pipelineFactory;

    public static HBasePipelineEnvironment loadEnvironment(Clock systemClock,ContextFactoryDriver ctxFactoryLoader) throws IOException{
        HBasePipelineEnvironment env = INSTANCE;
        if(env==null){
            synchronized(HBasePipelineEnvironment.class){
                env = INSTANCE;
                if(env==null){
                    SIEnvironment siEnv =HBaseSIEnvironment.loadEnvironment(systemClock,ZkUtils.getRecoverableZooKeeper());
                    env = INSTANCE = new HBasePipelineEnvironment(siEnv,ctxFactoryLoader,HPipelineExceptionFactory.INSTANCE);
                    PipelineDriver.loadDriver(env);
                }
            }
        }
        return env;
    }

    private HBasePipelineEnvironment(SIEnvironment env,
                                     ContextFactoryDriver ctxFactoryLoader,
                                     PipelineExceptionFactory pef){
        this.delegate = env;
        this.pipelineExceptionFactory = pef;
        this.contextFactoryLoader = ctxFactoryLoader;
        this.pipelineConfiguration = env.configuration();
        this.pipelineFactory = new AvailablePipelineFactory();

        KryoPool kryoPool=new KryoPool(pipelineConfiguration.getPipelineKryoPoolSize());
        kryoPool.setKryoRegistry(new PipelineKryoRegistry());
        //TODO -sf- enable snappy compression here
        this.compressor = new SimplePipelineCompressor(kryoPool,env.getSIDriver().getOperationFactory());

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

    @Override public TxnStore txnStore(){ return delegate.txnStore(); }
    @Override public OperationStatusFactory statusFactory(){ return delegate.statusFactory(); }
    @Override public TimestampSource timestampSource(){ return delegate.timestampSource(); }
    @Override public TxnSupplier txnSupplier(){ return delegate.txnSupplier(); }
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

    @Override
    public DistributedFileSystem fileSystem(){
        return delegate.fileSystem();
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
