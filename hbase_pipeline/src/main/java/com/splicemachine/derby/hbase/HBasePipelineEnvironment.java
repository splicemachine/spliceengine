package com.splicemachine.derby.hbase;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.pipeline.PipelineConfiguration;
import com.splicemachine.pipeline.api.BulkWriterFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.PipelineMeter;
import com.splicemachine.pipeline.client.RpcChannelFactory;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.PipelineEnvironment;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.pipeline.utils.SimplePipelineCompressor;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.txn.KeepAliveScheduler;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.driver.SIEnvironment;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
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
    private final PipelineMeter meter = new YammerPipelineMeter();

    public static HBasePipelineEnvironment loadEnvironment(ContextFactoryDriver ctxFactoryLoader){
        HBasePipelineEnvironment env = INSTANCE;
        if(env==null){
            synchronized(HBasePipelineEnvironment.class){
                env = INSTANCE;
                if(env==null){
                    SIEnvironment siEnv =HBaseSIEnvironment.loadEnvironment(ZkUtils.getRecoverableZooKeeper());
                    env= INSTANCE = new HBasePipelineEnvironment(siEnv,ctxFactoryLoader,HPipelineExceptionFactory.INSTANCE);
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
        pipelineConfiguration.addDefaults(PipelineConfiguration.defaults);

        KryoPool kryoPool=new KryoPool(pipelineConfiguration.getInt(PipelineConfiguration.PIPELINE_KRYO_POOL_SIZE));
        kryoPool.setKryoRegistry(new PipelineKryoRegistry());
        //TODO -sf- enable snappy compression here
        this.compressor = new SimplePipelineCompressor(kryoPool,env.getSIDriver().getOperationFactory());

        RpcChannelFactory channelFactory = ChannelFactoryService.loadChannelFactory();
        this.writerFactory = new CoprocessorWriterFactory(compressor,partitionInfoCache(),pipelineExceptionFactory,channelFactory);
    }

    @Override public PartitionFactory tableFactory(){ return delegate.tableFactory(); }
    @Override public ExceptionFactory exceptionFactory(){ return delegate.exceptionFactory(); }
    @Override public SDataLib dataLib(){ return delegate.dataLib(); }
    @Override public TxnStore txnStore(){ return delegate.txnStore(); }
    @Override public OperationStatusFactory statusFactory(){ return delegate.statusFactory(); }
    @Override public TimestampSource timestampSource(){ return delegate.timestampSource(); }
    @Override public TxnSupplier txnSupplier(){ return delegate.txnSupplier(); }
    @Override public IgnoreTxnCacheSupplier ignoreTxnSupplier(){ return delegate.ignoreTxnSupplier(); }
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
}
