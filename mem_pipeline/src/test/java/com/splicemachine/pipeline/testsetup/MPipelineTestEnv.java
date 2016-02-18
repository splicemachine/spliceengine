package com.splicemachine.pipeline.testsetup;

import com.google.common.base.Function;
import com.splicemachine.MapConfiguration;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.util.SkeletonDefaults;
import com.splicemachine.pipeline.MappedPipelineFactory;
import com.splicemachine.pipeline.PartitionWritePipeline;
import com.splicemachine.pipeline.PipelineConfiguration;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.Writer;
import com.splicemachine.pipeline.client.Monitor;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.context.NoOpPipelineMeter;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;
import com.splicemachine.pipeline.contextfactory.WriteContextFactory;
import com.splicemachine.pipeline.contextfactory.WriteContextFactoryManager;
import com.splicemachine.pipeline.mem.DirectBulkWriterFactory;
import com.splicemachine.pipeline.mem.DirectPipelineExceptionFactory;
import com.splicemachine.pipeline.ManualContextFactoryLoader;
import com.splicemachine.pipeline.traffic.AtomicSpliceWriteControl;
import com.splicemachine.pipeline.writer.SynchronousBucketingWriter;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.MemSITestEnv;
import com.splicemachine.si.api.server.TransactionalRegionFactory;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.si.impl.server.SITransactor;
import com.splicemachine.storage.MServerControl;
import com.splicemachine.storage.Partition;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class MPipelineTestEnv extends MemSITestEnv implements PipelineTestEnv{
    private final WriteCoordinator writeCoordinator;
    private final Map<Long,ContextFactoryLoader> contextFactoryLoaderMap = new ConcurrentHashMap<>();
    private final MappedPipelineFactory pipelineFactory;
    private final MapConfiguration config;
    private final TransactionalRegionFactory trf;

    @SuppressWarnings("unchecked")
    public MPipelineTestEnv() throws IOException{
        this.config = new MapConfiguration();
        config.addDefaults(new SkeletonDefaults(){
            @Override
            public long defaultLongFor(String key){
                switch(key){
                    case PipelineConfiguration.STARTUP_LOCK_WAIT_PERIOD: return Long.MAX_VALUE;
                }
                return super.defaultLongFor(key);
            }

            @Override
            public boolean hasLongDefault(String key){
                return PipelineConfiguration.STARTUP_LOCK_WAIT_PERIOD.equals(key);
            }
        });

         trf = buildTransactionalRegionFactory();
        pipelineFactory = new MappedPipelineFactory();
        DirectBulkWriterFactory bwf = new DirectBulkWriterFactory(pipelineFactory,
                new AtomicSpliceWriteControl(Integer.MAX_VALUE,Integer.MAX_VALUE,Integer.MAX_VALUE,Integer.MAX_VALUE),
                DirectPipelineExceptionFactory.INSTANCE);
        Writer writer = new SynchronousBucketingWriter(bwf,DirectPipelineExceptionFactory.INSTANCE,
                getTableFactory());
        Monitor monitor = new Monitor(Long.MAX_VALUE,Integer.MAX_VALUE,0,10l,Integer.MAX_VALUE);
        writeCoordinator = new WriteCoordinator(writer,writer,monitor,getTableFactory(),
                DirectPipelineExceptionFactory.INSTANCE,null);
        bwf.setWriteCoordinator(writeCoordinator);
    }

    @Override
    public void initialize() throws IOException{
        createTransactionalTable(Bytes.toBytes(Long.toString(1292)));
        personPartition = getTableFactory().getTable(Long.toString(1292));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void createTransactionalTable(byte[] tableNameBytes) throws IOException{
        super.createTransactionalTable(tableNameBytes);
        PartitionFactory tableFactory=getTableFactory();
        Partition table=tableFactory.getTable(tableNameBytes);
        long conglomerateId=Long.parseLong(Bytes.toString(tableNameBytes));
        WriteContextFactory wcf =WriteContextFactoryManager.getWriteContext(conglomerateId,
                config,tableFactory,pipelineExceptionFactory(),new Function<Object,String>(){
                    @Override
                    public String apply(Object input){
                        return (String)input;
                    }
                },contextFactoryLoader(conglomerateId));
        wcf.prepare();
        PartitionWritePipeline pwp = new PartitionWritePipeline(MServerControl.INSTANCE,table,
                wcf,
                trf.newRegion(table),
                NoOpPipelineMeter.INSTANCE,pipelineExceptionFactory());
        pipelineFactory.registerPipeline(Bytes.toString(tableNameBytes),pwp);
    }

    @Override
    public WriteCoordinator writeCoordinator(){
        return writeCoordinator;
    }

    @Override
    public ContextFactoryLoader contextFactoryLoader(long conglomerateId){
        ContextFactoryLoader contextFactoryLoader=contextFactoryLoaderMap.get(conglomerateId);
        if(contextFactoryLoader==null){
            contextFactoryLoader = new ManualContextFactoryLoader();
            contextFactoryLoaderMap.put(conglomerateId,contextFactoryLoader);
        }

        return contextFactoryLoader;
    }

    @Override
    public PipelineExceptionFactory pipelineExceptionFactory(){
        return DirectPipelineExceptionFactory.INSTANCE;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    @SuppressWarnings("unchecked")
    private TransactionalRegionFactory buildTransactionalRegionFactory(){
        SITransactor transactor = new SITransactor(getTxnStore(),
                getOperationFactory(),getBaseOperationFactory(),getOperationStatusFactory(),getExceptionFactory());
        return new TransactionalRegionFactory(this.getTxnStore(),
                transactor,getOperationFactory(),NoopRollForward.INSTANCE,NoOpReadResolver.INSTANCE);
    }
}
