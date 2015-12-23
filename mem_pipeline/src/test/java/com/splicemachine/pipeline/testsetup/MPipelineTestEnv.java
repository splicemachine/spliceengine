package com.splicemachine.pipeline.testsetup;

import com.splicemachine.MapConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.pipeline.PipelineConfiguration;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.Writer;
import com.splicemachine.pipeline.client.Monitor;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;
import com.splicemachine.pipeline.contextfactory.UnmanagedFactoryLoader;
import com.splicemachine.pipeline.mem.DirectBulkWriterFactory;
import com.splicemachine.pipeline.mem.DirectPipelineExceptionFactory;
import com.splicemachine.pipeline.mem.MContextFactoryLoader;
import com.splicemachine.pipeline.writer.SynchronousBucketingWriter;
import com.splicemachine.si.MemSITestEnv;
import com.splicemachine.si.api.server.TransactionalRegionFactory;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.si.impl.server.SITransactor;
import com.splicemachine.storage.MServerControl;

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

    @SuppressWarnings("unchecked")
    public MPipelineTestEnv() throws IOException{
        MapConfiguration config = new MapConfiguration();
        config.setDefault(PipelineConfiguration.STARTUP_LOCK_WAIT_PERIOD,Long.MAX_VALUE);

        TransactionalRegionFactory trf = buildTransactionalRegionFactory();
        DirectBulkWriterFactory bwf = new DirectBulkWriterFactory(getTableFactory(),
                config,
                DirectPipelineExceptionFactory.INSTANCE,
                contextFactoryLoaderMap,
                trf,
                MServerControl.INSTANCE);
        Writer writer = new SynchronousBucketingWriter(bwf,DirectPipelineExceptionFactory.INSTANCE,
                getTableFactory());
        Monitor monitor = new Monitor(Long.MAX_VALUE,Integer.MAX_VALUE,0,10l,Integer.MAX_VALUE);
        writeCoordinator = new WriteCoordinator(writer,writer,monitor,getTableFactory(),
                DirectPipelineExceptionFactory.INSTANCE);
        bwf.setWriteCoordinator(writeCoordinator);
    }

    @SuppressWarnings("unchecked")
    private TransactionalRegionFactory buildTransactionalRegionFactory(){
        DataStore ds = new DataStore(getDataLib(),
                SIConstants.SI_NEEDED,
                SIConstants.SI_DELETE_PUT,
                SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,
                SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,
                SIConstants.EMPTY_BYTE_ARRAY,
                SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES,
                SIConstants.DEFAULT_FAMILY_BYTES);
        SITransactor transactor = new SITransactor(getTxnStore(),getIgnoreTxnStore(),
                getOperationFactory(),ds,getOperationStatusFactory(),getExceptionFactory());
        return new TransactionalRegionFactory(this.getTxnStore(),
                getIgnoreTxnStore(),ds,transactor,getOperationFactory(),NoopRollForward.INSTANCE,NoOpReadResolver.INSTANCE);
    }

    @Override
    public WriteCoordinator writeCoordinator(){
        return writeCoordinator;
    }

    @Override
    public ContextFactoryLoader contextFactoryLoader(long conglomerateId){
        ContextFactoryLoader contextFactoryLoader=contextFactoryLoaderMap.get(conglomerateId);
        if(contextFactoryLoader==null){
            contextFactoryLoader = new MContextFactoryLoader();
            contextFactoryLoaderMap.put(conglomerateId,contextFactoryLoader);
        }

        return contextFactoryLoader;
    }

    @Override
    public PipelineExceptionFactory pipelineExceptionFactory(){
        return DirectPipelineExceptionFactory.INSTANCE;
    }
}
