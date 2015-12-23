package com.splicemachine.pipeline.mem;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.ServerControl;
import com.splicemachine.pipeline.api.BulkWriter;
import com.splicemachine.pipeline.api.BulkWriterFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;
import com.splicemachine.si.api.server.TransactionalRegionFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
@ThreadSafe
public class DirectBulkWriterFactory implements BulkWriterFactory{
    private final PartitionFactory<Object> partitionFactory;
    private final SConfiguration config;
    private final PipelineExceptionFactory exceptionFactory;
    private final Map<Long,ContextFactoryLoader> factoryLoaderMap;
    private final TransactionalRegionFactory txnRegionFactory;
    private final ServerControl serverControl;
    private WriteCoordinator writeCoordinator;

    public DirectBulkWriterFactory(PartitionFactory<Object> partitionFactory,
                                   SConfiguration config,
                                   PipelineExceptionFactory exceptionFactory,
                                   Map<Long,ContextFactoryLoader> factoryLoaderMap,
                                   TransactionalRegionFactory txnRegionFactory,
                                   ServerControl serverControl){
        this.partitionFactory=partitionFactory;
        this.config=config;
        this.exceptionFactory=exceptionFactory;
        this.factoryLoaderMap = factoryLoaderMap;
        this.txnRegionFactory=txnRegionFactory;
        this.serverControl=serverControl;
    }

    public void setWriteCoordinator(WriteCoordinator wc){
        this.writeCoordinator = wc;
    }

    @Override
    public BulkWriter newWriter(){
        return new DirectBulkWriter(partitionFactory,config,
                exceptionFactory,
                factoryLoaderMap,
                txnRegionFactory,
                serverControl,
                writeCoordinator);
    }

    @Override
    public void invalidateCache(byte[] tableName){

    }
}
