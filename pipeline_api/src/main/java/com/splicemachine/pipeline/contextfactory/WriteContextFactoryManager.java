package com.splicemachine.pipeline.contextfactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Function;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.si.api.server.TransactionalRegion;

/**
 * Creates and caches a WriteContextFactory for each conglomerateId.
 *
 * @author Scott Fines
 *         Date: 11/13/14
 */
public class WriteContextFactoryManager {

    protected static final ConcurrentMap<Long, DiscardingWriteContextFactory<TransactionalRegion>> ctxMap = new ConcurrentHashMap<>();


    public static <TableInfo> WriteContextFactory<TransactionalRegion> getWriteContext(long conglomerateId,
                                                                           SConfiguration config,
                                                                           PartitionFactory<TableInfo> basePartitionFactory,
                                                                           PipelineExceptionFactory pipelineExceptionFactory,
                                                                           Function<TableInfo,String> tableParser,
                                                                           ContextFactoryLoader factoryLoader) {
        DiscardingWriteContextFactory<TransactionalRegion> ctxFactory = ctxMap.get(conglomerateId);

        if (ctxFactory == null) {
            LocalWriteContextFactory<TableInfo> localWriteContextFactory;
            if(conglomerateId==-1L){
                localWriteContextFactory = LocalWriteContextFactory.unmanagedContextFactory(basePartitionFactory,pipelineExceptionFactory,tableParser);
                ctxFactory =new DiscardingWriteContextFactory<>(conglomerateId,localWriteContextFactory);
                ctxMap.put(conglomerateId,ctxFactory);
            }else{
                localWriteContextFactory=new LocalWriteContextFactory<>(conglomerateId,
                        config.getStartupLockWaitPeriod(),
                        basePartitionFactory,
                        pipelineExceptionFactory,
                        tableParser,
                        factoryLoader);
                DiscardingWriteContextFactory<TransactionalRegion> newFactory=new DiscardingWriteContextFactory<>(conglomerateId,localWriteContextFactory);

                DiscardingWriteContextFactory<TransactionalRegion> oldFactory=ctxMap.putIfAbsent(conglomerateId,newFactory);
                ctxFactory=(oldFactory!=null)?oldFactory:newFactory;

            }
        }
        ctxFactory.incrementRefCount();

        return ctxFactory;
    }

    protected static void remove(long conglomerateId, DiscardingWriteContextFactory value) {
        ctxMap.remove(conglomerateId, value);
    }

}
