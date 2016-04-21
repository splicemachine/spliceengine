package com.splicemachine.pipeline.contextfactory;


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


    public static <TableInfo> WriteContextFactory<TransactionalRegion> getWriteContext(long conglomerateId,
                                                                           SConfiguration config,
                                                                           PartitionFactory<TableInfo> basePartitionFactory,
                                                                           PipelineExceptionFactory pipelineExceptionFactory,
                                                                           Function<TableInfo,String> tableParser,
                                                                           ContextFactoryLoader factoryLoader) {
            LocalWriteContextFactory<TableInfo> localWriteContextFactory;
            if(conglomerateId==-1L){
                localWriteContextFactory = LocalWriteContextFactory.unmanagedContextFactory(basePartitionFactory,pipelineExceptionFactory,tableParser);
            }else{
                localWriteContextFactory=new LocalWriteContextFactory<>(conglomerateId,
                        config.getStartupLockWaitPeriod(),
                        basePartitionFactory,
                        pipelineExceptionFactory,
                        tableParser,
                        factoryLoader);

            }
        return localWriteContextFactory;

    }

}
