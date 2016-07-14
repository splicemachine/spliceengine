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
