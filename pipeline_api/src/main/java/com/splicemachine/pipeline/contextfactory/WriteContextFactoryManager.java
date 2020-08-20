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
 */

package com.splicemachine.pipeline.contextfactory;


import splice.com.google.common.base.Function;

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
