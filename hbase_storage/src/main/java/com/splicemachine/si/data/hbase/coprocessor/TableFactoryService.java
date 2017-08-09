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

package com.splicemachine.si.data.hbase.coprocessor;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.storage.PartitionInfoCache;
import org.apache.hadoop.hbase.TableName;
import java.io.IOException;
import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public class TableFactoryService{
    @SuppressWarnings("unchecked")
    public static PartitionFactory<TableName> loadTableFactory(Clock clock, SConfiguration configuration, PartitionInfoCache partitionCache) throws IOException{
        ServiceLoader<PartitionFactory> serviceLoader = ServiceLoader.load(PartitionFactory.class);
        Iterator<PartitionFactory> iter = serviceLoader.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No TableFactory found!");

        PartitionFactory stf = iter.next();
        stf.initialize(clock,configuration,partitionCache);
        return (PartitionFactory<TableName>)stf;
    }

}
