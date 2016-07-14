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

package com.splicemachine.derby.lifecycle;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionInfoCache;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public class MPipelinePartitionFactory implements PartitionFactory<Object>{
    private final PartitionFactory<Object> baseFactory;

    public MPipelinePartitionFactory(PartitionFactory<Object> baseFactory){
        this.baseFactory=baseFactory;
    }

    public void initialize(Clock clock,SConfiguration configuration,PartitionInfoCache partitionInfoCache) throws IOException{
        baseFactory.initialize(clock,configuration,partitionInfoCache);
    }

    public Partition getTable(String name) throws IOException{
        return baseFactory.getTable(name);
    }

    public PartitionAdmin getAdmin() throws IOException{
        return new MEnginePartitionAdmin(baseFactory.getAdmin());
    }

    public Partition getTable(Object tableName) throws IOException{
        return baseFactory.getTable(tableName);
    }

    public Partition getTable(byte[] name) throws IOException{
        return baseFactory.getTable(name);
    }
}
