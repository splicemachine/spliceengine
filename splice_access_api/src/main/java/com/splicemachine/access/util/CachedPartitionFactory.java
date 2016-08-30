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

package com.splicemachine.access.util;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionInfoCache;
import org.sparkproject.guava.cache.Cache;
import org.sparkproject.guava.cache.CacheBuilder;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.Collection;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
@NotThreadSafe
public abstract class CachedPartitionFactory<TableInfo> implements PartitionFactory<TableInfo>{
    private final Cache<String,Partition> tableCache = CacheBuilder.newBuilder().maximumSize(1000).build(); // Jl TODO Constants
    private final PartitionFactory<TableInfo> delegate;

    public CachedPartitionFactory(PartitionFactory<TableInfo> delegate){
        this.delegate=delegate;
    }

    @Override
    public PartitionAdmin getAdmin() throws IOException{
        return delegate.getAdmin();
    }

    @Override
    public Partition getTable(TableInfo tableName) throws IOException{
        return getTable(infoAsString(tableName));
    }

    @Override
    public void initialize(Clock clock,SConfiguration configuration, PartitionInfoCache partitionInfoCache) throws IOException{
       delegate.initialize(clock,configuration,partitionInfoCache);
    }

    @Override
    public Partition getTable(String name) throws IOException{
        Partition p = tableCache.getIfPresent(name);
        if(p==null){
            p = delegate.getTable(name);
            tableCache.put(name,p);
        }
        return p;
    }

    @Override
    public Partition getTable(byte[] name) throws IOException{
        return getTable(Bytes.toString(name));
    }

    public Collection<Partition> cachedPartitions(){
        return tableCache.asMap().values();
    }

    protected abstract String infoAsString(TableInfo tableName);
}
