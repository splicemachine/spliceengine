/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.access.util;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionInfoCache;
import org.spark_project.guava.cache.Cache;
import org.spark_project.guava.cache.CacheBuilder;
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
