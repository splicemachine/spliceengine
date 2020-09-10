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

package com.splicemachine.storage;

import splice.com.google.common.cache.Cache;
import splice.com.google.common.cache.CacheBuilder;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.primitives.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created by jleach on 2/19/16.
 */
public class MPartitionCache implements PartitionInfoCache<String> {
    private Cache<String, List<Partition>> partitionCache = CacheBuilder.newBuilder().maximumSize(100).build();

        //must be a no-args to support the PartitionCacheService--use configure() instead
        public MPartitionCache(){ }

        @Override
        public void invalidate(String tableName) throws IOException {
            partitionCache.invalidate(tableName);
        }

        @Override
        public void invalidate(byte[] tableName) throws IOException{
            invalidate(Bytes.toString(tableName));
        }

        @Override
        public void configure(SConfiguration configuration){
        }

        @Override
        public List<Partition> getIfPresent(String tableName) throws IOException {
            return partitionCache.getIfPresent(tableName);
        }

        @Override
        public void put(String tableName, List<Partition> partitions) throws IOException {
            partitionCache.put(tableName,partitions);
        }

    @Override
    public void invalidateAdapter(String s) throws IOException {
        throw new UnsupportedOperationException("Adapter operations not supported in mem platform");
    }

    @Override
    public void invalidateAdapter(byte[] tableName) throws IOException {
            // no-op
    }

    @Override
    public List<Partition> getAdapterIfPresent(String s) throws IOException {
        throw new UnsupportedOperationException("Adapter operations not supported in mem platform");
    }

    @Override
    public void putAdapter(String s, List<Partition> partitions) throws IOException {
        throw new UnsupportedOperationException("Adapter operations not supported in mem platform");
    }
}
