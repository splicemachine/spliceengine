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

package com.splicemachine.storage;

import org.spark_project.guava.cache.Cache;
import org.spark_project.guava.cache.CacheBuilder;
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
    }