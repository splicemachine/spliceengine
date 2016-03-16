package com.splicemachine.storage;

import org.sparkproject.guava.cache.Cache;
import org.sparkproject.guava.cache.CacheBuilder;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.primitives.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created by jleach on 2/19/16.
 */
public class MPartitionCache implements PartitionInfoCache<String> {
        private SConfiguration configuration;
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
            this.configuration=configuration;
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