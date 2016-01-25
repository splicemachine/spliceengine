package com.splicemachine.si.data.hbase.coprocessor;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.storage.PartitionInfoCache;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public class PartitionCacheService{
    private static volatile PartitionInfoCache INSTANCE;

    public static PartitionInfoCache loadPartitionCache(SConfiguration configuration){
        PartitionInfoCache cache = INSTANCE;
        if(cache==null){
            synchronized(PartitionCacheService.class){
                cache = INSTANCE;
                if(cache==null){
                    cache = INSTANCE = loadCache(configuration);
                }
            }
        }
        return cache;
    }

    private static PartitionInfoCache loadCache(SConfiguration config) {
        ServiceLoader<PartitionInfoCache> serviceLoader = ServiceLoader.load(PartitionInfoCache.class);
        Iterator<PartitionInfoCache> iter = serviceLoader.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No PartitionCache found!");

        PartitionInfoCache next=iter.next();
        next.configure(config);
        return next;
    }
}
