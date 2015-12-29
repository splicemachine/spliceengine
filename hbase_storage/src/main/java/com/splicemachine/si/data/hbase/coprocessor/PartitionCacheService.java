package com.splicemachine.si.data.hbase.coprocessor;

import com.splicemachine.storage.PartitionInfoCache;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public class PartitionCacheService{
    private static volatile PartitionInfoCache INSTANCE;

    public static PartitionInfoCache loadPartitionCache(){
        PartitionInfoCache cache = INSTANCE;
        if(cache==null){
            synchronized(PartitionCacheService.class){
                cache = INSTANCE;
                if(cache==null){
                    cache = INSTANCE = loadCache();
                }
            }
        }
        return cache;
    }

    private static PartitionInfoCache loadCache(){
        ServiceLoader<PartitionInfoCache> serviceLoader = ServiceLoader.load(PartitionInfoCache.class);
        Iterator<PartitionInfoCache> iter = serviceLoader.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No PartitionCache found!");

        return iter.next();
    }
}
