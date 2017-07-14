package com.splicemachine.db.impl.sql.catalog;


import org.spark_project.guava.cache.Cache;

import java.util.List;

public class TotalManagedCache<K, V> implements ManagedCacheMBean<K, V>{

    private final List<ManagedCache<K,V>> managedCache;

    public TotalManagedCache(List<ManagedCache<K, V>> managedCache){
        this.managedCache = managedCache;
    }
    @Override public long getSize(){
        long size = 0;
        for(ManagedCache<K, V> mc : managedCache){
            size += mc.getSize();
        }
        return size;
    }
    @Override public long getHitCount(){
        long hitCount = 0;
        for(ManagedCache<K, V> mc : managedCache){
            hitCount += mc.getHitCount();
        }
        return hitCount;
    }
    @Override public long getMissCount(){
        long missCount = 0;
        for(ManagedCache<K, V> mc : managedCache){
            missCount += mc.getMissCount();
        }
        return missCount;
    }
    @Override public double getHitRate(){
        double hitRate = 0;
        for(ManagedCache<K, V> mc : managedCache){
            hitRate += mc.getHitRate();
        }
        return hitRate/managedCache.size();
    }
    @Override public double getMissRate(){
        double missRate = 0;
        for(ManagedCache<K, V> mc : managedCache){
            missRate += mc.getMissRate();
        }
        return missRate/managedCache.size();
    }
    @Override public void invalidateAll(){
        for(ManagedCache<K, V> mc : managedCache){
            mc.invalidateAll();
        }
    }
    @Override public void put(K var1, V var2){
        throw new UnsupportedOperationException();
    }
    @Override public V getIfPresent(K k) {
        throw new UnsupportedOperationException();
    }
    @Override public void invalidate(K k) {
        throw new UnsupportedOperationException();
    }

}
