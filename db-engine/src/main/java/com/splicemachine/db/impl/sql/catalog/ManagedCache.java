package com.splicemachine.db.impl.sql.catalog;


public class ManagedCache<K, V> implements ManagedCacheMBean<K, V>{

    private final org.spark_project.guava.cache.Cache<K,V> managedCache;

    public ManagedCache(org.spark_project.guava.cache.Cache managedCache){
        this.managedCache = managedCache;
    }
    @Override public long getSize(){ return managedCache.size(); }
    @Override public long getHitCount(){ return managedCache.stats().hitCount(); }
    @Override public long getMissCount(){ return managedCache.stats().missCount(); }
    @Override public double getHitRate(){ return managedCache.stats().hitRate(); }
    @Override public double getMissRate(){ return managedCache.stats().missRate(); }
    @Override public void invalidateAll(){ managedCache.invalidateAll(); }
    @Override public void put(K var1, V var2){ managedCache.put(var1, var2); }
    @Override public V getIfPresent(K k) { return managedCache.getIfPresent(k);}
    @Override public void invalidate(K k) { managedCache.invalidate(k);}

}
