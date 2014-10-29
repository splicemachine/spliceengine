package com.splicemachine.hbase.regioninfocache;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * For JMX
 */
class HBaseRegionCacheStatus implements RegionCacheStatus {

    private AtomicLong cacheUpdatedTimestamp;
    private CacheRefreshRunnable cacheRefreshRunnable;
    private Map<byte[], SortedSet<Pair<HRegionInfo, ServerName>>> regionCache;
    private long cacheUpdatePeriod;

    HBaseRegionCacheStatus(AtomicLong cacheUpdatedTimestamp,
                           CacheRefreshRunnable cacheRefreshRunnable,
                           Map<byte[], SortedSet<Pair<HRegionInfo, ServerName>>> regionCache,
                           long cacheUpdatePeriod) {
        this.cacheUpdatedTimestamp = cacheUpdatedTimestamp;
        this.cacheRefreshRunnable = cacheRefreshRunnable;
        this.regionCache = regionCache;
        this.cacheUpdatePeriod = cacheUpdatePeriod;
    }

    @Override
    public long getLastUpdatedTimestamp() {
        return cacheUpdatedTimestamp.get();
    }

    @Override
    public void updateCache() {
        cacheRefreshRunnable.run();
    }

    @Override
    public int getNumCachedRegions(String tableName) {
        SortedSet<Pair<HRegionInfo, ServerName>> regions = regionCache.get(tableName.getBytes());
        if (regions == null) {
            return 0;
        }
        return regions.size();
    }

    @Override
    public long getNumCachedTables() {
        return regionCache.size();
    }

    @Override
    public long getCacheUpdatePeriod() {
        return cacheUpdatePeriod;
    }
}
