package com.splicemachine.hbase.regioninfocache;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.splicemachine.concurrent.MoreExecutors;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.RegionCacheComparator;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import javax.management.*;
import java.util.Collections;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Makes HRegionInfo objects available in memory. Refresh rate configured in SpliceConstants.
 *
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class HBaseRegionCache implements RegionCache {

    protected static final Logger LOG = Logger.getLogger(HBaseRegionCache.class);

    private static final RegionCache INSTANCE = new HBaseRegionCache(
            SpliceConstants.cacheUpdatePeriod,
            MoreExecutors.namedSingleThreadScheduledExecutor("region-cache-thread-%d"));

    private static final RegionCacheComparator REGION_CACHE_COMPARATOR = new RegionCacheComparator();

    private static final SortedSet<Pair<HRegionInfo, ServerName>> EMPTY_REGION_PAIR_SET =
            Collections.unmodifiableSortedSet(Sets.<Pair<HRegionInfo, ServerName>>newTreeSet(REGION_CACHE_COMPARATOR));

    private final ConcurrentSkipListMap<byte[], SortedSet<Pair<HRegionInfo, ServerName>>> regionCache;
    private final ScheduledExecutorService cacheUpdater;
    private final long cacheUpdatePeriod;
    private final AtomicLong cacheUpdatedTimestamp = new AtomicLong();
    private CacheRefreshRunnable cacheRefreshRunnable;
    private final RegionCacheStatus status;

    public static RegionCache getInstance() {
        return INSTANCE;
    }

    private HBaseRegionCache(long cacheUpdatePeriod, ScheduledExecutorService cacheUpdater) {
        this.regionCache = new ConcurrentSkipListMap<byte[], SortedSet<Pair<HRegionInfo, ServerName>>>(Bytes.BYTES_COMPARATOR);
        this.cacheUpdater = cacheUpdater;
        this.cacheUpdatePeriod = cacheUpdatePeriod;
        this.status = new HBaseRegionCacheStatus(cacheUpdatedTimestamp, cacheRefreshRunnable, regionCache, cacheUpdatePeriod);
    }

    @Override
    public void start() {
        Preconditions.checkState(cacheRefreshRunnable == null, "Only expected start to be called once!");
        cacheRefreshRunnable = new CacheRefreshRunnable(regionCache, cacheUpdatedTimestamp, null);
        cacheUpdater.scheduleAtFixedRate(cacheRefreshRunnable, 0l, cacheUpdatePeriod, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        cacheUpdater.shutdownNow();
    }

    @Override
    public SortedSet<Pair<HRegionInfo, ServerName>> getRegions(byte[] tableName) {
        SortedSet<Pair<HRegionInfo, ServerName>> regions = regionCache.get(tableName);

        if (regions == null) {

            /* Refresh just this one table on calling thread */
            new CacheRefreshRunnable(regionCache, cacheUpdatedTimestamp, tableName).run();

            /* Most of the time this will return regions that were loaded by the line above. However if there were no
             * region rows in META for the requested table place an empty set in the cache so that we only attempt
             * to scan for this table on the next full cache refresh, not every time this method is called. */
            regions = regionCache.putIfAbsent(tableName, EMPTY_REGION_PAIR_SET);
        }

        /* The contract of this method is that we return an empty set, rather than null, if there is no region info available. */
        return regions != null ? regions : EMPTY_REGION_PAIR_SET;
    }

    @Override

    public SortedSet<Pair<HRegionInfo, ServerName>> getRegionsInRange(byte[] tableName, byte[] startRow, byte[] stopRow) {
        SortedSet<Pair<HRegionInfo, ServerName>> regions = getRegions(tableName);
        if (startRow.length <= 0 && stopRow.length <= 0) {
            //short circuit in the case where all regions are contained
            return regions;
        }
        SortedSet<Pair<HRegionInfo, ServerName>> containedRegions = Sets.newTreeSet(REGION_CACHE_COMPARATOR);
        if (Bytes.equals(startRow, stopRow)) {
            for (Pair<HRegionInfo, ServerName> info : regions) {
                if (info.getFirst().containsRow(startRow)) {
                    containedRegions.add(info);
                    return containedRegions;
                }
            }
        }
        for (Pair<HRegionInfo, ServerName> info : regions) {
            if (HRegionUtil.containsRange(info.getFirst(), startRow, stopRow))
                containedRegions.add(info);
        }
        return containedRegions;
    }

    @Override
    public void invalidate(byte[] tableName) {
        SpliceLogUtils.debug(LOG, "invalidating region cache for table = %s ", Bytes.toString(tableName));
        regionCache.remove(tableName);
    }

    @Override
    public long size() {
        return regionCache.size();
    }

    @Override
    public long getUpdateTimestamp() {
        return cacheUpdatedTimestamp.get();
    }

    @Override
    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        ObjectName cacheName = new ObjectName("com.splicemachine.region:type=RegionCacheStatus");
        mbs.registerMBean(status, cacheName);
    }

}
