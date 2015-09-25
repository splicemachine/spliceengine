package com.splicemachine.hbase.regioninfocache;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.splicemachine.concurrent.LongStripedSynchronizer;
import com.splicemachine.concurrent.MoreExecutors;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.RegionCacheComparator;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.regionserver.BaseHRegionUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import javax.management.*;
import java.io.IOException;
import java.util.Collections;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

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

    // private but passed to CacheRefreshRunnable and HBaseRegionCacheStatus
    private final ConcurrentSkipListMap<byte[], SortedSet<Pair<HRegionInfo, ServerName>>> regionCache;
    
    private final ScheduledExecutorService cacheUpdater;
    private final long cacheUpdatePeriod;
    private final AtomicLong cacheUpdatedTimestamp = new AtomicLong();
    private final HConnection connection;
    private CacheRefreshRunnable cacheRefreshRunnable;
    private final RegionCacheStatus status;
    private final LongStripedSynchronizer<Lock> lockStriper = LongStripedSynchronizer.stripedLock(64);

    public static RegionCache getInstance() {
        return INSTANCE;
    }

    private HBaseRegionCache(long cacheUpdatePeriod, ScheduledExecutorService cacheUpdater) {
        this.regionCache = new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);
        this.cacheUpdater = cacheUpdater;
        this.cacheUpdatePeriod = cacheUpdatePeriod;
        this.status = new HBaseRegionCacheStatus(cacheUpdatedTimestamp, cacheRefreshRunnable, regionCache, cacheUpdatePeriod);
        try {
            this.connection = HConnectionManager.getConnection(SpliceConstants.config);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start() {
        Preconditions.checkState(cacheRefreshRunnable == null, "Only expected start to be called once!");
        cacheRefreshRunnable = new CacheRefreshRunnable(regionCache, connection,cacheUpdatedTimestamp, null);
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

            Lock lock = lockStriper.get(Bytes.mapKey(tableName));
            lock.lock();
            try {
                regions = regionCache.get(tableName);
                if(regions!=null) return regions;
            /* Refresh just this one table on calling thread */
                new CacheRefreshRunnable(regionCache, connection, cacheUpdatedTimestamp, tableName).run();

            /* Most of the time this will return regions that were loaded by the line above. However if there were no
             * region rows in META for the requested table place an empty set in the cache so that we only attempt
             * to scan for this table on the next full cache refresh, not every time this method is called. */
                regions = regionCache.putIfAbsent(tableName, EMPTY_REGION_PAIR_SET);
            }finally{
                lock.unlock();
            }
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
            if (BaseHRegionUtil.containsRange(info.getFirst(), startRow, stopRow))
                containedRegions.add(info);
        }
        return containedRegions;
    }

    @Override
    public void invalidate(byte[] tableNameBytes) {
        SpliceLogUtils.debug(LOG, "Invalidating region cache for table = %s ", Bytes.toString(tableNameBytes));
        regionCache.remove(tableNameBytes);
        connection.clearRegionCache(tableNameBytes);
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
