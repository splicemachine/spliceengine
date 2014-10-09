package com.splicemachine.hbase;

import com.google.common.cache.CacheLoader;
import com.google.common.collect.Sets;
import com.splicemachine.concurrent.MoreExecutors;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import javax.management.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.SortedSet;
import java.util.concurrent.*;

/**
 * Makes HRegionInfo for out tables available in memory. Refresh rate configured in SpliceConstants.
 *
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class HBaseRegionCache implements RegionCache {

    private static final Logger CACHE_LOG = Logger.getLogger(HBaseRegionCache.class);
    private static final RegionCache INSTANCE = HBaseRegionCache.create(SpliceConstants.cacheUpdatePeriod);

    private final ConcurrentSkipListMap<byte[], SortedSet<HRegionInfo>> regionCache;
    private final ScheduledExecutorService cacheUpdater;

    private final long cacheUpdatePeriod;
    private volatile long cacheUpdatedTimestamp;
    private CacheRefreshRunnable cacheRefreshRunnable;

    private final RegionCacheStatus status = new RegionStatus();
    private final RegionCacheLoader regionCacheLoader = new RegionCacheLoader(SpliceConstants.config);

    private HBaseRegionCache(long cacheUpdatePeriod,
                             ScheduledExecutorService cacheUpdater) {
        this.regionCache = new ConcurrentSkipListMap<byte[], SortedSet<HRegionInfo>>(Bytes.BYTES_COMPARATOR);
        this.cacheUpdater = cacheUpdater;
        this.cacheUpdatePeriod = cacheUpdatePeriod;
    }

    public static RegionCache getInstance() {
        return INSTANCE;
    }

    public static RegionCache create(long cacheUpdatePeriod) {
        ScheduledExecutorService cacheUpdater = MoreExecutors.namedSingleThreadScheduledExecutor("tablewriter-cacheupdater-%d");
        return new HBaseRegionCache(cacheUpdatePeriod, cacheUpdater);
    }

    @Override
    public void start() {
        cacheRefreshRunnable = new CacheRefreshRunnable();
        cacheUpdater.scheduleAtFixedRate(cacheRefreshRunnable, 0l, cacheUpdatePeriod, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        cacheUpdater.shutdownNow();
    }

    @Override
    public SortedSet<HRegionInfo> getRegions(byte[] tableName) throws ExecutionException {
        SortedSet<HRegionInfo> regions = regionCache.get(tableName);
        if (regions == null) {
            try {
                regions = regionCacheLoader.load(tableName);
            } catch (Exception e) {
                throw new ExecutionException(e);
            }
            regionCache.putIfAbsent(tableName, regions);
        }
        return regions;
    }

    @Override
    public SortedSet<HRegionInfo> getRegionsInRange(byte[] tableName, byte[] startRow, byte[] stopRow) throws ExecutionException {
        SortedSet<HRegionInfo> regions = getRegions(tableName);
        if (startRow.length <= 0 && stopRow.length <= 0) {
            //short circuit in the case where all regions are contained
            return regions;
        }
        SortedSet<HRegionInfo> containedRegions = Sets.newTreeSet();
        if (Bytes.equals(startRow, stopRow)) {
            for (HRegionInfo info : regions) {
                if (info.containsRow(startRow)) {
                    containedRegions.add(info);
                    return containedRegions;
                }
            }
        }
        for (HRegionInfo info : regions) {
            if (HRegionUtil.containsRange(info, startRow, stopRow))
                containedRegions.add(info);
        }
        return containedRegions;
    }

    @Override
    public void invalidate(byte[] tableName) {
        regionCache.remove(tableName);
    }

    @Override
    public long size() {
        return regionCache.size();
    }

    @Override
    public long getUpdateTimestamp() {
        return cacheUpdatedTimestamp;
    }

    @Override
    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        ObjectName cacheName = new ObjectName("com.splicemachine.region:type=RegionCacheStatus");
        mbs.registerMBean(status, cacheName);
    }

    /**
     * Task scheduled to periodically refresh cache.
     */
    private class CacheRefreshRunnable implements Runnable {

        @Override
        public void run() {
            SpliceLogUtils.debug(CACHE_LOG, "Refreshing Region cache for all tables");
            MetaScanner.MetaScannerVisitor visitor = new MetaScanner.MetaScannerVisitor() {
                private byte[] lastByte;
                private SortedSet<HRegionInfo> regionInfos = new ConcurrentSkipListSet<HRegionInfo>();

                @Override
                public boolean processRow(Result rowResult) throws IOException {
                    HRegionInfo info = MetaReader.parseHRegionInfoFromCatalogResult(rowResult, HConstants.REGIONINFO_QUALIFIER);
                    if (lastByte == null) {
                        lastByte = info.getTableName();
                        regionInfos.add(info);
                    } else if (Arrays.equals(lastByte, info.getTableName())) {
                        if (!info.isOffline() && !info.isSplitParent() && !info.isSplit())
                            regionInfos.add(info);
                    } else {
                        regionCache.put(lastByte, regionInfos);
                        lastByte = info.getTableName();
                        regionInfos = new ConcurrentSkipListSet<HRegionInfo>();
                        if (!info.isOffline() && !info.isSplitParent() && !info.isSplit())
                            regionInfos.add(info);
                    }
                    return true;
                }

                @Override
                public void close() throws IOException {
                    if(lastByte != null) {
                        regionCache.put(lastByte, regionInfos);
                    }
                }
            };

            try {
                MetaScanner.metaScan(SpliceConstants.config, visitor);
            } catch (IOException e) {
                if (e instanceof RegionServerStoppedException) {
                    getInstance().shutdown();
                    SpliceLogUtils.info(CACHE_LOG, "The region cache is shutting down as the server has stopped");

                } else {
                    SpliceLogUtils.error(CACHE_LOG, "Unable to update region cache", e);
                }
            }
            cacheUpdatedTimestamp = System.currentTimeMillis();
        }
    }

    /**
     * CacheLoader instance
     */
    private static class RegionCacheLoader extends CacheLoader<Integer, SortedSet<HRegionInfo>> {
        private final Configuration configuration;

        public RegionCacheLoader(Configuration configuration) {
            this.configuration = configuration;
        }

        public SortedSet<HRegionInfo> load(final byte[] data) throws Exception {
            final SortedSet<HRegionInfo> regionInfos = new ConcurrentSkipListSet<HRegionInfo>();
            final MetaScanner.MetaScannerVisitor visitor = new MetaScanner.MetaScannerVisitor() {
                @Override
                public boolean processRow(Result rowResult) throws IOException {
                    HRegionInfo info = MetaReader.parseHRegionInfoFromCatalogResult(rowResult, HConstants.REGIONINFO_QUALIFIER);
                    if (Bytes.equals(data, info.getTableName())
                            && !info.isOffline()
                            && !info.isSplit()
                            && !info.isSplitParent()) {
                        regionInfos.add(info);
                    }
                    return true;
                }

                @Override
                public void close() throws IOException {
                }
            };

            try {
                MetaScanner.metaScan(configuration, visitor);
            } catch (IOException e) {
                SpliceLogUtils.error(CACHE_LOG, "Unable to update region cache", e);
            }
            SpliceLogUtils.trace(CACHE_LOG, "loaded regions %s", regionInfos);
            return regionInfos;

        }

        @Override
        public SortedSet<HRegionInfo> load(final Integer key) throws Exception {
            SpliceLogUtils.trace(CACHE_LOG, "Loading regions for key %d", key);
            final SortedSet<HRegionInfo> regionInfos = new ConcurrentSkipListSet<HRegionInfo>();
            final MetaScanner.MetaScannerVisitor visitor = new MetaScanner.MetaScannerVisitor() {
                @Override
                public boolean processRow(Result rowResult) throws IOException {
                    HRegionInfo info = MetaReader.parseHRegionInfoFromCatalogResult(rowResult, HConstants.REGIONINFO_QUALIFIER);
                    Integer tableKey = Bytes.mapKey(info.getTableName());
                    if (key.equals(tableKey)
                            && !info.isOffline()
                            && !info.isSplit()
                            && !info.isSplitParent()) {
                        regionInfos.add(info);
                    }

                    return true;
                }

                @Override
                public void close() throws IOException {
                }
            };

            try {
                MetaScanner.metaScan(configuration, visitor);
            } catch (IOException e) {
                if (e instanceof RegionServerStoppedException) {
                    getInstance().shutdown();
                    SpliceLogUtils.info(CACHE_LOG, "The region cache is shutting down as the server has stopped");

                    throw e;
                } else {
                    SpliceLogUtils.error(CACHE_LOG, "Unable to update region cache", e);
                }
            }
            SpliceLogUtils.trace(CACHE_LOG, "loaded regions %s", regionInfos);
            return regionInfos;
        }
    }

    /**
     * For JMX
     */
    private class RegionStatus implements RegionCacheStatus {

        @Override
        public long getLastUpdatedTimestamp() {
            return cacheUpdatedTimestamp;
        }

        @Override
        public void updateCache() {
            cacheRefreshRunnable.run();
        }

        @Override
        public int getNumCachedRegions(String tableName) {
            SortedSet<HRegionInfo> regions = regionCache.get(tableName.getBytes());
            if (regions == null) return 0;
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

}
