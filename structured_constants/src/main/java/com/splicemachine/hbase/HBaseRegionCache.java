package com.splicemachine.hbase;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.io.IOException;
import java.util.Arrays;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class HBaseRegionCache implements RegionCache {
    private static final RegionCache INSTANCE = HBaseRegionCache.create(SpliceConstants.cacheExpirationPeriod,SpliceConstants.cacheUpdatePeriod);

    private static final Logger CACHE_LOG = Logger.getLogger(HBaseRegionCache.class);
    private final LoadingCache<Integer,SortedSet<HRegionInfo>> regionCache;
    private final ScheduledExecutorService cacheUpdater;

    private final long cacheUpdatePeriod;
    private volatile long cacheUpdatedTimestamp;
    private RegionCacheLoader regionCacheLoader;

    private RegionCacheStatus status = new RegionStatus();

    private HBaseRegionCache(long cacheUpdatePeriod,
                             LoadingCache<Integer, SortedSet<HRegionInfo>> regionCache,
                             ScheduledExecutorService cacheUpdater) {
        this.regionCache = regionCache;
        this.cacheUpdater = cacheUpdater;
        this.cacheUpdatePeriod = cacheUpdatePeriod;
    }

    public static RegionCache getInstance(){
        return INSTANCE;
    }

    public static RegionCache create(long cacheExpirationPeriod,long cacheUpdatePeriod){
        LoadingCache<Integer,SortedSet<HRegionInfo>> regionCache =
                CacheBuilder.newBuilder()
                        .expireAfterWrite(cacheExpirationPeriod,TimeUnit.SECONDS)
                        .build(new RegionLoader(SpliceConstants.config));

        ThreadFactory cacheFactory = new ThreadFactoryBuilder()
                .setNameFormat("tablewriter-cacheupdater-%d")
                .setDaemon(true)
                .setPriority(Thread.NORM_PRIORITY).build();
        ScheduledExecutorService cacheUpdater = Executors.newSingleThreadScheduledExecutor(cacheFactory);

        return new HBaseRegionCache(cacheUpdatePeriod,regionCache,cacheUpdater);
    }

    @Override
    public void start(){
        regionCacheLoader = new RegionCacheLoader();
        cacheUpdater.scheduleAtFixedRate(regionCacheLoader,0l,cacheUpdatePeriod, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown(){
        cacheUpdater.shutdownNow();
    }

    @Override
    public SortedSet<HRegionInfo> getRegions(TableName tableName) throws ExecutionException {
        return regionCache.get(Bytes.mapKey(tableName.getName()));
    }

    @Override
    public void invalidate(TableName tableName){
        regionCache.invalidate(Bytes.mapKey(tableName.getName()));
    }

    @Override
    public long size(){
        return regionCache.size();
    }

    @Override
    public long getUpdateTimestamp() {
        return cacheUpdatedTimestamp;
    }

    @Override
    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        ObjectName cacheName = new ObjectName("com.splicemachine.region:type=RegionCacheStatus");
        mbs.registerMBean(status,cacheName);
    }

    private class RegionCacheLoader  implements Runnable{

        @Override
        public void run() {
            SpliceLogUtils.debug(CACHE_LOG, "Refreshing Region cache for all tables");
            MetaScanner.MetaScannerVisitor visitor = new MetaScanner.MetaScannerVisitor() {
                private byte[] lastByte;
                private SortedSet<HRegionInfo> regionInfos = new ConcurrentSkipListSet<HRegionInfo>();            	
                @Override
                public boolean processRow(Result rowResult) throws IOException {
//                    byte[] bytes = rowResult.getValue(HConstants.CATALOG_FAMILY,HConstants.REGIONINFO_QUALIFIER);
//                    if(bytes==null){
//                    	SpliceLogUtils.error(CACHE_LOG, "Error processing row with null bytes row={%s}", rowResult);
//                        return true;
//                    }
//                    HRegionInfo info = MetaReader.parseHRegionInfoFromCatalogResult(rowResult,HConstants.REGIONINFO_QUALIFIER);
                    HRegionInfo info = MetaScanner.getHRegionInfo(rowResult);
                    if (lastByte==null) {
                    	lastByte = info.getTable().getName();
                    	regionInfos.add(info);
                    }	
                    else if (Arrays.equals(lastByte,info.getTable().getName())) {
												if(!info.isOffline() &&!info.isSplitParent() &&!info.isSplit())
                        	regionInfos.add(info);
                    } else {
                    	regionCache.put(Bytes.mapKey(lastByte), regionInfos);
                    	lastByte = info.getTable().getName();
                    	regionInfos = new ConcurrentSkipListSet<HRegionInfo>();
												if(!info.isOffline() &&!info.isSplitParent() &&!info.isSplit())
                        	regionInfos.add(info);
                    }
                    return true;
                }

                @Override
                public void close() throws IOException {
                	regionCache.put(Bytes.mapKey(lastByte), regionInfos);
                }
            };

            try {
                MetaScanner.metaScan(SpliceConstants.config,visitor);
            } catch (IOException e) {
                SpliceLogUtils.error(CACHE_LOG,"Unable to update region cache",e);
            }
            cacheUpdatedTimestamp = System.currentTimeMillis();
        }
    }

    private static class RegionLoader extends CacheLoader<Integer, SortedSet<HRegionInfo>> {
        private final Configuration configuration;

        public RegionLoader(Configuration configuration) {
            this.configuration = configuration;
        }

        @Override
        public SortedSet<HRegionInfo> load(final Integer key) throws Exception {
            SpliceLogUtils.trace(CACHE_LOG,"Loading regions for key %d",key);
            final SortedSet<HRegionInfo> regionInfos = new ConcurrentSkipListSet<HRegionInfo>();
            final MetaScanner.MetaScannerVisitor visitor = new MetaScanner.MetaScannerVisitor() {
                @Override
                public boolean processRow(Result rowResult) throws IOException {
//                    HRegionInfo info = MetaReader.parseHRegionInfoFromCatalogResult(rowResult,HConstants.REGIONINFO_QUALIFIER);
                    HRegionInfo info = MetaScanner.getHRegionInfo(rowResult);
//                    byte[] bytes = rowResult.getValue(HConstants.CATALOG_FAMILY,HConstants.REGIONINFO_QUALIFIER);
//                    if(bytes==null){
//                    	SpliceLogUtils.error(CACHE_LOG, "Error processing row with null bytes row={%s}", rowResult);
//                        return true;
//                    }
//                    HRegionInfo info = Writables.getHRegionInfo(bytes);
                    Integer tableKey = Bytes.mapKey(info.getTable().getName());
                    if(key.equals(tableKey)
														&& !info.isOffline()
														&&!info.isSplit()
														&&!info.isSplitParent()){
                        regionInfos.add(info);
                    }
                    return true;
                }

                @Override
                public void close() throws IOException {
                }
            };

            try {
                MetaScanner.metaScan(configuration,visitor);
            } catch (IOException e) {
                SpliceLogUtils.error(CACHE_LOG,"Unable to update region cache",e);
            }
            SpliceLogUtils.trace(CACHE_LOG,"loaded regions %s",regionInfos);
            return regionInfos;
        }
    }

    private class RegionStatus implements RegionCacheStatus{

        @Override
        public long getLastUpdatedTimestamp() {
            return cacheUpdatedTimestamp;
        }

        @Override
        public void updateCache() {
            regionCacheLoader.run();
        }

        @Override
        public int getNumCachedRegions(String tableName) {
            try {
                return regionCache.get(Bytes.mapKey(tableName.getBytes())).size();
            } catch (ExecutionException e) {
                return -1;
            }
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
