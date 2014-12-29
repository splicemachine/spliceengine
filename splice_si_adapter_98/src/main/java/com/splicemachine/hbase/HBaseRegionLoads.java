package com.splicemachine.hbase;

import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.splicemachine.concurrent.DynamicScheduledRunnable;
import com.splicemachine.concurrent.MoreExecutors;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author P Trolard
 *         Date: 19/03/2014
 */
public class HBaseRegionLoads {

    private static final Logger LOG = Logger.getLogger(HBaseRegionLoads.class);

    private static HBaseAdmin admin;

    // Periodic updating

    private static final int UPDATE_MULTIPLE = 15;
    private static final int SMALLEST_UPDATE_INTERVAL = 200;
    private static final AtomicBoolean started = new AtomicBoolean(false);
    private static final AtomicReference<Map<String, Map<String,RegionLoad>>> cache =
        // The cache is a map from tablename to map of regionname to RegionLoad
        new AtomicReference<>();

    private static final Runnable updater = new Runnable() {
        @Override
        public void run() {
            long begin = System.currentTimeMillis();
            Map<String,Map<String,RegionLoad>> loads = fetchRegionLoads();
            cache.set(loads);
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Region loads loaded in %dms:\n%s",
                                           System.currentTimeMillis() - begin,
                                           loads.keySet()));
            }
        }
    };

    private static ScheduledExecutorService updateService =
            MoreExecutors.namedSingleThreadScheduledExecutor("hbase-region-load-updater-%d");

    /**
     * Start updating in background every UPDATE_MULTIPLE multiples
     * of update running time
     */
    public static void start() {
        if (started.compareAndSet(false, true)) {
            updateService
                .execute(new DynamicScheduledRunnable(updater,
                        updateService,
                        UPDATE_MULTIPLE,
                        SMALLEST_UPDATE_INTERVAL));
        }
    }

    /**
     * Update now, blocking until finished or interrupted
     */
    public static void update() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        updateService.execute(new Runnable() {
            @Override
            public void run() {
                updater.run();
                latch.countDown();
            }
        });
        latch.await();
    }

    /**
     * Schedule an update to run as soon as possible
     */
    public static void scheduleUpdate() {
        updateService.execute(updater);
    }

    // Fetching

    private static String tableForRegion(String regionName){
        int comma = regionName.indexOf(",");
        if (comma > -1) {
            return regionName.substring(0,comma);
        }
        return regionName;
    }

    private static HBaseAdmin getAdmin() {
        if (admin == null) {
            admin = SpliceUtilities.getAdmin();
        }
        try {
            // Check to see if this admin instance still good
            admin.isMasterRunning();
        } catch (MasterNotRunningException | ZooKeeperConnectionException e) {
            // If not, close & get a new one
            Closeables.closeQuietly(admin);
            admin = SpliceUtilities.getAdmin();
        }
        return admin;
    }

    private static Map<String, Map<String,RegionLoad>> fetchRegionLoads() {
        Map<String, Map<String,RegionLoad>> regionLoads =
            new HashMap<String, Map<String,RegionLoad>>(){
                @Override
                public Map<String, RegionLoad> get(Object key) {
                    Map<String, RegionLoad> value = super.get(key);
                    if(value==null)
                        value = Maps.newHashMap();
                    super.put((String)key,value);

                    return value;
                }
            };
        HBaseAdmin admin = getAdmin();
        try {
            ClusterStatus clusterStatus = admin.getClusterStatus();
            for (ServerName serverName : clusterStatus.getServers()) {
                final ServerLoad serverLoad = clusterStatus.getLoad(serverName);

                for (Map.Entry<byte[], RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {
                    String regionName = Bytes.toString(entry.getKey());
                    String tableName = tableForRegion(regionName);
                    Map<String,RegionLoad> loads = regionLoads.get(tableName);
                    loads.put(regionName, entry.getValue());
                }
            }
        } catch (IOException e) {
            SpliceLogUtils.error(LOG, "Unable to fetch region load info", e);
        }
        return Collections.unmodifiableMap(regionLoads);
    }

    // Lookups

    public static Collection<RegionLoad> getCachedRegionLoadsForTable(String tableName){
        Map<String,Map<String,RegionLoad>> loads = cache.get();
        if (loads == null){
            return null;
        }
        Map<String, RegionLoad> regions = loads.get(tableName);
        return regions == null ? null : regions.values();
    }

    public static Map<String, RegionLoad> getCachedRegionLoadsMapForTable(String tableName){
        Map<String,Map<String,RegionLoad>> loads = cache.get();
        if (loads == null){
            return null;
        }
        return loads.get(tableName);
    }
    /**
     * Region Size in MB
     */
    public static int memstoreAndStorefileSize(RegionLoad load){
        return load.getStorefileSizeMB() + load.getMemStoreSizeMB();
    }

    public static long memstoreAndStoreFileSize(String tableName) {
        Map<String,RegionLoad> regionLoads = HBaseRegionLoads.getCachedRegionLoadsMapForTable(tableName);
    	if (regionLoads == null)
    		return -1;
    	long cost = 0;
        for (RegionLoad regionLoad: regionLoads.values()) {
        	cost += HBaseRegionLoads.memstoreAndStorefileSize(regionLoad);
        }
        return cost;
    }
    

    
}
