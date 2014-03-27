package com.splicemachine.hbase;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.concurrent.DynamicSchedule;
import com.splicemachine.utils.Misc;
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
import java.util.concurrent.*;
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
    private static final AtomicReference<Map<String, Map<String,HServerLoad.RegionLoad>>> cache =
        // The cache is a map from tablename to map of regionname to RegionLoad
        new AtomicReference<Map<String, Map<String,HServerLoad.RegionLoad>>>();

    private static final Runnable updater = new Runnable() {
        @Override
        public void run() {
            long begin = System.currentTimeMillis();
            Map<String,Map<String,HServerLoad.RegionLoad>> loads = fetchRegionLoads();
            cache.set(loads);
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Region loads loaded in %dms:\n%s",
                                           System.currentTimeMillis() - begin,
                                           loads.keySet()));
            }
        }
    };

    private static ScheduledExecutorService updateService =
        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                                                       .setNameFormat("hbase-region-load-updater-%d")
                                                       .setDaemon(true)
                                                       .build());

    /**
     * Start updating in background every UPDATE_MULTIPLE multiples
     * of update running time
     */
    public static void start() {
        if (started.compareAndSet(false, true)) {
            updateService
                .execute(DynamicSchedule.runAndScheduleAsMultiple(updater,
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

    private static String tableForRegion(byte[] regionName){
        String name = Bytes.toString(regionName);
        int comma = name.indexOf(",");
        if (comma > -1) {
            return name.substring(0,comma);
        }
        return name;
    }

    private static HBaseAdmin getAdmin() {
        if (admin == null) {
            admin = SpliceUtilities.getAdmin();
        }
        try {
            // Check to see if this admin instance still good
            admin.isMasterRunning();
        } catch (MasterNotRunningException e) {
            // If not, close & get a new one
            Closeables.closeQuietly(admin);
            admin = SpliceUtilities.getAdmin();
        } catch (ZooKeeperConnectionException e) {
            Closeables.closeQuietly(admin);
            admin = SpliceUtilities.getAdmin();
        }
        return admin;
    }

    private static Supplier<Map<String,HServerLoad.RegionLoad>> newMap = new Supplier<Map<String, HServerLoad.RegionLoad>>() {
        @Override
        public Map<String, HServerLoad.RegionLoad> get() {
            return Maps.newHashMap();
        }
    };

    private static Map<String, Map<String,HServerLoad.RegionLoad>> fetchRegionLoads() {
        Map<String, Map<String,HServerLoad.RegionLoad>> regionLoads =
            new HashMap<String, Map<String,HServerLoad.RegionLoad>>();
        HBaseAdmin admin = getAdmin();
        try {
            ClusterStatus clusterStatus = admin.getClusterStatus();
            for (ServerName serverName : clusterStatus.getServers()) {
                final HServerLoad serverLoad = clusterStatus.getLoad(serverName);

                for (Map.Entry<byte[], HServerLoad.RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {
                    String tableName = tableForRegion(entry.getKey());
                    Map<String,HServerLoad.RegionLoad> loads =
                        Misc.lookupOrDefault(regionLoads, tableName, newMap);
                    loads.put(Bytes.toString(entry.getKey()), entry.getValue());
                }
            }
        } catch (IOException e) {
            SpliceLogUtils.error(LOG, "Unable to fetch region load info", e);
        }
        return Collections.unmodifiableMap(regionLoads);
    }

    // Lookups

    public static Collection<HServerLoad.RegionLoad> getCachedRegionLoadsForTable(String tableName){
        Map<String,Map<String,HServerLoad.RegionLoad>> loads = cache.get();
        if (loads == null){
            return null;
        }
        Map<String, HServerLoad.RegionLoad> regions = loads.get(tableName);
        return regions == null ? null : regions.values();
    }

    public static int memstoreAndStorefileSize(HServerLoad.RegionLoad load){
        return load.getStorefileSizeMB() + load.getMemStoreSizeMB();
    }

}
