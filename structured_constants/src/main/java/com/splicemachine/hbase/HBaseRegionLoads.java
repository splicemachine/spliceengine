package com.splicemachine.hbase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.concurrent.DynamicSchedule;
import com.splicemachine.utils.Misc;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;

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
        new AtomicReference<Map<String, Map<String,RegionLoad>>>();

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

    private static final Supplier<Map<String,RegionLoad>> newMap =
        new Supplier<Map<String, RegionLoad>>() {
            @Override
            public Map<String, RegionLoad> get() {
                return Maps.newHashMap();
            }
        };

    private static Map<String, Map<String,RegionLoad>> fetchRegionLoads() {
        Map<String, Map<String,RegionLoad>> regionLoads =
            new HashMap<String, Map<String,RegionLoad>>();
        HBaseAdmin admin = getAdmin();
        try {
            ClusterStatus clusterStatus = admin.getClusterStatus();
            for (ServerName serverName : clusterStatus.getServers()) {
                final ServerLoad serverLoad = clusterStatus.getLoad(serverName);

                for (Map.Entry<byte[], RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {
                    String tableName = tableForRegion(entry.getKey());
                    Map<String,RegionLoad> loads =
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

    public static Collection<RegionLoad> getCachedRegionLoadsForTable(String tableName){
        Map<String,Map<String,RegionLoad>> loads = cache.get();
        if (loads == null){
            return null;
        }
        Map<String, RegionLoad> regions = loads.get(tableName);
        return regions == null ? null : regions.values();
    }

    public static int memstoreAndStorefileSize(RegionLoad load){
        return load.getStorefileSizeMB() + load.getMemStoreSizeMB();
    }

}
