package com.splicemachine.hbase;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.utils.Misc;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.ServerName;
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

    // Periodic updating

    //private static final int UPDATE_INTERVAL = 60 * 1000;
    private static final int UPDATE_INTERVAL = 200;
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

    /*
     * Start updating in background every UPDATE_INTERVAL ms
     */
    public static void start() {
        if (started.compareAndSet(false, true)) {
            updateService.scheduleAtFixedRate(updater, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        }
    }

    /*
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

    /*
     * Schedule an update to run as soon as possible
     */
    public static void scheduleUpdate() {
        updateService.execute(updater);
    }

    // Fetching

    public static String tableForRegion(byte[] regionName){
        String[] splits = Bytes.toString(regionName).split(",", 2);
        if (splits.length > 0){
            return splits[0];
        }
        return "";
    }

    private static Map<String, Map<String,HServerLoad.RegionLoad>> fetchRegionLoads() {
        Map<String, Map<String,HServerLoad.RegionLoad>> regionLoads =
            new HashMap<String, Map<String,HServerLoad.RegionLoad>>();
        HBaseAdmin admin = SpliceUtilities.getAdmin();
        try {
            ClusterStatus clusterStatus = admin.getClusterStatus();
            for (ServerName serverName : clusterStatus.getServers()) {
                final HServerLoad serverLoad = clusterStatus.getLoad(serverName);

                for (Map.Entry<byte[], HServerLoad.RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {
                    String tableName = tableForRegion(entry.getKey());
                    Map<String,HServerLoad.RegionLoad> loads =
                        Misc.lookupOrDefault(regionLoads,
                                                tableName,
                                                Maps.<String,HServerLoad.RegionLoad>newHashMap());
                    loads.put(Bytes.toString(entry.getKey()), entry.getValue());
                }
            }
        } catch (IOException e) {
            SpliceLogUtils.error(LOG, "Unable to fetch region load info", e);
        } finally {
            if (admin != null)
                try {
                    admin.close();
                } catch (IOException e) {
                    // ignore
                }
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
