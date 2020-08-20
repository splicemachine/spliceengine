/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.hbase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import com.google.common.collect.Maps;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.storage.SkeletonHBaseClientPartition;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.regionserver.HBasePlatformUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import splice.com.google.common.base.Throwables;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.concurrent.MoreExecutors;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.HPartitionLoad;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionLoad;
import com.splicemachine.storage.PartitionServer;
import com.splicemachine.storage.PartitionServerLoad;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils.BlockingRpcCallback;

/**
 * @author P Trolard
 *         Date: 19/03/2014
 */
public class HBaseRegionLoads implements PartitionLoadWatcher{
    private static final Logger LOG = Logger.getLogger(HBaseRegionLoads.class);
    // Periodic updating
    private static final AtomicBoolean started = new AtomicBoolean(false);
    // The cache is a map from tablename to map of regionname to RegionLoad
    private static final AtomicReference<Map<String, Map<String,PartitionLoad>>> cache = new AtomicReference<>();

    public static final HBaseRegionLoads INSTANCE = new HBaseRegionLoads();

    private HBaseRegionLoads(){}

    //TODO -sf- this isn't implemented properly
    @Override
    public void stopWatching(){
        updateService.shutdown();
    }

    private static final Runnable updater = new Runnable() {
        @Override
        public void run() {
            long begin = System.currentTimeMillis();
            Map<String,Map<String,PartitionLoad>> loads = fetchRegionLoads();
            cache.set(loads);
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Region loads loaded in %dms:%n%s",
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
    @Override
    public void startWatching() {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"start attempted");
        if (started.compareAndSet(false, true)) {
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"update service scheduled");

            SConfiguration configuration=SIDriver.driver().getConfiguration();
            long updateInterval = configuration.getRegionLoadUpdateInterval();
            long initialDelay = new Random().nextInt((int) updateInterval); // avoid refreshes from all RSs at once

            // initialize an empty cache
            cache.set(new HashMap<>());

            // schedule first update right away
            updateService.execute(updater);

            // schedule remaining updates staggered
            updateService.scheduleWithFixedDelay(updater,initialDelay,updateInterval,TimeUnit.SECONDS);
        }
    }

    /**
     * Update now, blocking until finished or interrupted
     */
    public static void update() throws InterruptedException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"update service scheduled");
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


    private static Map<String, Map<String,PartitionLoad>> fetchRegionLoads() {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"fetch region loads");
        Map<String, Map<String,PartitionLoad>> regionLoads =
            new HashMap<String, Map<String,PartitionLoad>>(){
                @Override
                public Map<String, PartitionLoad> get(Object key) {
                    Map<String, PartitionLoad> value = super.get(key);
                    if(value==null)
                        value = Maps.newHashMap();
                    super.put((String)key,value);

                    return value;
                }
            };
        try(PartitionAdmin admin =SIDriver.driver().getTableFactory().getAdmin()){

            Collection<PartitionServer> partitionServers=admin.allServers();
            for(PartitionServer ps:partitionServers){
                PartitionServerLoad load=ps.getLoad();
                if (LOG.isDebugEnabled())
                    SpliceLogUtils.debug(LOG,"cluster status for serverLoad=%s",load);
                Set<PartitionLoad> partitionLoadMap = load.getPartitionLoads();
                for(PartitionLoad pLoad:partitionLoadMap){
                    String tableName = tableForRegion(pLoad.getPartitionName());
                    regionLoads.get(tableName).put(pLoad.getPartitionName(),pLoad);
                }
            }
        }catch(Exception e){
            SpliceLogUtils.error(LOG,"Unable to fetch region load info",e);
        }
        return regionLoads;
    }

    // Lookups

    public static Map<String, PartitionLoad> getCostWhenNoCachedRegionLoadsFound(String tableName){
        try (Partition p =  SIDriver.driver().getTableFactory().getTable(tableName)){
            Map<byte[], Pair<String, Long>> ret = ((SkeletonHBaseClientPartition)p).coprocessorExec(SpliceMessage.SpliceDerbyCoprocessorService.class,
                     new Batch.Call<SpliceMessage.SpliceDerbyCoprocessorService, Pair<String, Long>>() {
                        @Override
                        public Pair<String, Long> call(SpliceMessage.SpliceDerbyCoprocessorService inctance) throws IOException {
                            ServerRpcController controller = new ServerRpcController();
                            SpliceMessage.SpliceRegionSizeRequest message = SpliceMessage.SpliceRegionSizeRequest.newBuilder().build();
                            BlockingRpcCallback<SpliceMessage.SpliceRegionSizeResponse> rpcCallback = new BlockingRpcCallback<>();
                            inctance.computeRegionSize(controller, message, rpcCallback);
                            if (controller.failed()) {
                                Throwable t = Throwables.getRootCause(controller.getFailedOn());
                                if (t instanceof IOException) throw (IOException) t;
                                else throw new IOException(t);
                            }
                            SpliceMessage.SpliceRegionSizeResponse response = rpcCallback.get();

                            return Pair.newPair(response.getEncodedName(), response.getSizeInBytes());
                        }
                    });
            Collection<Pair<String, Long>> collection = ret.values();
            Map<String, PartitionLoad> retMap = new HashMap<>();
            for(Pair<String, Long> info : collection){
                long size = info.getSecond();
                HPartitionLoad value=new HPartitionLoad(info.getFirst(),size/2,
                        size/2);
                retMap.put(info.getFirst(),value);
            }

            return retMap;
        } catch (Throwable th){
            SpliceLogUtils.error(LOG,"Unable to fetch region load info",th);
        }
        /*
         * When we fail for whatever reason, we don't want to blow up the query, we just return no
         * cached information. This will screw up the planning phase (since there is nothing to work with), but
         * at least it won't explode.
         */
        return Collections.emptyMap();
    }

    @Override
    public Collection<PartitionLoad> tableLoad(String tableName, boolean refresh){
        if (refresh) {
            Map<String, Map<String, PartitionLoad>> loads = cache.get();
            if (loads == null) {
                if (LOG.isDebugEnabled())
                    SpliceLogUtils.debug(LOG, "This should not happen");
                return Collections.emptyList();
            }
            Map<String, PartitionLoad> regions = getCostWhenNoCachedRegionLoadsFound(tableName);
            loads.put(HBaseTableInfoFactory.getInstance(HConfiguration.getConfiguration()).getTableInfo(tableName).getNameWithNamespaceInclAsString(),
                    regions
            );
            return regions.values();
        }
        return getCachedRegionLoadsForTable(tableName);
    }

    public static Collection<PartitionLoad> getCachedRegionLoadsForTable(String tableName) {
        Map<String, Map<String, PartitionLoad>> loads = cache.get();
        if (loads == null) {
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG, "This should not happen");
            return Collections.emptyList();
        }
        Map<String, PartitionLoad> regions = loads.get(HBaseTableInfoFactory.getInstance(HConfiguration.getConfiguration()).getTableInfo(tableName).getNameWithNamespaceInclAsString());
        if(regions==null || regions.isEmpty()){
            regions = getCostWhenNoCachedRegionLoadsFound(tableName);
            loads.put(HBaseTableInfoFactory.getInstance(HConfiguration.getConfiguration()).getTableInfo(tableName).getNameWithNamespaceInclAsString(),
                    regions
            );
        }
        return regions.values();
    }

    public static Map<String, PartitionLoad> getCachedRegionLoadsMapForTable(String tableName){
        Map<String,Map<String,PartitionLoad>> loads = cache.get();
        if (loads == null||loads.isEmpty()){
            return getCostWhenNoCachedRegionLoadsFound(tableName);
        }
        //we don't want to return null ever, so return an empty map when we don't have anything else
        if(!loads.containsKey(tableName)) return Collections.emptyMap();
        return loads.get(tableName);
    }
    /**
     * Region Size in bytes
     */
    public static long memstoreAndStorefileSize(PartitionLoad load){
        return load.getStorefileSize() + load.getMemStoreSize();
    }

    public static long memstoreAndStoreFileSize(String tableName) {
        Map<String,PartitionLoad> regionLoads = getCachedRegionLoadsMapForTable(tableName);
    	if (regionLoads == null)
    		return -1;
    	long cost = 0;
        for (PartitionLoad regionLoad: regionLoads.values()) {
        	cost += memstoreAndStorefileSize(regionLoad);
        }
        return cost;
    }
    

    
}
