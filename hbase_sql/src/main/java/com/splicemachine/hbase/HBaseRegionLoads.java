package com.splicemachine.hbase;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.protobuf.SpliceZeroCopyByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.access.hbase.HBaseTableFactory;
import com.splicemachine.concurrent.MoreExecutors;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.hbase.table.SpliceRpcController;
import com.splicemachine.storage.PartitionLoad;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author P Trolard
 *         Date: 19/03/2014
 */
public class HBaseRegionLoads implements PartitionLoadWatcher{
    private static final Logger LOG = Logger.getLogger(HBaseRegionLoads.class);
    // Periodic updating
    private static final AtomicBoolean started = new AtomicBoolean(false);
    // The cache is a map from tablename to map of regionname to RegionLoad
    private static final AtomicReference<Map<String, Map<String,RegionLoad>>> cache = new AtomicReference<>();


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
    @Override
    public void startWatching() {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"start attempted");
        if (started.compareAndSet(false, true)) {
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"update service scheduled");
            updateService.scheduleAtFixedRate(updater,0l,SpliceConstants.regionLoadUpdateInterval,TimeUnit.SECONDS);
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


    private static Map<String, Map<String,RegionLoad>> fetchRegionLoads() {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"fetch region loads");
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
        try(Admin admin = HBaseConnectionFactory.getInstance().getConnection().getAdmin()){
            ClusterStatus clusterStatus=admin.getClusterStatus();
            for(ServerName serverName : clusterStatus.getServers()){
                final ServerLoad serverLoad=clusterStatus.getLoad(serverName);
                if (LOG.isDebugEnabled())
                    SpliceLogUtils.debug(LOG,"cluster status for serverLoad=%s",serverLoad);
                for(Map.Entry<byte[], RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()){
                    String regionName=Bytes.toString(entry.getKey());
                    String tableName=tableForRegion(regionName);
                    Map<String, RegionLoad> loads=regionLoads.get(tableName);
                    if (LOG.isDebugEnabled())
                        SpliceLogUtils.debug(LOG,"processing regionName=%s, tableName=%s, loads=%s",regionName,tableName,loads);
                    loads.put(regionName,entry.getValue());
                }
            }
        }catch(IOException e){
            SpliceLogUtils.error(LOG,"Unable to fetch region load info",e);
        }
        return Collections.unmodifiableMap(regionLoads);
    }

    // Lookups

    public static Map<String, RegionLoad> getCostWhenNoCachedRegionLoadsFound(String tableName){
        try (Table t =  HBaseTableFactory.getInstance().getTable(tableName)){
            Map<byte[], Pair<String, Long>> ret = t.coprocessorService(SpliceMessage.SpliceDerbyCoprocessorService.class, HConstants.EMPTY_START_ROW,
                    HConstants.EMPTY_END_ROW, new Batch.Call<SpliceMessage.SpliceDerbyCoprocessorService, Pair<String, Long>>() {
                        @Override
                        public Pair<String, Long> call(SpliceMessage.SpliceDerbyCoprocessorService inctance) throws IOException {
                            SpliceRpcController controller = new SpliceRpcController();
                            SpliceMessage.SpliceRegionSizeRequest message = SpliceMessage.SpliceRegionSizeRequest.newBuilder().build();
                            BlockingRpcCallback<SpliceMessage.SpliceRegionSizeResponse> rpcCallback = new BlockingRpcCallback<>();
                            inctance.computeRegionSize(controller, message, rpcCallback);
                            if (controller.failed()) {
                                Throwable t = Throwables.getRootCause(controller.getThrowable());
                                if (t instanceof IOException) throw (IOException) t;
                                else throw new IOException(t);
                            }
                            SpliceMessage.SpliceRegionSizeResponse response = rpcCallback.get();

                            return Pair.newPair(response.getEncodedName(), response.getSizeInBytes());
                        }
                    });
            Collection<Pair<String, Long>> collection = ret.values();
            long factor = 1024 * 1024;
            Map<String, RegionLoad> retMap = new HashMap<>();
            for(Pair<String, Long> info : collection){
                long sizeMB = info.getSecond() / factor;
                ClusterStatusProtos.RegionLoad.Builder rl = ClusterStatusProtos.RegionLoad.newBuilder();
                rl.setMemstoreSizeMB((int)(sizeMB / 2));
                rl.setStorefileSizeMB((int) (sizeMB / 2));
                rl.setRegionSpecifier(HBaseProtos.RegionSpecifier.newBuilder()
                    .setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.ENCODED_REGION_NAME).setValue(
                                        ZeroCopyLiteralByteString.copyFromUtf8(info.getFirst())).build());
                ClusterStatusProtos.RegionLoad load = rl.build();
                retMap.put(info.getFirst(), new RegionLoad(load));
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

    public Collection<PartitionLoad> tableLoad(String tableName){
        throw new UnsupportedOperationException("IMPLEMENT");
    }
    public static Collection<RegionLoad> getCachedRegionLoadsForTable(String tableName) {
        Map<String, Map<String, RegionLoad>> loads = cache.get();
        if (loads == null) {
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG, "This should not happen");
            return Collections.emptyList();
        }
        Map<String, RegionLoad> regions = loads.get(tableName);
        if(regions==null || regions.isEmpty()){
            regions = getCostWhenNoCachedRegionLoadsFound(tableName);
        }
        return regions.values();
    }

    public static Map<String, RegionLoad> getCachedRegionLoadsMapForTable(String tableName){
        Map<String,Map<String,RegionLoad>> loads = cache.get();
        if (loads == null||loads.isEmpty()){
            return getCostWhenNoCachedRegionLoadsFound(tableName);
        }
        //we don't want to return null ever, so return an empty map when we don't have anything else
        if(!loads.containsKey(tableName)) return Collections.emptyMap();
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
