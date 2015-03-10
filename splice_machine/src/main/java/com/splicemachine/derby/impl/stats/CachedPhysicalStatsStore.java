package com.splicemachine.derby.impl.stats;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.async.HBaseClient;
import com.splicemachine.async.KeyValue;
import com.splicemachine.async.Scanner;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.derby.iapi.catalog.PhysicalStatsDescriptor;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.stumbleupon.async.Deferred;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public class CachedPhysicalStatsStore implements PhysicalStatisticsStore {
    private static final Logger LOG = Logger.getLogger(CachedPhysicalStatsStore.class);
    private final Cache<String,PhysicalStatsDescriptor> physicalStatisticsCache;
    private final ScheduledExecutorService refreshThread;
    private final HBaseClient hbaseClient;
    private final byte[] physicalStatsId;

    public CachedPhysicalStatsStore(ScheduledExecutorService refreshThread,
                                    HBaseClient hbaseClient,
                                    byte[] physicalStatsId) {
        this.hbaseClient = hbaseClient;
        this.physicalStatsId = physicalStatsId;
        this.physicalStatisticsCache = CacheBuilder.newBuilder()
                .expireAfterWrite(StatsConstants.DEFAULT_PARTITION_CACHE_EXPIRATION, TimeUnit.MILLISECONDS)
                .build();
        this.refreshThread = refreshThread;
    }

    public void start(){
        refreshThread.scheduleAtFixedRate(new Refresher(),0l,StatsConstants.DEFAULT_PARTITION_CACHE_EXPIRATION/3,TimeUnit.MILLISECONDS);
    }

    public void shutdown(){
        refreshThread.shutdownNow();
    }

    @Override
    public List<PhysicalStatsDescriptor> allPhysicalStats() {
        List<PhysicalStatsDescriptor> descriptors = new ArrayList<>((int)physicalStatisticsCache.size());
        for(PhysicalStatsDescriptor descriptor:physicalStatisticsCache.asMap().values()){
            descriptors.add(descriptor);
        }
        return descriptors;
    }

    private class Refresher implements Runnable{

        @Override
        public void run() {
            Scanner scanner = hbaseClient.newScanner(physicalStatsId);
            scanner.setStartKey(HConstants.EMPTY_START_ROW);
            scanner.setStopKey(HConstants.EMPTY_START_ROW);
            try{
                Deferred<ArrayList<ArrayList<KeyValue>>> arrayListDeferred = scanner.nextRows();
                ArrayList<ArrayList<KeyValue>> join;
                MultiFieldDecoder decoder = MultiFieldDecoder.create();
                while((join = arrayListDeferred.join())!=null){
                    for(ArrayList<KeyValue> row:join){
                        KeyValue dataColumn = matchDataColumn(row);
                        decoder.set(dataColumn.key());
                        String hostname = decoder.decodeNextString();
                        decoder.set(dataColumn.value());
                        int cpus = decoder.decodeNextInt();
                        long maxHeap = decoder.decodeNextLong();
                        int numIpc = decoder.decodeNextInt();
                        long localReadLatency = decoder.decodeNextLong();
                        long remoteReadLatency = decoder.decodeNextLong();
                        long writeLatency = decoder.decodeNextLong();

                        PhysicalStatsDescriptor statsDescriptor = new PhysicalStatsDescriptor(hostname,
                                cpus,
                                maxHeap,
                                numIpc,
                                localReadLatency,
                                remoteReadLatency,
                                writeLatency);
                        physicalStatisticsCache.put(hostname,statsDescriptor);
                    }
                    arrayListDeferred = scanner.nextRows();
                }
            } catch (InterruptedException e) {
                LOG.info("Interrupted while fetching Physical Statistics");
            } catch (Exception e) {
                LOG.warn("Error while fetching Physical Statistics", e);
            } finally{
                scanner.close();
            }
        }

        private KeyValue matchDataColumn(List<KeyValue> row) {
            for(KeyValue kv:row){
               if(Bytes.equals(kv.qualifier(), SIConstants.PACKED_COLUMN_BYTES)) return kv;
            }
            throw new IllegalStateException("Programmer error: Did not find a data column!");
        }
    }
}
