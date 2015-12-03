package com.splicemachine.derby.impl.stats;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.derby.iapi.catalog.PhysicalStatsDescriptor;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.storage.EntryDecoder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
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
    private final byte[] physicalStatsId;

    public CachedPhysicalStatsStore(ScheduledExecutorService refreshThread,
                                    byte[] physicalStatsId) {
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
            Scan s = new Scan();
            s.setStartRow(HConstants.EMPTY_START_ROW);
            s.setStopRow(HConstants.EMPTY_START_ROW);
            try(HTableInterface table =SpliceAccessManager.getHTable(physicalStatsId)){
                try(ResultScanner rs = table.getScanner(s)){
                    EntryDecoder decoder=new EntryDecoder();
                    Result join;
                    while((join=rs.next())!=null){
                        Cell dataColumn=matchDataColumn(join.rawCells());
                        MultiFieldDecoder fieldDecoder=decoder.get();
                        fieldDecoder.set(dataColumn.getRowArray(),dataColumn.getRowOffset(),dataColumn.getRowLength());
                        String hostname=fieldDecoder.decodeNextString();

                        decoder.set(dataColumn.getValueArray(),dataColumn.getValueOffset(),dataColumn.getValueLength());
                        fieldDecoder=decoder.get();
                        int cpus=fieldDecoder.decodeNextInt();
                        long maxHeap=fieldDecoder.decodeNextLong();
                        int numIpc=fieldDecoder.decodeNextInt();
                        PhysicalStatsDescriptor statsDescriptor=new PhysicalStatsDescriptor(hostname,
                                cpus,
                                maxHeap,
                                numIpc);
                        physicalStatisticsCache.put(hostname,statsDescriptor);
                    }
                }
            }catch (Exception e) {
                LOG.warn("Error while fetching Physical Statistics", e);
            }
        }

        private Cell matchDataColumn(Cell[] row) {
            byte[] packedColumnBytes=SIConstants.PACKED_COLUMN_BYTES;
            for(Cell kv:row){
                if(Bytes.equals(kv.getQualifierArray(),kv.getQualifierOffset(),kv.getQualifierLength(),packedColumnBytes,0,packedColumnBytes.length)) return kv;
            }
            throw new IllegalStateException("Programmer error: Did not find a data column!");
        }
    }
}
