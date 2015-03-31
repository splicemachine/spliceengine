package com.splicemachine.derby.impl.stats;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.async.HBaseClient;
import com.splicemachine.async.KeyValue;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.derby.iapi.catalog.TableStatisticsDescriptor;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnOperationFactory;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionLifecycle;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;
import com.stumbleupon.async.Deferred;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public class HBaseTableStatisticsStore implements TableStatisticsStore {
    private static final Logger LOG = Logger.getLogger(HBaseColumnStatisticsStore.class);
    private final Cache<String,TableStatisticsDescriptor> tableStatsCache;
    private final ScheduledExecutorService refreshThread;
    private final byte[] tableStatsConglom;
    private final HBaseClient hbaseClient;

    public HBaseTableStatisticsStore(ScheduledExecutorService refreshThread, byte[] tableStatsConglom, HBaseClient hbaseClient) {
        this.refreshThread = refreshThread;
        this.tableStatsConglom = tableStatsConglom;
        this.hbaseClient = hbaseClient;
        this.tableStatsCache = CacheBuilder.newBuilder().expireAfterWrite(StatsConstants.partitionCacheExpiration, TimeUnit.MILLISECONDS)
                .maximumSize(StatsConstants.partitionCacheSize).build();
    }

    public void start() throws ExecutionException {
        try {
            TxnView refreshTxn = TransactionLifecycle.getLifecycleManager().beginTransaction(Txn.IsolationLevel.READ_UNCOMMITTED);
            refreshThread.scheduleAtFixedRate(new Refresher(refreshTxn),0l,StatsConstants.partitionCacheExpiration/3,TimeUnit.MILLISECONDS);
        } catch (IOException e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public TableStatisticsDescriptor[] fetchTableStatistics(TxnView txn, long conglomerateId, List<String> partitionsToFetch) throws ExecutionException {
        TableStatisticsDescriptor[]  stats =new TableStatisticsDescriptor[partitionsToFetch.size()];
        Map<String,Integer> toFetch = new HashMap<>();
        for(int i=0;i<partitionsToFetch.size();i++){
            String partition = partitionsToFetch.get(i);
            TableStatisticsDescriptor tStats = tableStatsCache.getIfPresent(partition);
            if(tStats!=null)stats[i] = tStats;
            else{
               toFetch.put(partition, i);
            }
        }

        List<Get> gets = new ArrayList<>(toFetch.size());
        MultiFieldEncoder keyEncoder = MultiFieldEncoder.create(2);
        keyEncoder.encodeNext(conglomerateId).mark();
        TxnOperationFactory txnOperationFactory = TransactionOperations.getOperationFactory();
        for(String partition:toFetch.keySet()){
            keyEncoder.reset();
            byte[] key = keyEncoder.encodeNext(partition).build();
            gets.add(txnOperationFactory.newGet(txn, key));
        }

        try(HTableInterface table = SpliceAccessManager.getHTable(tableStatsConglom)){
            Result[] results = table.get(gets);
            int i=0;
            EntryDecoder entryDecoder = new EntryDecoder();
            for(String partition:toFetch.keySet()){
                Result r = results[i];
                if(r==null||r.isEmpty()) continue;
                Cell cell = CellUtils.matchDataColumn(r.rawCells());
                stats[toFetch.get(partition)] = decode(entryDecoder,cell);
                i++;
            }
        } catch (IOException e) {
            throw new ExecutionException(e);
        }
        return stats;
    }

    @Override
    public void invalidate(long conglomerateId,Collection<String> partitionsToInvalidate) {
        for(String partition:partitionsToInvalidate){
            tableStatsCache.invalidate(partition);
        }
    }

    protected TableStatisticsDescriptor decode(EntryDecoder cachedDecoder,Cell cell) throws IOException {
        assert cell!=null: "Programmer error: no data column returned!";
        MultiFieldDecoder decoder = cachedDecoder.get();
        decoder.set(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength());
        long conglomId = decoder.decodeNextLong();
        String partitionId = decoder.decodeNextString();

        cachedDecoder.set(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
        BitIndex index=cachedDecoder.getCurrentIndex();
        decoder=cachedDecoder.get();
        long timestamp;
        boolean isStale;
        boolean inProgress;
        timestamp=index.isSet(0)?decoder.decodeNextLong():System.currentTimeMillis();
        isStale=!index.isSet(1) || decoder.decodeNextBoolean();
        inProgress=index.isSet(2) && decoder.decodeNextBoolean();

        TableStatisticsDescriptor stats = null;
        if(index.isSet(3))
            stats=decode(decoder,conglomId,partitionId,timestamp,isStale,inProgress);
        if(!inProgress && stats!=null){
            /*
             * If the table is currently in progress, then we don't want to cache the value
             * because it's likely to change again soon. Otherwise, we can freely cache the
             * TableStats
             */
            tableStatsCache.put(partitionId,stats);
        }
        return stats;
    }

    private TableStatisticsDescriptor decode(MultiFieldDecoder decoder,
                                             long conglomId,
                                             String partitionId,
                                             long timestamp,
                                             boolean isStale,
                                             boolean inProgress){
        long rowCount = decoder.decodeNextLong();
        long size = decoder.decodeNextLong();
        int avgRowWidth = decoder.decodeNextInt();
        long queryCount = decoder.decodeNextLong();
        long localReadLat = decoder.decodeNextLong();
        long remoteReadLat = decoder.decodeNextLong();
        if(remoteReadLat<0){
            /*
             * We have a situation where it's difficult to obtain remote read
             * latency for base tables. In those cases, we store a number
             * <0 to indicate that we didn't collect it. When we detect
             * that, we will it in with at "configured remote latency"--e.g.
             * a (configurable) constant rate times the local latency
             */
            remoteReadLat = (long)(StatsConstants.remoteLatencyScaleFactor*localReadLat);
        }
        long writeLat = decoder.decodeNextLong();
        /*
         * Get the estimated latencies for opening and closing a scanner.
         *
         * If the number is <0, then we default to using the remote read latency as a measure.
         */
        long openLat = decoder.decodeNextLong();
        if(openLat<0)
            openLat = remoteReadLat;
        long closeLat = decoder.decodeNextLong();
        if(closeLat<0)
            closeLat = remoteReadLat;


        return new TableStatisticsDescriptor(conglomId,
                partitionId,
                timestamp,
                isStale,inProgress,
                rowCount,
                size,
                avgRowWidth,
                queryCount,
                localReadLat,
                remoteReadLat,
                writeLat,
                openLat,
                closeLat);
    }

    protected TableStatisticsDescriptor decode(EntryDecoder cachedDecoder,KeyValue cell){
        assert cell!=null:"Programmer error: no data column returned!";
        MultiFieldDecoder decoder=cachedDecoder.get();
        decoder.set(cell.key());
        long conglomId=decoder.decodeNextLong();
        String partitionId=decoder.decodeNextString();

        cachedDecoder.set(cell.value());
        BitIndex index=cachedDecoder.getCurrentIndex();
        decoder=cachedDecoder.get();
        long timestamp;
        boolean isStale;
        boolean inProgress;
        timestamp=index.isSet(0)?decoder.decodeNextLong():System.currentTimeMillis();
        isStale=!index.isSet(1) || decoder.decodeNextBoolean();
        inProgress=index.isSet(2) && decoder.decodeNextBoolean();

        TableStatisticsDescriptor stats = null;
        if(index.isSet(3))
            stats=decode(decoder,conglomId,partitionId,timestamp,isStale,inProgress);
        if(!inProgress && stats!=null){
            /*
             * If the table is currently in progress, then we don't want to cache the value
             * because it's likely to change again soon. Otherwise, we can freely cache the
             * TableStats
             */
            tableStatsCache.put(partitionId,stats);
        }
        return stats;
    }

    private class Refresher implements Runnable {
        private final TxnView refreshTxn;

        public Refresher(TxnView refreshTxn) {
            this.refreshTxn = refreshTxn;
        }

        @Override
        public void run() {
            //TODO -sf- use the transaction
            com.splicemachine.async.Scanner scanner = hbaseClient.newScanner(tableStatsConglom);
            Deferred<ArrayList<ArrayList<KeyValue>>> data = scanner.nextRows();
            ArrayList<ArrayList<KeyValue>> rowBatch;
            EntryDecoder decoder = new EntryDecoder();
            try {
                while((rowBatch = data.join())!=null){
                    for(List<KeyValue> row:rowBatch){
                        KeyValue kv = matchDataColumn(row);
                        TableStatisticsDescriptor stats = decode(decoder,kv); //caches the record
                    }
                    data = scanner.nextRows();
                }
            } catch (Exception e) {
                LOG.warn("Exception encountered while reading table statistics",e);
            }finally{
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
