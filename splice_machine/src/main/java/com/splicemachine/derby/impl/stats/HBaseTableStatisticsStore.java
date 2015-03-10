package com.splicemachine.derby.impl.stats;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.async.HBaseClient;
import com.splicemachine.async.KeyValue;
import com.splicemachine.constants.SIConstants;
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
    private final Cache<String,TableStats> tableStatsCache;
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
    public TableStats[] fetchTableStatistics(TxnView txn, long conglomerateId, List<String> partitionsToFetch) throws ExecutionException {
        TableStats[]  stats =new TableStats[partitionsToFetch.size()];
        Map<String,Integer> toFetch = new HashMap<>();
        for(int i=0;i<partitionsToFetch.size();i++){
            String partition = partitionsToFetch.get(i);
            TableStats tStats = tableStatsCache.getIfPresent(partition);
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

    protected TableStats decode(EntryDecoder cachedDecoder,Cell cell) throws IOException {
        assert cell!=null: "Programmer error: no data column returned!";
        MultiFieldDecoder decoder = cachedDecoder.get();
        decoder.set(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength());
        long conglomId = decoder.decodeNextLong();
        String partitionId = decoder.decodeNextString();

        cachedDecoder.set(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
        decoder = cachedDecoder.get();
        long timestamp = decoder.decodeNextLong();
        boolean isStale = decoder.decodeNextBoolean();
        boolean inProgress= decoder.decodeNextBoolean();
        long rowCount = decoder.decodeNextLong();
        long size = decoder.decodeNextLong();
        int avgRowWidth = decoder.decodeNextInt();
        long queryCount = decoder.decodeNextLong();

        TableStats stats = new TableStats(conglomId,partitionId,timestamp,rowCount,size,avgRowWidth,queryCount);
        if(!inProgress){
            /*
             * If the table is currently in progress, then we don't want to cache the value
             * because it's likely to change again soon. Otherwise, we can freely cache the
             * TableStats
             */
            tableStatsCache.put(partitionId,stats);
        }
        return stats;
    }

    protected TableStats decode(MultiFieldDecoder cachedDecoder,KeyValue cell) {
        assert cell!=null: "Programmer error: no data column returned!";
        cachedDecoder.set(cell.key());
        long conglomId = cachedDecoder.decodeNextLong();
        String partitionId = cachedDecoder.decodeNextString();

        cachedDecoder.set(cell.value());
        long timestamp = cachedDecoder.decodeNextLong();
        boolean isStale = cachedDecoder.decodeNextBoolean();
        boolean inProgress= cachedDecoder.decodeNextBoolean();
        long rowCount = cachedDecoder.decodeNextLong();
        long size = cachedDecoder.decodeNextLong();
        int avgRowWidth = cachedDecoder.decodeNextInt();
        long queryCount = cachedDecoder.decodeNextLong();

        TableStats stats = new TableStats(conglomId,partitionId,timestamp,rowCount,size,avgRowWidth,queryCount);
        if(!inProgress){
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
            com.splicemachine.async.Scanner scanner = hbaseClient.newScanner(tableStatsConglom);
            Deferred<ArrayList<ArrayList<KeyValue>>> data = scanner.nextRows();
            ArrayList<ArrayList<KeyValue>> rowBatch;
            MultiFieldDecoder decoder = MultiFieldDecoder.create();
            try {
                while((rowBatch = data.join())!=null){
                    for(List<KeyValue> row:rowBatch){
                        KeyValue kv = matchDataColumn(row);
                        TableStats stats = decode(decoder,kv); //caches the record
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
