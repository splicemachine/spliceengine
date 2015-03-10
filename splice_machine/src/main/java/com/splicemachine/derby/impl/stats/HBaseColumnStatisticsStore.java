package com.splicemachine.derby.impl.stats;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.async.AsyncAttributeHolder;
import com.splicemachine.async.HBaseClient;
import com.splicemachine.async.KeyValue;
import com.splicemachine.async.SortedMultiScanner;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionLifecycle;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.utils.kryo.KryoObjectInput;
import org.apache.hadoop.hbase.HConstants;
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
public class HBaseColumnStatisticsStore implements ColumnStatisticsStore {
    private static final Logger LOG = Logger.getLogger(HBaseColumnStatisticsStore.class);
    private final Cache<String,List<ColumnStatistics>> columnStatsCache;
    private ScheduledExecutorService refreshThread;
    private final byte[] columnStatsTable;
    private final HBaseClient hbaseClient;

    public HBaseColumnStatisticsStore(ScheduledExecutorService refreshThread,byte[] columnStatsTable, HBaseClient hbaseClient) {
        this.refreshThread = refreshThread;
        this.columnStatsTable = columnStatsTable;
        this.hbaseClient = hbaseClient;
        this.columnStatsCache = CacheBuilder.newBuilder().expireAfterWrite(StatsConstants.partitionCacheExpiration, TimeUnit.MILLISECONDS)
                .maximumSize(StatsConstants.partitionCacheSize).build();
    }

    public void start() throws ExecutionException {
        try {
            Txn txn = TransactionLifecycle.getLifecycleManager().beginTransaction(Txn.IsolationLevel.READ_UNCOMMITTED);
            refreshThread.scheduleAtFixedRate(new Refresher(txn),0l,StatsConstants.partitionCacheExpiration,TimeUnit.MILLISECONDS);
        } catch (IOException e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public Map<String, List<ColumnStatistics>> fetchColumnStats(TxnView txn, long conglomerateId, Collection<String> partitions) throws ExecutionException {
        Map<String,List<ColumnStatistics>> partitionIdToColumnMap = new HashMap<>();
        List<String> toFetch = new LinkedList<>();
        for(String partition:partitions){
            List<ColumnStatistics> colStats = columnStatsCache.getIfPresent(partition);
            if(colStats!=null){
                partitionIdToColumnMap.put(partition,colStats);
            }else{
                toFetch.add(partition);
            }
        }

        //fetch the missing keys
        Kryo kryo = SpliceKryoRegistry.getInstance().get();
        Map<String,List<ColumnStatistics>> toCacheMap = new HashMap<>(partitions.size()-partitionIdToColumnMap.size());
        try(SortedMultiScanner scanner = getScanner(txn, conglomerateId, toFetch)){
            List<KeyValue> nextRow;
            EntryDecoder decoder = new EntryDecoder();
            while((nextRow = scanner.nextKeyValues())!=null){
                KeyValue kv = matchDataColumn(nextRow);
                MultiFieldDecoder fieldDecoder = decoder.get();
                fieldDecoder.set(kv.key());
                long conglomId = fieldDecoder.decodeNextLong();
                String partitionId = fieldDecoder.decodeNextString();
                int colId = fieldDecoder.decodeNextInt();

                decoder.set(kv.value());
                byte[] data = decoder.get().decodeNextBytesUnsorted();
                KryoObjectInput koi = new KryoObjectInput(new Input(data),kryo);
                ColumnStatistics stats = (ColumnStatistics)koi.readObject();
                List<ColumnStatistics> colStats = partitionIdToColumnMap.get(partitionId);
                if(colStats==null){
                    colStats = new ArrayList<>(1);
                    partitionIdToColumnMap.put(partitionId,colStats);
                    toCacheMap.put(partitionId,colStats);
                }
                colStats.add(stats);
            }

            for(Map.Entry<String,List<ColumnStatistics>> toCache:toCacheMap.entrySet()){
                columnStatsCache.put(toCache.getKey(),toCache.getValue());
            }

        } catch (Exception e) {
            throw new ExecutionException(e);
        }finally{
            SpliceKryoRegistry.getInstance().returnInstance(kryo);
        }
        return partitionIdToColumnMap;
    }

    @Override
    public void invalidate(long conglomerateId, Collection<String> partitions) {
        for(String partition:partitions){
            columnStatsCache.invalidate(partition);
        }
    }

    private SortedMultiScanner getScanner(TxnView txn, long conglomId, List<String> toFetch) {
        byte[] encodedTxn = TransactionOperations.getOperationFactory().encode(txn);
        Map<String,byte[]> txnAttributeMap = new HashMap<>();
        txnAttributeMap.put(SIConstants.SI_TRANSACTION_ID_KEY, encodedTxn);
        txnAttributeMap.put(SIConstants.SI_NEEDED, SIConstants.SI_NEEDED_VALUE_BYTES);
        if(conglomId<0) {
            return getGlobalScanner(txnAttributeMap);
        }
        MultiFieldEncoder encoder = MultiFieldEncoder.create(2);
        encoder.encodeNext(conglomId).mark();
        List<com.splicemachine.async.Scanner> scanners = new ArrayList<>(toFetch.size());
        for(String partition:toFetch){
            encoder.reset();
            byte[] key = encoder.encodeNext(partition).build();
            byte[] stop = BytesUtil.unsignedCopyAndIncrement(key);
            com.splicemachine.async.Scanner scanner = hbaseClient.newScanner(columnStatsTable);
            scanner.setStartKey(key);
            scanner.setStopKey(stop);
            scanner.setFilter(new AsyncAttributeHolder(txnAttributeMap));
            scanners.add(scanner);
        }

        return new SortedMultiScanner(scanners,128,org.apache.hadoop.hbase.util.Bytes.BYTES_COMPARATOR, Metrics.noOpMetricFactory());
    }

    private SortedMultiScanner getGlobalScanner(Map<String,byte[]> attributes) {
        com.splicemachine.async.Scanner scanner = hbaseClient.newScanner(columnStatsTable);
        scanner.setStartKey(HConstants.EMPTY_START_ROW);
        scanner.setStopKey(HConstants.EMPTY_END_ROW);
        scanner.setFilter(new AsyncAttributeHolder(attributes));

        return new SortedMultiScanner(Arrays.asList(scanner),
                128,org.apache.hadoop.hbase.util.Bytes.BYTES_COMPARATOR, Metrics.noOpMetricFactory());
    }

    private KeyValue matchDataColumn(List<KeyValue> row) {
        for(KeyValue kv:row){
            if(Bytes.equals(kv.qualifier(), SIConstants.PACKED_COLUMN_BYTES)) return kv;
        }
        throw new IllegalStateException("Programmer error: Did not find a data column!");
    }

    private class Refresher implements Runnable {
        private final TxnView baseTxn; //should be read-only, and use READ_UNCOMMITTED isolation level

        public Refresher(TxnView baseTxn) {
            this.baseTxn = baseTxn;
        }

        @Override
        public void run() {
            Kryo kryo = SpliceKryoRegistry.getInstance().get();
            Map<String,List<ColumnStatistics>> toCacheMap = new HashMap<>();
            try (SortedMultiScanner scanner = getScanner(baseTxn, -1, null)) {
                List<KeyValue> nextRow;
                EntryDecoder decoder = new EntryDecoder();
                while ((nextRow = scanner.nextKeyValues()) != null) {
                    KeyValue kv = matchDataColumn(nextRow);
                    MultiFieldDecoder fieldDecoder = decoder.get();
                    fieldDecoder.set(kv.key());
                    long conglomId = fieldDecoder.decodeNextLong();
                    String partitionId = fieldDecoder.decodeNextString();
                    int colId = fieldDecoder.decodeNextInt();

                    decoder.set(kv.value());
                    byte[] data = decoder.get().decodeNextBytesUnsorted();
                    KryoObjectInput koi = new KryoObjectInput(new Input(data), kryo);
                    ColumnStatistics stats = (ColumnStatistics) koi.readObject();
                    List<ColumnStatistics> colStats = toCacheMap.get(partitionId);
                    if (colStats == null) {
                        colStats = new ArrayList<>(1);
                        toCacheMap.put(partitionId, colStats);
                    }
                    colStats.add(stats);
                }

                for (Map.Entry<String, List<ColumnStatistics>> toCache : toCacheMap.entrySet()) {
                    columnStatsCache.put(toCache.getKey(), toCache.getValue());
                }
            } catch (Exception e) {
                LOG.warn("Error encountered while refresshing Column Statistics Cache", e);
            }
        }
    }
}
