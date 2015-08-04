package com.splicemachine.derby.impl.stats;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.async.AsyncAttributeHolder;
import com.splicemachine.async.HBaseClient;
import com.splicemachine.async.KeyValue;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.derby.iapi.catalog.TableStatisticsDescriptor;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnOperationFactory;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionLifecycle;
import com.splicemachine.storage.EntryDecoder;
import com.stumbleupon.async.Deferred;
import org.apache.hadoop.hbase.HConstants;
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
    private final TableStatsDecoder tableStatsDecoder;

    public HBaseTableStatisticsStore(ScheduledExecutorService refreshThread,
                                     byte[] tableStatsConglom,
                                     HBaseClient hbaseClient,
                                     TableStatsDecoder tableStatsDecoder) {
        this.refreshThread = refreshThread;
        this.tableStatsConglom = tableStatsConglom;
        this.hbaseClient = hbaseClient;
        this.tableStatsCache = CacheBuilder.newBuilder().expireAfterWrite(StatsConstants.partitionCacheExpiration, TimeUnit.MILLISECONDS)
                .maximumSize(StatsConstants.partitionCacheSize).build();
        this.tableStatsDecoder = tableStatsDecoder;
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
    	boolean isTrace = LOG.isTraceEnabled();
    	TableStatisticsDescriptor[] stats = new TableStatisticsDescriptor[partitionsToFetch.size()];
        Map<String,Integer> toFetch = new HashMap<>();
        for(int i=0;i<partitionsToFetch.size();i++){
            String partition = partitionsToFetch.get(i);
            TableStatisticsDescriptor tStats = tableStatsCache.getIfPresent(partition);
            if(tStats!=null) {
            	if (isTrace) LOG.trace(String.format("Table stats found in cache (size=%d) for conglomerate %d (rowCount=%d), partition %s",
        			tableStatsCache.size(), conglomerateId, tStats.getRowCount(), partition));
            	stats[i] = tStats;
            }else{
				if (isTrace) LOG.trace(String.format("Table stats NOT found in cache (size=%d) for conglomerate %d, partition %s",
					tableStatsCache.size(), conglomerateId, partition));
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
                if(r==null||r.isEmpty()) {
                	if (isTrace) LOG.trace(String.format("No table stats found for conglomerate %d, partition %s",
                    	conglomerateId, partition));
                	continue;
                }
                int index = toFetch.get(partition);
                stats[index] = tableStatsDecoder.decode(r,entryDecoder);
            	if (isTrace) LOG.trace(String.format("Adding table stats to cache for conglomerate %d (rowCount=%d), partition %s",
            		conglomerateId, stats[index].getRowCount(), partition));
                tableStatsCache.put(partition, stats[index]);
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

    private com.splicemachine.async.Scanner getScanner(TxnView txn, long conglomId, List<String> toFetch) {
    	// TODO (wjk): enhance this to provide more general support like in HBaseColumnStatisticsStore
        byte[] encodedTxn = TransactionOperations.getOperationFactory().encode(txn);
        Map<String,byte[]> txnAttributeMap = new HashMap<>();
        txnAttributeMap.put(SIConstants.SI_TRANSACTION_ID_KEY, encodedTxn);
        txnAttributeMap.put(SIConstants.SI_NEEDED, SIConstants.SI_NEEDED_VALUE_BYTES);
        return getGlobalScanner(txnAttributeMap);
    }

    private com.splicemachine.async.Scanner getGlobalScanner(Map<String,byte[]> attributes) {
        com.splicemachine.async.Scanner scanner = hbaseClient.newScanner(tableStatsConglom);
        scanner.setStartKey(HConstants.EMPTY_START_ROW);
        scanner.setStopKey(HConstants.EMPTY_END_ROW);
        scanner.setFilter(new AsyncAttributeHolder(attributes));
        return scanner;
    }

    private class Refresher implements Runnable {
        private final TxnView refreshTxn;

        public Refresher(TxnView refreshTxn) {
            this.refreshTxn = refreshTxn;
        }

        public void run() {
        	boolean isTrace = LOG.isTraceEnabled();
            //SortedMultiScanner scanner = getScanner(refreshTxn, -1, null);
            com.splicemachine.async.Scanner scanner = getScanner(refreshTxn, -1, null);
            //com.splicemachine.async.Scanner scanner = hbaseClient.newScanner(tableStatsConglom);
            Deferred<ArrayList<ArrayList<KeyValue>>> data = scanner.nextRows();
            ArrayList<ArrayList<KeyValue>> rowBatch;
            EntryDecoder decoder = new EntryDecoder();
            try {
                while((rowBatch = data.join())!=null){
                    for(List<KeyValue> row:rowBatch){
                        KeyValue kv = matchDataColumn(row);
                        TableStatisticsDescriptor stats = tableStatsDecoder.decode(kv,decoder);
                        if (stats != null && !stats.isInProgress()) {
							if (isTrace) LOG.trace(String.format("Refreshing cached table stats for conglomerate %d (rowCount=%d), partition %s",
								stats.getConglomerateId(), stats.getRowCount(), stats.getPartitionId()));
							tableStatsCache.put(stats.getPartitionId(), stats);
                        }
                    }
                    data = scanner.nextRows();
                }
            } catch (Exception e) {
                LOG.warn("Error encountered while refreshing Table Statistics Cache", e);
            } finally {
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
