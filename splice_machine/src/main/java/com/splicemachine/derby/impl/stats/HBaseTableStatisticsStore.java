package com.splicemachine.derby.impl.stats;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.catalog.TableStatisticsDescriptor;
import com.splicemachine.derby.impl.storage.ClientResultScanner;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.MeasuredResultScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnOperationFactory;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionLifecycle;
import com.splicemachine.storage.EntryDecoder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
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
    private final TableStatsDecoder tableStatsDecoder;

    public HBaseTableStatisticsStore(ScheduledExecutorService refreshThread,
                                     byte[] tableStatsConglom,
                                     TableStatsDecoder tableStatsDecoder) {
        this.refreshThread = refreshThread;
        this.tableStatsConglom = tableStatsConglom;
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

    private MeasuredResultScanner getScanner(TxnView txn, long conglomId, List<String> toFetch) throws IOException{
    	// TODO (wjk): enhance this to provide more general support like in HBaseColumnStatisticsStore
        byte[] encodedTxn = TransactionOperations.getOperationFactory().encode(txn);
        Scan s = new Scan();
        s.setStartRow(HConstants.EMPTY_START_ROW);
        s.setStopRow(HConstants.EMPTY_END_ROW);
        s.setAttribute(SIConstants.SI_TRANSACTION_ID_KEY,encodedTxn);
        s.setAttribute(SIConstants.SI_NEEDED,SIConstants.SI_NEEDED_VALUE_BYTES);
        ClientResultScanner results=new ClientResultScanner(tableStatsConglom,s,false,Metrics.noOpMetricFactory());
        try{
            results.open();
        }catch(StandardException e){
            throw new IOException(e);
        }
        return results;
//        return getGlobalScanner(txnAttributeMap);
    }

//    private com.splicemachine.async.Scanner getGlobalScanner(Map<String,byte[]> attributes) {
//        com.splicemachine.async.Scanner scanner = hbaseClient.newScanner(tableStatsConglom);
//        scanner.setStartKey(HConstants.EMPTY_START_ROW);
//        scanner.setStopKey(HConstants.EMPTY_END_ROW);
//        scanner.setFilter(new AsyncAttributeHolder(attributes));
//        return scanner;
//    }

    private class Refresher implements Runnable {
        private final TxnView refreshTxn;

        public Refresher(TxnView refreshTxn) {
            this.refreshTxn = refreshTxn;
        }

        public void run() {
        	boolean isTrace = LOG.isTraceEnabled();
            EntryDecoder decoder = new EntryDecoder();
            try(MeasuredResultScanner scanner = getScanner(refreshTxn,-1,null)) {
                Result rowBatch;
                while((rowBatch = scanner.next())!=null){
                    TableStatisticsDescriptor stats = tableStatsDecoder.decode(rowBatch,decoder);
                    if (stats != null && !stats.isInProgress()) {
                        if (isTrace) LOG.trace(String.format("Refreshing cached table stats for conglomerate %d (rowCount=%d), partition %s",
                                stats.getConglomerateId(), stats.getRowCount(), stats.getPartitionId()));
                        tableStatsCache.put(stats.getPartitionId(), stats);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Error encountered while refreshing Table Statistics Cache", e);
            }
        }

        @SuppressWarnings("ForLoopReplaceableByForEach")
        private Cell matchDataColumn(Cell[] row) {
            byte[] qualifier = SIConstants.PACKED_COLUMN_BYTES;
            for(int i=0;i<row.length;i++){
                Cell kv = row[i];
                if(Bytes.equals(kv.getQualifierArray(),kv.getQualifierOffset(),kv.getQualifierLength(),
                        qualifier,0,qualifier.length))
                    return kv;

            }
            throw new IllegalStateException("Programmer error: Did not find a data column!");
        }
    }
}
