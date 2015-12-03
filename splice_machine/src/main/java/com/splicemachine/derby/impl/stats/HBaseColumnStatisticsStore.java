package com.splicemachine.derby.impl.stats;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.impl.storage.ClientResultScanner;
import com.splicemachine.derby.impl.storage.ProbeDistributedScanner;
import com.splicemachine.derby.impl.storage.SpliceResultScanner;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.MeasuredResultScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionLifecycle;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.utils.kryo.KryoObjectInput;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
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
public class HBaseColumnStatisticsStore implements ColumnStatisticsStore {
    private static final Logger LOG = Logger.getLogger(HBaseColumnStatisticsStore.class);
    @SuppressWarnings("rawtypes")
	private final Cache<String,List<ColumnStatistics>> columnStatsCache;
    private ScheduledExecutorService refreshThread;
    private final byte[] columnStatsTable;

    public HBaseColumnStatisticsStore(ScheduledExecutorService refreshThread,byte[] columnStatsTable) {
        this.refreshThread = refreshThread;
        this.columnStatsTable = columnStatsTable;
        this.columnStatsCache = CacheBuilder.newBuilder().expireAfterWrite(StatsConstants.partitionCacheExpiration, TimeUnit.MILLISECONDS)
                .maximumSize(StatsConstants.partitionCacheSize).build();
    }

    public void start() throws ExecutionException {
        try {
            TxnView txn = TransactionLifecycle.getLifecycleManager().beginTransaction(Txn.IsolationLevel.READ_UNCOMMITTED);
            refreshThread.scheduleAtFixedRate(new Refresher(txn),0l,StatsConstants.partitionCacheExpiration,TimeUnit.MILLISECONDS);
        } catch (IOException e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public Map<String, List<ColumnStatistics>> fetchColumnStats(TxnView txn, long conglomerateId, Collection<String> partitions) throws ExecutionException {
    	boolean isTrace = LOG.isTraceEnabled();
        Map<String,List<ColumnStatistics>> partitionIdToColumnMap = new HashMap<>();
        List<String> toFetch = new LinkedList<>();
        for(String partition:partitions){
            List<ColumnStatistics> colStats = columnStatsCache.getIfPresent(partition);
            if(colStats!=null){
            	if (isTrace) LOG.trace(String.format("Column stats found in cache for conglomerate %d, partition %s", conglomerateId, partition));
                partitionIdToColumnMap.put(partition,colStats);
            }else{
            	if (isTrace) LOG.trace(String.format("Column stats NOT found in cache for conglomerate %d, partition %s", conglomerateId, partition));
                toFetch.add(partition);
            }
        }
        if(toFetch.size()<=0) return partitionIdToColumnMap; //nothing to fetch

        //fetch the missing keys
        Kryo kryo = SpliceKryoRegistry.getInstance().get();
        Map<String,List<ColumnStatistics>> toCacheMap = new HashMap<>(partitions.size()-partitionIdToColumnMap.size());
        try(MeasuredResultScanner scanner = getScanner(txn, conglomerateId, toFetch)){
            Result nextRow;
            EntryDecoder decoder = new EntryDecoder();
            while((nextRow = scanner.next())!=null){
                Cell kv = matchDataColumn(nextRow.rawCells());
                MultiFieldDecoder fieldDecoder = decoder.get();
                fieldDecoder.set(kv.getRowArray(),kv.getRowOffset(),kv.getRowLength());
                long conglomId = fieldDecoder.decodeNextLong();
                String partitionId = fieldDecoder.decodeNextString();
                int colId = fieldDecoder.decodeNextInt();

                decoder.set(kv.getValueArray(),kv.getValueOffset(),kv.getValueLength());
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
            	if (isTrace) LOG.trace(String.format("Adding column stats to cache for partition %s", toCache.getKey()));
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

    private MeasuredResultScanner getScanner(TxnView txn, long conglomId, List<String> toFetch) throws IOException{
        byte[] encodedTxn = TransactionOperations.getOperationFactory().encode(txn);
        if(conglomId<0) {
            return getGlobalScanner(encodedTxn);
        }
        MultiFieldEncoder encoder = MultiFieldEncoder.create(2);
        encoder.encodeNext(conglomId).mark();
        SpliceResultScanner[] scanners = new SpliceResultScanner[toFetch.size()];
        int i=0;
        for(String partition:toFetch){
            encoder.reset();
            byte[] key = encoder.encodeNext(partition).build();
            byte[] stop = BytesUtil.unsignedCopyAndIncrement(key);

            Scan scan = new Scan();
            scan.setStartRow(key);
            scan.setStopRow(stop);
            scan.setAttribute(SIConstants.SI_TRANSACTION_ID_KEY,encodedTxn);
            scan.setAttribute(SIConstants.SI_NEEDED,SIConstants.SI_NEEDED_VALUE_BYTES);

            ClientResultScanner results=new ClientResultScanner(columnStatsTable,scan,false,Metrics.noOpMetricFactory());
            try{
                results.open();
            }catch(StandardException e){
                throw new IOException(e);
            }
            scanners[i] =results;
            i++;
        }

        ProbeDistributedScanner results=new ProbeDistributedScanner(scanners);
        try{
            results.open();
        }catch(StandardException e){
            throw new IOException(e);
        }
        return results;
    }

    private MeasuredResultScanner getGlobalScanner(byte[] encodedTxn) throws IOException{
        Scan scan = new Scan();
        scan.setStartRow(HConstants.EMPTY_START_ROW);
        scan.setStopRow(HConstants.EMPTY_END_ROW);
        scan.setAttribute(SIConstants.SI_TRANSACTION_ID_KEY,encodedTxn);
        scan.setAttribute(SIConstants.SI_NEEDED,SIConstants.SI_NEEDED_VALUE_BYTES);

        ClientResultScanner results=new ClientResultScanner(columnStatsTable,scan,false,Metrics.noOpMetricFactory());
        try{
            results.open();
        }catch(StandardException e){
            throw new IOException(e); //shouldn't happen
        }
        return results;
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    private Cell matchDataColumn(Cell[] row) {
        byte[] dataQualifier=SIConstants.PACKED_COLUMN_BYTES;
        for(int i=0;i<row.length;i++){
            Cell c = row[i];
            if(Bytes.equals(c.getQualifierArray(),c.getQualifierOffset(),c.getQualifierLength(),dataQualifier,0,dataQualifier.length)) return c;
        }
        throw new IllegalStateException("Programmer error: Did not find a data column!");
    }

    @SuppressWarnings("unused")
	private class Refresher implements Runnable {
        private final TxnView baseTxn; //should be read-only, and use READ_UNCOMMITTED isolation level

        public Refresher(TxnView baseTxn) {
            this.baseTxn = baseTxn;
        }

        @Override
        public void run() {
        	boolean isTrace = LOG.isTraceEnabled();
            Kryo kryo = SpliceKryoRegistry.getInstance().get();
			Map<String,List<ColumnStatistics>> toCacheMap = new HashMap<>();
            
            try(MeasuredResultScanner scanner = getScanner(baseTxn, -1, null)) {
                Result nextRow;
                EntryDecoder decoder = new EntryDecoder();
                while ((nextRow = scanner.next()) != null) {
                    Cell kv = matchDataColumn(nextRow.rawCells());
                    MultiFieldDecoder fieldDecoder = decoder.get();
                    fieldDecoder.set(kv.getRowArray(),kv.getRowOffset(),kv.getRowLength());
                    long conglomId = fieldDecoder.decodeNextLong();
                    String partitionId = fieldDecoder.decodeNextString();
                    int colId = fieldDecoder.decodeNextInt();

                    decoder.set(kv.getValueArray(),kv.getValueOffset(),kv.getValueLength());
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
                	if (isTrace) LOG.trace(String.format("Refreshing cached column stats for partition %s", toCache.getKey()));
                    columnStatsCache.put(toCache.getKey(), toCache.getValue());
                }
            } catch (Exception e) {
                LOG.warn("Error encountered while refreshing Column Statistics Cache", e);
	        }
        }
    }
}
