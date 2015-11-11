package com.splicemachine.derby.impl.stats;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionLifecycle;
import com.splicemachine.stats.ColumnStatistics;
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

    public HBaseColumnStatisticsStore(ScheduledExecutorService refreshThread) {
        this.refreshThread = refreshThread;
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
        return partitionIdToColumnMap;

    }

    @Override
    public void invalidate(long conglomerateId, Collection<String> partitions) {
        for(String partition:partitions){
            columnStatsCache.invalidate(partition);
        }
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
        }
    }
}
