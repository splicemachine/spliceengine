package com.splicemachine.derby.impl.stats;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.db.iapi.sql.dictionary.TableStatisticsDescriptor;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionLifecycle;
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

    public HBaseTableStatisticsStore(ScheduledExecutorService refreshThread) {
        this.refreshThread = refreshThread;
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
        return stats;
    }

    @Override
    public void invalidate(long conglomerateId,Collection<String> partitionsToInvalidate) {
        for(String partition:partitionsToInvalidate){
            tableStatsCache.invalidate(partition);
        }
    }

    private class Refresher implements Runnable {
        private final TxnView refreshTxn;

        public Refresher(TxnView refreshTxn) {
            this.refreshTxn = refreshTxn;
        }

        public void run() {
        	boolean isTrace = LOG.isTraceEnabled();
        }

    }
}
