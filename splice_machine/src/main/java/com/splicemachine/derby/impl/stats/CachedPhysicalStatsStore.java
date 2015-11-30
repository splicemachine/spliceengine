package com.splicemachine.derby.impl.stats;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.db.iapi.sql.dictionary.PhysicalStatsDescriptor;
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

    public CachedPhysicalStatsStore(ScheduledExecutorService refreshThread) {
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

    private class Refresher implements Runnable {

        @Override
        public void run() {
        }
    }
}
