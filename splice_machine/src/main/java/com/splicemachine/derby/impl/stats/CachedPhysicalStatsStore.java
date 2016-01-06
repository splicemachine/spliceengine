package com.splicemachine.derby.impl.stats;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.sql.dictionary.PhysicalStatsDescriptor;
import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public class CachedPhysicalStatsStore implements PhysicalStatisticsStore {
    private static final Logger LOG = Logger.getLogger(CachedPhysicalStatsStore.class);
    private final Cache<String,PhysicalStatsDescriptor> physicalStatisticsCache;

    public CachedPhysicalStatsStore() {
        SConfiguration config =EngineDriver.driver().getConfiguration();
        this.physicalStatisticsCache = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getLong(StatsConfiguration.PARTITION_CACHE_EXPIRATION), TimeUnit.MILLISECONDS)
                .build();
    }

    @Override
    public List<PhysicalStatsDescriptor> allPhysicalStats() {
        List<PhysicalStatsDescriptor> descriptors = new ArrayList<>((int)physicalStatisticsCache.size());
        for(PhysicalStatsDescriptor descriptor:physicalStatisticsCache.asMap().values()){
            descriptors.add(descriptor);
        }
        return descriptors;
    }

}
