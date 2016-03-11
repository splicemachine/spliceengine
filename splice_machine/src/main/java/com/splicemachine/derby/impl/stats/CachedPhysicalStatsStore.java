package com.splicemachine.derby.impl.stats;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.sparkproject.guava.cache.Cache;
import org.sparkproject.guava.cache.CacheBuilder;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.sql.dictionary.PhysicalStatsDescriptor;

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
                .expireAfterWrite(config.getPartitionCacheExpiration(), TimeUnit.MILLISECONDS)
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
