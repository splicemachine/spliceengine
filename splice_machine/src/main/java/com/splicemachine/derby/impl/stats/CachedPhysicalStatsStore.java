/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
