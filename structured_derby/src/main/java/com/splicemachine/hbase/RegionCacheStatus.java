package com.splicemachine.hbase;

import javax.management.MXBean;

/**
 * @author Scott Fines
 *         Created on: 8/14/13
 */
@MXBean
public interface RegionCacheStatus {

    long getLastUpdatedTimestamp();

    void updateCache();

    int getNumCachedRegions(String tableName);

    long getNumCachedTables();

    long getCacheUpdatePeriod();
}
