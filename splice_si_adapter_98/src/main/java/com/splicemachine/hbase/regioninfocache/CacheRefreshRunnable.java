package com.splicemachine.hbase.regioninfocache;

import com.splicemachine.constants.SpliceConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.MetaScanAction;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;

import static com.splicemachine.utils.SpliceLogUtils.*;

/**
 * Task scheduled to periodically refresh cache.
 */
class CacheRefreshRunnable implements Runnable {

    /* Intentionally using same logger for all classes in this package. */
    private static final Logger LOG = Logger.getLogger(com.splicemachine.hbase.regioninfocache.HBaseRegionCache.class);

    private final Map<byte[], SortedSet<Pair<HRegionInfo, ServerName>>> regionCache;
    private final AtomicLong cacheUpdatedTimestamp;
    private final byte[] updateTableName;
    private final HConnection connection;

    CacheRefreshRunnable(Map<byte[], SortedSet<Pair<HRegionInfo, ServerName>>> regionCache,
                         HConnection connection,
                         AtomicLong cacheUpdatedTimestamp, byte[] updateTableName) {
        this.regionCache = regionCache;
        this.cacheUpdatedTimestamp = cacheUpdatedTimestamp;
        this.updateTableName = updateTableName;
        this.connection = connection;
    }

    CacheRefreshRunnable(Map<byte[], SortedSet<Pair<HRegionInfo, ServerName>>> regionCache,
                         AtomicLong cacheUpdatedTimestamp, byte[] updateTableName) {
        this.regionCache = regionCache;
        this.cacheUpdatedTimestamp = cacheUpdatedTimestamp;
        this.updateTableName = updateTableName;
        try {
            connection = HConnectionManager.getConnection(SpliceConstants.config);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        debug(LOG, "Refreshing region cache for table = %s", (updateTableName == null ? "ALL" : Bytes.toString(updateTableName)));
        doRun(startTime,10);

        /* Only update the refresh timestamp if we are loading for all tables */
        if (updateTableName == null) {
            cacheUpdatedTimestamp.set(System.currentTimeMillis());
        }
    }

    private void doRun(long startTime,int iteration) {
        if(iteration<0) {
            info(LOG,"Giving up refresh after many attempts");
            return;
        }
        RegionMetaScannerVisitor visitor = new RegionMetaScannerVisitor(updateTableName);
        try {
            if(updateTableName!=null)
                MetaScanAction.metaScan(visitor,connection,TableName.valueOf(updateTableName));
            else
                MetaScanAction.metaScan(visitor,connection, null);

            Map<byte[], SortedSet<Pair<HRegionInfo, ServerName>>> newRegionInfoMap = visitor.getRegionPairMap();
            regionCache.putAll(newRegionInfoMap);
            
            if(updateTableName!=null)
                connection.clearRegionCache(updateTableName);
            else
                connection.clearRegionCache(); // TODO: this clears server cache too - is that ok?
            
            if (LOG.isDebugEnabled()) {
            	LOG.debug(String.format("Updated %s region cache entries for table %s in %s ms ",
            		newRegionInfoMap.size(), (updateTableName == null ? "ALL" : Bytes.toString(updateTableName)), System.currentTimeMillis() - startTime));
            }
        } catch (IOException e) {
            if (e instanceof RegionServerStoppedException) {
                HBaseRegionCache.getInstance().shutdown();
                info(LOG, "The region cache is shutting down as the server has stopped");
            } else if(e instanceof NotServingRegionException){
                debug(LOG, "META region is not currently available, retrying");
                doRun(startTime,iteration-1);
            } else{
                error(LOG, "Unable to update region cache", e);
            }
        }
    }

}
