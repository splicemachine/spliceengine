package com.splicemachine.compactions;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.hbase.SpliceCompactionUtils;
import com.splicemachine.hbase.TransactionsWatcher;
import com.splicemachine.si.impl.Transaction;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.server.FlushLifeCycleTrackerWithConfig;
import com.splicemachine.si.impl.server.PurgeConfigBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.DefaultStoreFlusher;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.MemStoreSnapshot;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;

import java.io.IOException;
import java.sql.Driver;
import java.sql.Timestamp;
import java.util.List;

public class SpliceDefaultFlusher extends DefaultStoreFlusher {
    public SpliceDefaultFlusher(Configuration conf, HStore store) throws IOException {
        super(conf, store);
    }

    public List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushId,
                                    MonitoredTask status, ThroughputController throughputController,
                                    FlushLifeCycleTracker tracker) throws IOException {
        SIDriver driver=SIDriver.driver();
        PurgeConfigBuilder purgeConfig = new PurgeConfigBuilder();
        SConfiguration conf = driver.getConfiguration();
        if (conf.getOlapCompactionAutomaticallyPurgeDeletedRows()) {
            purgeConfig.purgeDeletesDuringFlush();
        } else {
            purgeConfig.noPurgeDeletes();
        }
        purgeConfig.transactionLowWatermark(SpliceCompactionUtils.getTxnLowWatermark(store));
        purgeConfig.purgeUpdates(conf.getOlapCompactionAutomaticallyPurgeOldUpdates());
        return super.flushSnapshot(snapshot, cacheFlushId, status, throughputController,
                new FlushLifeCycleTrackerWithConfig(tracker, purgeConfig.build()));
    }
}
