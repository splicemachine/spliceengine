/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.compactions;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.hbase.SpliceCompactionUtils;
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
