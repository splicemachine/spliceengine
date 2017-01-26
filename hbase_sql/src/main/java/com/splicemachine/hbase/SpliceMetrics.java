/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.hbase;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;

/**
 * @author Scott Fines
 * Created on: 4/10/13
 */
public class SpliceMetrics implements Updater {
    private final MetricsRecord taskMetrics;
    private final MetricsRecord jobMetrics;
    private final MetricsRecord writerMetrics;

    private MetricsRegistry writerRegistry = new MetricsRegistry();

    /*Table Writer metrics*/
    private final MetricsLongValue maxBufferHeapSizeWriter = new MetricsLongValue("maxBufferHeapSize",writerRegistry);
    private final MetricsIntValue maxBufferEntriesWriter = new MetricsIntValue("maxBufferEntries",writerRegistry);
    private final MetricsIntValue maxFlushesPerBufferWriter = new MetricsIntValue("maxFlushesPerBuffer",writerRegistry);
    private final MetricsIntValue outstandingCallBuffersWriter = new MetricsIntValue("outstandingCallBuffers",writerRegistry);
    private final MetricsIntValue pendingBufferFlushesWriter = new MetricsIntValue("pendingBufferFlushes",writerRegistry);
    private final MetricsIntValue executingBufferFlushesWriter = new MetricsIntValue("executingBufferFlushes",writerRegistry);
    private final MetricsIntValue runningWriteThreadsWriter = new MetricsIntValue("runningWriteThreads",writerRegistry);
    private final MetricsLongValue totalBufferFlushesWriter = new MetricsLongValue("totalBufferFlushes",writerRegistry);
    private final MetricsLongValue cachedTablesWriter = new MetricsLongValue("cachedTables",writerRegistry);
    private final MetricsLongValue cacheLastUpdatedWriter = new MetricsLongValue("cacheLastUpdated",writerRegistry);
    private final MetricsIntValue compressedWritesWriter = new MetricsIntValue("compressedWrites",writerRegistry);

    public SpliceMetrics() {
        MetricsContext context = MetricsUtil.getContext("splice");
        taskMetrics = MetricsUtil.createRecord(context,"tasks");
        jobMetrics = MetricsUtil.createRecord(context,"jobs");
        writerMetrics = MetricsUtil.createRecord(context,"writer");
        context.registerUpdater(this);
    }

    @Override
    public void doUpdates(MetricsContext context) {
        synchronized (this){
            //Get current view of the Task Scheduler

            maxBufferHeapSizeWriter.pushMetric(this.writerMetrics);
            maxBufferEntriesWriter.pushMetric(this.writerMetrics);
            maxFlushesPerBufferWriter.pushMetric(this.writerMetrics);
            outstandingCallBuffersWriter.pushMetric(this.writerMetrics);
            pendingBufferFlushesWriter.pushMetric(this.writerMetrics);
            executingBufferFlushesWriter.pushMetric(this.writerMetrics);
            totalBufferFlushesWriter.pushMetric(this.writerMetrics);
            runningWriteThreadsWriter.pushMetric(this.writerMetrics);
            cachedTablesWriter.pushMetric(this.writerMetrics);
            cacheLastUpdatedWriter.pushMetric(this.writerMetrics);
            compressedWritesWriter.pushMetric(this.writerMetrics);

        }
        this.taskMetrics.update();
        this.jobMetrics.update();
        this.writerMetrics.update();
    }
}
