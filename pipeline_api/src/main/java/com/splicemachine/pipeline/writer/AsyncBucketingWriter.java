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

package com.splicemachine.pipeline.writer;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.configuration.PipelineConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.pipeline.api.BulkWriterFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.api.Writer;
import com.splicemachine.pipeline.client.ActionStatusReporter;
import com.splicemachine.pipeline.client.BulkWriteAction;
import com.splicemachine.pipeline.client.BulkWrites;
import com.splicemachine.pipeline.config.CountingWriteConfiguration;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.pipeline.threadpool.MonitoredThreadPool;
import com.splicemachine.pipeline.writerstatus.ActionStatusMonitor;

/**
 * @author Scott Fines
 *         eated on: 8/8/13
 */
public class AsyncBucketingWriter implements Writer {

    private final MonitoredThreadPool writerPool;
    private final ActionStatusReporter statusMonitor;
    private final ActionStatusMonitor monitor;
    private final PipelineExceptionFactory exceptionFactory;
    private final BulkWriterFactory writerFactory;
    private final PartitionFactory partitionFactory;
    private final Clock clock;

    public AsyncBucketingWriter(MonitoredThreadPool writerPool,
                                BulkWriterFactory writerFactory,
                                PipelineExceptionFactory exceptionFactory,
                                PartitionFactory partitionFactory,
                                Clock clock) {
        this.writerPool = writerPool;
        this.statusMonitor = new ActionStatusReporter();
        this.monitor = new ActionStatusMonitor(statusMonitor);
        this.exceptionFactory = exceptionFactory;
        this.writerFactory = writerFactory;
        this.partitionFactory = partitionFactory;
        this.clock = clock;
    }

    @Override
    public Future<WriteStats> write(byte[] tableName, BulkWrites bulkWrites, WriteConfiguration writeConfiguration) throws ExecutionException {
        assert bulkWrites!=null:"Bulk Writes Passed in are null";
        WriteConfiguration countingWriteConfiguration = new CountingWriteConfiguration(writeConfiguration, statusMonitor,exceptionFactory);
        BulkWriteAction action = new BulkWriteAction(tableName,
                bulkWrites,
                countingWriteConfiguration,
                statusMonitor,
                writerFactory,
                exceptionFactory,
                partitionFactory,
                clock);
        statusMonitor.totalFlushesSubmitted.incrementAndGet();
        return writerPool.submit(action);
    }

    @Override
    public void stopWrites() {
        writerPool.shutdown();
    }

    @Override
    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        mbs.registerMBean(monitor, new ObjectName(PipelineConfiguration.WRITER_STATUS_OBJECT_LOCATION));
        mbs.registerMBean(writerPool, new ObjectName(PipelineConfiguration.THREAD_POOL_STATUS_LOCATION));
    }
}
