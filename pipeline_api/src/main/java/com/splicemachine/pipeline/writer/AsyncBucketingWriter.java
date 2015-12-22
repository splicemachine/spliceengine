package com.splicemachine.pipeline.writer;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.pipeline.PipelineConfiguration;
import com.splicemachine.pipeline.api.*;
import com.splicemachine.pipeline.client.ActionStatusReporter;
import com.splicemachine.pipeline.client.BulkWriteAction;
import com.splicemachine.pipeline.client.BulkWrites;
import com.splicemachine.pipeline.threadpool.MonitoredThreadPool;
import com.splicemachine.pipeline.config.CountingWriteConfiguration;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.pipeline.writerstatus.ActionStatusMonitor;
import com.splicemachine.utils.Sleeper;

import javax.management.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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

    public AsyncBucketingWriter(MonitoredThreadPool writerPool,
                                BulkWriterFactory writerFactory,
                                PipelineExceptionFactory exceptionFactory,
                                PartitionFactory partitionFactory) {
        this.writerPool = writerPool;
        this.statusMonitor = new ActionStatusReporter();
        this.monitor = new ActionStatusMonitor(statusMonitor);
        this.exceptionFactory = exceptionFactory;
        this.writerFactory = writerFactory;
        this.partitionFactory = partitionFactory;
    }

    @Override
    public Future<WriteStats> write(byte[] tableName, BulkWrites bulkWrites, WriteConfiguration writeConfiguration) throws ExecutionException {
        WriteConfiguration countingWriteConfiguration = new CountingWriteConfiguration(writeConfiguration, statusMonitor,exceptionFactory);
        BulkWriteAction action = new BulkWriteAction(tableName,
                bulkWrites,
                countingWriteConfiguration,
                statusMonitor,
                writerFactory,
                exceptionFactory,
                partitionFactory,
                Sleeper.THREAD_SLEEPER);
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
