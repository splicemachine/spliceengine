package com.splicemachine.pipeline.writer;

import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.impl.ActionStatusReporter;
import com.splicemachine.pipeline.impl.BulkWriteAction;
import com.splicemachine.pipeline.impl.BulkWrites;
import com.splicemachine.pipeline.threadpool.MonitoredThreadPool;
import com.splicemachine.pipeline.writeconfiguration.CountingWriteConfiguration;
import com.splicemachine.pipeline.writerstatus.ActionStatusMonitor;
import javax.management.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Scott Fines
 *         eated on: 8/8/13
 */
public class AsyncBucketingWriter extends BucketingWriter {

    private final MonitoredThreadPool writerPool;
    private final ActionStatusReporter statusMonitor;
    private final ActionStatusMonitor monitor;

    public AsyncBucketingWriter(MonitoredThreadPool writerPool) {
        this.writerPool = writerPool;
        this.statusMonitor = new ActionStatusReporter();
        this.monitor = new ActionStatusMonitor(statusMonitor);
    }

    @Override
    public Future<WriteStats> write(byte[] tableName, BulkWrites bulkWrites, WriteConfiguration writeConfiguration) throws ExecutionException {
        WriteConfiguration countingWriteConfiguration = new CountingWriteConfiguration(writeConfiguration, statusMonitor);
        BulkWriteAction action = new BulkWriteAction(tableName,
                bulkWrites,
                countingWriteConfiguration,
                statusMonitor);
        statusMonitor.totalFlushesSubmitted.incrementAndGet();
        return writerPool.submit(action);
    }

    @Override
    public void stopWrites() {
        writerPool.shutdown();
    }

    @Override
    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        mbs.registerMBean(monitor, new ObjectName(WRITER_STATUS_OBJECT_LOCATION));
        mbs.registerMBean(writerPool, new ObjectName(THREAD_POOL_STATUS_LOCATION));
    }
}
