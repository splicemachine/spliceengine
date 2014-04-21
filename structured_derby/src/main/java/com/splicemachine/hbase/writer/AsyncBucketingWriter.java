package com.splicemachine.hbase.writer;

import com.splicemachine.hbase.MonitoredThreadPool;
import com.splicemachine.hbase.RegionCache;
import org.apache.hadoop.hbase.client.HConnection;
import javax.management.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Scott Fines
 * eated on: 8/8/13
 */
public class AsyncBucketingWriter extends BucketingWriter {
    private final MonitoredThreadPool writerPool;
    private final BulkWriteAction.ActionStatusReporter statusMonitor;
    private final ActionStatusMonitor monitor;

    public AsyncBucketingWriter(MonitoredThreadPool writerPool,
                                RegionCache regionCache,
                                HConnection connection) {
        super(regionCache, connection);
        this.writerPool = writerPool;
        this.statusMonitor = new BulkWriteAction.ActionStatusReporter();
        this.monitor = new ActionStatusMonitor(statusMonitor);
    }

    @Override
    public Future<WriteStats> write(byte[] tableName, BulkWrite bulkWrite, WriteConfiguration writeConfiguration) throws ExecutionException {
        WriteConfiguration countingWriteConfiguration = new CountingWriteConfiguration(writeConfiguration,statusMonitor);
        BulkWriteAction action = new BulkWriteAction(tableName,
                bulkWrite,
                regionCache,
                countingWriteConfiguration,
                connection,
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
        ObjectName monitorName = new ObjectName("com.splicemachine.writer.async:type=WriterStatus");
        mbs.registerMBean(monitor,monitorName);

        ObjectName poolName = new ObjectName("com.splicemachine.writer.async:type=ThreadPoolStatus");
        mbs.registerMBean(writerPool,poolName);
    }
}
