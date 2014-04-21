package com.splicemachine.hbase.writer;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;

import com.splicemachine.hbase.RegionCache;

/**
 * @author Scott Fines
 *         Created on: 9/6/13
 */
public class SynchronousBucketingWriter extends BucketingWriter{
    private final BulkWriteAction.ActionStatusReporter statusMonitor;
    private ActionStatusMonitor monitor;

    protected SynchronousBucketingWriter(RegionCache regionCache, HConnection connection) {
        super(regionCache, connection);
        this.statusMonitor = new BulkWriteAction.ActionStatusReporter();
        this.monitor = new ActionStatusMonitor(statusMonitor);
    }

    @Override
    public Future<WriteStats> write(TableName tableName,
																		BulkWrite bulkWrite,
																		WriteConfiguration writeConfiguration) throws ExecutionException {
        WriteConfiguration countingWriteConfiguration = new CountingWriteConfiguration(writeConfiguration,statusMonitor);
        BulkWriteAction action = new BulkWriteAction(tableName,
                bulkWrite,
                regionCache,
                countingWriteConfiguration,
                connection,
                statusMonitor);
        statusMonitor.totalFlushesSubmitted.incrementAndGet();
        Exception e = null;
				WriteStats stats = null;
        try {
						stats = action.call();
				} catch (Exception error) {
           e = error;
        }
        return new FinishedFuture(e,stats);
    }

    @Override
    public void stopWrites() {
        //no-op
    }

    @Override
    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        ObjectName monitorName = new ObjectName("com.splicemachine.writer.synchronous:type=WriterStatus");
        mbs.registerMBean(monitor,monitorName);
    }

    private static class FinishedFuture implements Future<WriteStats> {
				private final WriteStats stats;
				private Exception e;

        public FinishedFuture(Exception e,WriteStats stats) {
						this.e = e;
						this.stats = stats;
				}
        @Override public boolean cancel(boolean mayInterruptIfRunning) { return false; }
        @Override public boolean isCancelled() { return false; }
        @Override public boolean isDone() { return true; }

        @Override
        public WriteStats get() throws InterruptedException, ExecutionException {
            if(e!=null) throw new ExecutionException(e);
						return stats;
        }

        @Override
        public WriteStats get(long timeout, TimeUnit unit) throws InterruptedException,
                ExecutionException, TimeoutException {
            return get();
        }
    }
}
