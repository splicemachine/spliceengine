package com.splicemachine.hbase.writer;

import com.splicemachine.hbase.RegionCache;
import org.apache.hadoop.hbase.client.HConnection;

import javax.management.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    public Future<Void> write(byte[] tableName,
                              BulkWrite bulkWrite,
                              RetryStrategy retryStrategy) throws ExecutionException {
        RetryStrategy countingRetryStrategy = new CountingRetryStrategy(retryStrategy,statusMonitor);
        BulkWriteAction action = new BulkWriteAction(tableName,
                bulkWrite,
                regionCache,
                countingRetryStrategy,
                connection,
                statusMonitor);
        statusMonitor.totalFlushesSubmitted.incrementAndGet();
        Exception e = null;
        try {
            action.call();
        } catch (Exception error) {
           e = error;
        }
        return new FinishedFuture(e);
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

    private class FinishedFuture implements Future<Void> {
        private Exception e;

        public FinishedFuture(Exception e) { this.e = e; }
        @Override public boolean cancel(boolean mayInterruptIfRunning) { return false; }
        @Override public boolean isCancelled() { return false; }
        @Override public boolean isDone() { return true; }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            if(e!=null) throw new ExecutionException(e);
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) throws InterruptedException,
                ExecutionException, TimeoutException {
            return get();
        }
    }
}
