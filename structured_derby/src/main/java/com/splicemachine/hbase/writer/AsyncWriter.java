package com.splicemachine.hbase.writer;

import com.splicemachine.hbase.MonitoredThreadPool;
import com.splicemachine.hbase.RegionCache;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.ipc.HBaseClient;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;

import javax.management.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Scott Fines
 * eated on: 8/8/13
 */
public class AsyncWriter extends AbstractWriter {
    private final MonitoredThreadPool writerPool;
    private final BulkWriteAction.ActionStatusReporter statusMonitor;
    private final WriterMonitor monitor = new WriterMonitor();

    public AsyncWriter(MonitoredThreadPool writerPool,
                          RegionCache regionCache,
                          HConnection connection) {
        super(regionCache, connection);
        this.writerPool = writerPool;
        this.statusMonitor = new BulkWriteAction.ActionStatusReporter();
    }

    @Override
    protected Future<Void> write(byte[] tableName, BulkWrite bulkWrite,RetryStrategy retryStrategy) throws ExecutionException {
        RetryStrategy countingRetryStrategy = new CountingRetryStrategy(retryStrategy,statusMonitor);
        BulkWriteAction action = new BulkWriteAction(tableName,
                bulkWrite,
                regionCache,
                countingRetryStrategy,
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
        ObjectName monitorName = new ObjectName("com.splicemachine.writer:type=WriterStatus");
        mbs.registerMBean(monitor,monitorName);

        ObjectName poolName = new ObjectName("com.splicemachine.writer:type=ThreadPoolStatus");
        mbs.registerMBean(writerPool,poolName);
    }

    private class WriterMonitor implements WriterStatus{

        @Override
        public int getExecutingBufferFlushes() {
            return statusMonitor.numExecutingFlushes.get();
        }

        @Override
        public long getTotalSubmittedFlushes() {
            return statusMonitor.totalFlushesSubmitted.get();
        }

        @Override
        public long getFailedBufferFlushes() {
            return statusMonitor.failedBufferFlushes.get();
        }

        @Override
        public long getNotServingRegionFlushes() {
            return statusMonitor.notServingRegionFlushes.get();
        }

        @Override
        public long getWrongRegionFlushes() {
            return statusMonitor.wrongRegionFlushes.get();
        }

        @Override
        public long getTimedOutFlushes() {
            return statusMonitor.timedOutFlushes.get();
        }

        @Override
        public long getGlobalErrors() {
            return statusMonitor.globalFailures.get();
        }

        @Override
        public long getPartialFailures() {
            return statusMonitor.partialFailures.get();
        }
    }

    private class CountingRetryStrategy implements RetryStrategy {
        private final RetryStrategy delegate;
        private final BulkWriteAction.ActionStatusReporter statusReporter;

        public CountingRetryStrategy(RetryStrategy retryStrategy, BulkWriteAction.ActionStatusReporter statusMonitor) {
            this.delegate = retryStrategy;
            this.statusReporter = statusMonitor;
        }

        @Override
        public int getMaximumRetries() {
            return delegate.getMaximumRetries();
        }

        @Override
        public WriteResponse globalError(Throwable t) throws ExecutionException {
            statusReporter.globalFailures.incrementAndGet();
            if(t instanceof HBaseClient.CallTimeoutException)
                statusReporter.timedOutFlushes.incrementAndGet();
            else if(t instanceof NotServingRegionException)
                statusReporter.notServingRegionFlushes.incrementAndGet();
            else if(t instanceof WrongRegionException)
                statusReporter.wrongRegionFlushes.incrementAndGet();
            return delegate.globalError(t);
        }

        @Override
        public WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
            statusReporter.partialFailures.incrementAndGet();
            //look for timeouts, not serving regions, wrong regions, and so forth
            boolean notServingRegion= false;
            boolean wrongRegion = false;
            boolean failed = false;
            boolean writeConflict = false;
            for(WriteResult writeResult:result.getFailedRows().values()){
                WriteResult.Code code = writeResult.getCode();
                switch (code) {
                    case FAILED:
                        failed=true;
                        break;
                    case WRITE_CONFLICT:
                        writeConflict=true;
                        break;
                    case NOT_SERVING_REGION:
                        notServingRegion = true;
                        break;
                    case WRONG_REGION:
                        wrongRegion = true;
                        break;
                }
            }
            if(notServingRegion)
                statusReporter.notServingRegionFlushes.incrementAndGet();
            if(wrongRegion)
                statusReporter.wrongRegionFlushes.incrementAndGet();
            if(writeConflict)
                statusReporter.writeConflictBufferFlushes.incrementAndGet();
            if(failed)
                statusReporter.failedBufferFlushes.incrementAndGet();
            return delegate.partialFailure(result,request);
        }

        @Override
        public long getPause() {
            return delegate.getPause();
        }
    }
}
