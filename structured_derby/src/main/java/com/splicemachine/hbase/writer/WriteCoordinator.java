package com.splicemachine.hbase.writer;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.sql.execute.index.IndexNotSetUpException;
import com.splicemachine.hbase.MonitoredThreadPool;
import com.splicemachine.hbase.RegionCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;

import javax.management.*;
import java.io.IOException;
import java.net.ConnectException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class WriteCoordinator {
    private final RegionCache regionCache;
    private final Writer asynchronousWriter;
    private final Writer synchronousWriter;

    private final Monitor monitor;
    private static final PreFlushHook noopFlushHook = new PreFlushHook() {
        @Override
        public List<KVPair> transform(List<KVPair> buffer) throws Exception {
            return buffer;
        }
    };
    public static final long pause = SpliceConstants.config.getLong(HConstants.HBASE_CLIENT_PAUSE,
            HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    public static PreFlushHook noOpFlushHook = new PreFlushHook() {
        @Override
        public List<KVPair> transform(List<KVPair> buffer) throws Exception {
            return buffer;
        }
    };

    public static WriteCoordinator create(Configuration config) throws IOException {
        assert config!=null;

        HConnection connection= HConnectionManager.getConnection(config);

        MonitoredThreadPool writerPool = MonitoredThreadPool.create();
        //TODO -sf- make region caching optional
        RegionCache regionCache = RegionCache.create(SpliceConstants.cacheExpirationPeriod,SpliceConstants.cacheUpdatePeriod);

        int maxEntries = SpliceConstants.maxBufferEntries;
        Writer writer = new AsyncBucketingWriter(writerPool,regionCache,connection);
        Writer syncWriter = new SynchronousBucketingWriter(regionCache,connection);
        Monitor monitor = new Monitor(SpliceConstants.writeBufferSize,maxEntries,SpliceConstants.numRetries,pause);
        return new WriteCoordinator(regionCache,writer, syncWriter, monitor);
    }

    private WriteCoordinator(RegionCache regionCache,
                             Writer asynchronousWriter,
                             Writer synchronousWriter, Monitor monitor) {
        this.regionCache = regionCache;
        this.asynchronousWriter = asynchronousWriter;
        this.synchronousWriter = synchronousWriter;
        this.monitor = monitor;
    }

    /**
     * Used to register this coordinator with JMX
     * @param mbs
     */
    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        ObjectName coordinatorName = new ObjectName("com.splicemachine.writer:type=WriteCoordinatorStatus");
        mbs.registerMBean(monitor,coordinatorName);

        regionCache.registerJMX(mbs);

        asynchronousWriter.registerJMX(mbs);
        synchronousWriter.registerJMX(mbs);
    }

    public interface PreFlushHook{
        public List<KVPair> transform(List<KVPair> buffer) throws Exception;
    }

    public void start(){
        regionCache.start();
    }

    public void shutdown(){
        regionCache.shutdown();
        asynchronousWriter.stopWrites();
    }

    public PipingWriteBuffer pipedWriteBuffer(byte[] tableName, String txnId){
        monitor.outstandingBuffers.incrementAndGet();
        //TODO -sf- add a global maxHeapSize rather than multiplying by 10
        return new PipingWriteBuffer(tableName,txnId, asynchronousWriter,regionCache,defaultRetryStrategy,
                new BufferConfiguration() {
                    @Override
                    public long getMaxHeapSize() {
                        return monitor.getMaxHeapSize()*10;
                    }

                    @Override
                    public int getMaxEntries() {
                        return monitor.getMaxEntries();
                    }
                });
    }

    public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName,String txnId){
        monitor.outstandingBuffers.incrementAndGet();

        return new PipingWriteBuffer(tableName,txnId, asynchronousWriter,regionCache,defaultRetryStrategy,monitor){
            @Override
            public void close() throws Exception {
                monitor.outstandingBuffers.decrementAndGet();
                super.close();
            }
        };
    }

    public RecordingCallBuffer<KVPair> writeBuffer(byte[] tableName,String txnId,
                                                       PreFlushHook flushHook,Writer.RetryStrategy retryStrategy){
        monitor.outstandingBuffers.incrementAndGet();
        return new PipingWriteBuffer(tableName,txnId, asynchronousWriter,regionCache, flushHook, retryStrategy,monitor) {
            @Override
            public void close() throws Exception {
                monitor.outstandingBuffers.decrementAndGet();
                super.close();
            }
        };
    }

    public RecordingCallBuffer<KVPair> synchronousWriteBuffer(byte[] tableName,
                                                                  String txnId, PreFlushHook flushHook,
                                                                  Writer.RetryStrategy retryStrategy){
        monitor.outstandingBuffers.incrementAndGet();
        return new PipingWriteBuffer(tableName,txnId,synchronousWriter,regionCache, flushHook, retryStrategy,monitor) {
            @Override
            public void close() throws Exception {
                monitor.outstandingBuffers.decrementAndGet();
                super.close();
            }
        };
    }

    private abstract class BufferListener implements CallBuffer.Listener<KVPair>{
        private final PreFlushHook preFlushHook;
        protected final byte[] tableName;
        protected final Writer.RetryStrategy retryStrategy;
        private long totalFlushes = 0l;

        protected BufferListener(PreFlushHook preFlushHook, byte[] tableName,Writer.RetryStrategy retryStrategy) {
            this.preFlushHook = preFlushHook;
            this.tableName = tableName;
            this.retryStrategy = retryStrategy;
        }

        @Override
        public long heapSize(KVPair element) {
            return element.getRow().length+element.getValue().length+1;
        }

        @Override
        public void bufferFlushed(List<KVPair> entries, CallBuffer<KVPair> source) throws Exception {
            totalFlushes++;
            entries = preFlushHook.transform(entries);

            String transactionId = ((TransactionalCallBuffer<KVPair>) source).getTransactionId();
            doWrite(entries,transactionId);
        }

        public abstract void ensureFlushed() throws ExecutionException, InterruptedException;

        protected abstract void doWrite(List<KVPair> entries, String transactionId) throws ExecutionException;

        public long getTotalFlushes() {
            return totalFlushes;
        }
    }

    private class SyncBufferListener extends BufferListener{
        private SyncBufferListener(PreFlushHook preFlushHook, byte[] tableName, Writer.RetryStrategy retryStrategy) {
            super(preFlushHook, tableName, retryStrategy);
        }

        @Override
        public void ensureFlushed() throws ExecutionException, InterruptedException {
            //no-op
        }

        @Override
        protected void doWrite(List<KVPair> entries, String transactionId) throws ExecutionException {
            Future<Void> future = asynchronousWriter.write(tableName,entries,transactionId,retryStrategy);
            try {
                future.get();
            } catch (InterruptedException e) {
                throw new ExecutionException(e);
            }
        }
    }

    private class AsyncBufferListener extends BufferListener{
        private List<Future<Void>> futures = Lists.newArrayList();

        private AsyncBufferListener(PreFlushHook preFlushHook, byte[] tableName, Writer.RetryStrategy retryStrategy) {
            super(preFlushHook, tableName, retryStrategy);
        }

        @Override
        public void ensureFlushed() throws ExecutionException, InterruptedException {
            for(Future<Void> future:futures){
                future.get();
            }
        }

        @Override
        protected void doWrite(List<KVPair> entries, String transactionId) throws ExecutionException {
            /*
             * Before writing this, check the previous buffers that have been flushed. Remove any successful
             * flushes from the list to avoid memory leaks, and throw errors on any Future that has failed for
             * any reason.
             */
            Iterator<Future<Void>> futureIterator = futures.iterator();
            while(futureIterator.hasNext()){
                Future<Void> future = futureIterator.next();
                if(future.isDone()){
                    try {
                        future.get(); //check for errors
                    } catch (InterruptedException e) {
                        throw new ExecutionException(e);
                    }
                    futureIterator.remove();
                }
            }
            //submit the flush
            futures.add(asynchronousWriter.write(tableName, entries, transactionId, retryStrategy));
        }
    }

    private static class Monitor implements WriteCoordinatorStatus,BufferConfiguration{
        private volatile long maxHeapSize;
        private volatile int maxEntries;
        private volatile int maxRetries;

        private AtomicInteger outstandingBuffers = new AtomicInteger(0);
        private volatile long pauseTime;

        private Monitor(long maxHeapSize, int maxEntries, int maxRetries,long pauseTime) {
            this.maxHeapSize = maxHeapSize;
            this.maxEntries = maxEntries;
            this.maxRetries = maxRetries;
            this.pauseTime = pauseTime;
        }

        @Override public long getMaxBufferHeapSize() { return maxHeapSize; }
        @Override public void setMaxBufferHeapSize(long newMaxHeapSize) { this.maxHeapSize = newMaxHeapSize; }
        @Override public int getMaxBufferEntries() { return maxEntries; }
        @Override public void setMaxBufferEntries(int newMaxBufferEntries) { this.maxEntries = newMaxBufferEntries; }
        @Override public int getOutstandingCallBuffers() { return outstandingBuffers.get(); }
        @Override public int getMaximumRetries() { return maxRetries; }
        @Override public void setMaximumRetries(int newMaxRetries) { this.maxRetries = newMaxRetries; }
        @Override public long getPauseTime() { return pauseTime; }
        @Override public void setPauseTime(long newPauseTimeMs) { this.pauseTime = newPauseTimeMs; }
        @Override public long getMaxHeapSize() { return maxHeapSize; }
        @Override public int getMaxEntries() { return maxEntries; }
    }

    private final Writer.RetryStrategy defaultRetryStrategy = new Writer.RetryStrategy() {
        @Override public int getMaximumRetries() { return monitor.getMaximumRetries(); }
        @Override public long getPause() { return monitor.getPauseTime(); }

        @Override
        public Writer.WriteResponse globalError(Throwable t) throws ExecutionException {
            if(t instanceof ConnectException
                    || t instanceof WrongRegionException
                    || t instanceof IndexNotSetUpException
                    || t instanceof NotServingRegionException
                    || t instanceof RegionTooBusyException)
                return Writer.WriteResponse.RETRY;
            else
                return Writer.WriteResponse.THROW_ERROR;
        }

        @Override
        public Writer.WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
            Map<Integer,WriteResult> failedRows = result.getFailedRows();
            for(WriteResult writeResult:failedRows.values()){
                if(!writeResult.canRetry())
                    return Writer.WriteResponse.THROW_ERROR;
            }
            return Writer.WriteResponse.RETRY;
        }
    };
}
