package com.splicemachine.hbase.writer;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.hbase.MonitoredThreadPool;
import com.splicemachine.hbase.RegionCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class WriteCoordinator {
    private final RegionCache regionCache;
    private final Writer writer;

    private volatile long maxHeapSize;
    private volatile int maxPendingBuffers;
    private volatile int maxEntries;
    private static final PreFlushHook noopFlushHook = new PreFlushHook() {
        @Override
        public List<KVPair> transform(List<KVPair> buffer) throws Exception {
            return buffer;
        }
    };
    public static final long pause = SpliceConstants.config.getLong(HConstants.HBASE_CLIENT_PAUSE,
            HConstants.DEFAULT_HBASE_CLIENT_PAUSE);

    private static final Writer.RetryStrategy defaultRetryStrategy = new Writer.RetryStrategy() {
        @Override
        public int getMaximumRetries() {
            return SpliceConstants.numRetries;
        }

        @Override
        public Writer.WriteResponse globalError(Throwable t) throws ExecutionException {
            if(Exceptions.shouldRetry(t))
                return Writer.WriteResponse.RETRY;
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

        @Override
        public long getPause() {
            return pause;
        }
    };


    public static WriteCoordinator create(Configuration config) throws IOException {
        assert config!=null;

        HConnection connection= HConnectionManager.getConnection(config);

        MonitoredThreadPool writerPool = MonitoredThreadPool.create();
        RegionCache regionCache;
//        if(SpliceConstants.enableRegionCache){
        //TODO -sf- make region caching optional
            regionCache = RegionCache.create(SpliceConstants.cacheExpirationPeriod,SpliceConstants.cacheUpdatePeriod);
//        }

        int maxPendingBuffers = config.getInt(SpliceConstants.CONFIG_WRITE_BUFFER_MAX_FLUSHES, SpliceConstants.DEFAULT_MAX_PENDING_BUFFERS);

        int maxEntries = SpliceConstants.maxBufferEntries;
        Writer writer = new AsyncWriter(writerPool,regionCache,connection);
        return new WriteCoordinator(regionCache,writer,maxPendingBuffers,SpliceConstants.writeBufferSize,maxEntries);
    }

    private WriteCoordinator(RegionCache regionCache, Writer writer,int maxPendingBuffers,long maxHeapSize,int maxEntries) {
        this.regionCache = regionCache;
        this.writer = writer;
        this.maxHeapSize = maxHeapSize;
        this.maxPendingBuffers = maxPendingBuffers;
        this.maxEntries = maxEntries;
    }

    public interface PreFlushHook{
        public List<KVPair> transform(List<KVPair> buffer) throws Exception;
    }

    public void start(){
        regionCache.start();
    }

    public void shutdown(){
        regionCache.shutdown();
        writer.stopWrites();
    }

    public TransactionalCallBuffer<KVPair> writeBuffer(byte[] tableName,String txnId){
        final BufferListener listener = new AsyncBufferListener(noopFlushHook,maxPendingBuffers,tableName, defaultRetryStrategy);
        return new TransactionalUnsafeCallBuffer<KVPair>(txnId,maxHeapSize,maxEntries,listener){
            @Override
            public void close() throws Exception {
                listener.ensureFlushed();
                super.close();
            }
        };
    }

    public TransactionalCallBuffer<KVPair> writeBuffer(byte[] tableName,String txnId,
                                                       PreFlushHook flushHook,Writer.RetryStrategy retryStrategy){
        final BufferListener listener = new AsyncBufferListener(flushHook,maxPendingBuffers,tableName, retryStrategy);
        return new TransactionalUnsafeCallBuffer<KVPair>(txnId,maxHeapSize,maxEntries,listener){
            @Override
            public void close() throws Exception {
                listener.ensureFlushed();
                super.close();
            }
        };
    }

    public TransactionalCallBuffer<KVPair> synchronousWriteBuffer(byte[] tableName,
                                                                  String txnId, PreFlushHook flushHook,
                                                                  Writer.RetryStrategy retryStrategy){
        final BufferListener listener = new SyncBufferListener(flushHook,maxPendingBuffers,tableName, retryStrategy);
        return new TransactionalUnsafeCallBuffer<KVPair>(txnId,maxHeapSize,maxEntries,listener);
    }

    private abstract class BufferListener implements CallBuffer.Listener<KVPair>{
        private final PreFlushHook preFlushHook;
        protected final byte[] tableName;
        protected final Writer.RetryStrategy retryStrategy;

        protected BufferListener(PreFlushHook preFlushHook, int maxPendingBuffers, byte[] tableName,Writer.RetryStrategy retryStrategy) {
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
            entries = preFlushHook.transform(entries);

            String transactionId = ((TransactionalCallBuffer<KVPair>) source).getTransactionId();
            doWrite(entries,transactionId);
        }

        public abstract void ensureFlushed() throws ExecutionException, InterruptedException;

        protected abstract void doWrite(List<KVPair> entries, String transactionId) throws ExecutionException;
    }

    private class SyncBufferListener extends BufferListener{
        private SyncBufferListener(PreFlushHook preFlushHook, int maxPendingBuffers, byte[] tableName, Writer.RetryStrategy retryStrategy) {
            super(preFlushHook, maxPendingBuffers, tableName, retryStrategy);
        }

        @Override
        public void ensureFlushed() throws ExecutionException, InterruptedException {
            //no-op
        }

        @Override
        protected void doWrite(List<KVPair> entries, String transactionId) throws ExecutionException {
            Future<Void> future = writer.write(tableName,entries,transactionId,retryStrategy);
            try {
                future.get();
            } catch (InterruptedException e) {
                throw new ExecutionException(e);
            }
        }
    }

    private class AsyncBufferListener extends BufferListener{
        private List<Future<Void>> futures = Lists.newArrayList();

        private AsyncBufferListener(PreFlushHook preFlushHook, int maxPendingBuffers, byte[] tableName, Writer.RetryStrategy retryStrategy) {
            super(preFlushHook, maxPendingBuffers, tableName, retryStrategy);
        }

        @Override
        public void ensureFlushed() throws ExecutionException, InterruptedException {
            for(Future<Void> future:futures){
                future.get();
            }
        }

        @Override
        protected void doWrite(List<KVPair> entries, String transactionId) throws ExecutionException {
            futures.add(writer.write(tableName,entries,transactionId,retryStrategy));
        }
    }

}
