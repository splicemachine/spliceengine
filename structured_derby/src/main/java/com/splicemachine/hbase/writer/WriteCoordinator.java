package com.splicemachine.hbase.writer;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.MonitoredThreadPool;
import com.splicemachine.hbase.RegionCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

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
        private final Semaphore pendingBuffersPermit;
        protected final byte[] tableName;
        protected final Writer.RetryStrategy retryStrategy;

        protected BufferListener(PreFlushHook preFlushHook, int maxPendingBuffers, byte[] tableName,Writer.RetryStrategy retryStrategy) {
            this.preFlushHook = preFlushHook;
            this.tableName = tableName;
            this.pendingBuffersPermit = new Semaphore(maxPendingBuffers);
            this.retryStrategy = retryStrategy;

        }

        @Override
        public long heapSize(KVPair element) {
            return element.getRow().length+element.getValue().length+1;
        }

        @Override
        public void bufferFlushed(List<KVPair> entries, CallBuffer<KVPair> source) throws Exception {
            entries = preFlushHook.transform(entries);
            //acquire a permit
            pendingBuffersPermit.acquire();

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
