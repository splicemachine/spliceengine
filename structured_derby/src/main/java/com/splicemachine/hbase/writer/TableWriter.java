package com.splicemachine.hbase.writer;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.hbase.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Global interface for writing to HBase.
 *
 * There are several issues with using a traditional HTableInterface which this
 * implementation attempts to solve:
 *
 * 1. Creating an HTableInterface is generally an expensive operation, involving synchronized
 * access to an HConnection, as well as reading the same fields out of Configuration.
 * 2. Once created, HTableInterface implementations are generally unable to be shared amongst
 * multiple writers without risking data-share issues. Specifically, when {@code autoFlush}
 * is disabled, HTableInterface implementations must maintain a single shared buffer of
 * writes.
 * 3. There is no region-awareness state that is maintained, so time must be spent on every
 * write asking the connection to lookup region information and validate it. While HConnection
 * implementations generally maintain a region-level cache, that cache is insufficient when
 * multiple tables are constructed, as they often won't share the same connection. The
 * end result is more time spent on preparing to write than necessary.
 * 4. HTableInterface implementations are synchronous by nature. It is therefore not
 * possible to offload a batch of writes to a separate thread for later writing.
 *
 * This implementation attempts to resolve the HTableInterface troubles by creating
 * a single, non-blocking, global Table management tool. It wraps a single HConnection,
 * which allows for better region caching at the connection level, and allows for both
 * synchronous and asynchronous buffer flushing. Additionally, it maintains a cache of
 * region information, and uses the {@link com.splicemachine.hbase.BatchProtocol} coprocessor endpoint to perform writes.
 * This cache is maintained and always used--there is never a situation in which the cache is not used,
 * although there are some situations in which the cache may be invalidated (either wholly or in part),
 * which may impose additional temporary latency on writes until the cache is repopulated.
 *
 * TableWriters deal out CallBuffers, which are isolated from one another,
 * allowing multiple different operations to use the same TableWriter
 * instance without fear of cross contamination.
 *
 * CallBuffers come in asynchronous and synchronous forms. Synchronous CallBuffers will
 * immediately write their buffer upon flushing, while Asynchronous buffers do not have
 * that guarantee. However, Asynchronous CallBuffers will wait for all flushed buffers
 * to complete writing before closing, which allows callers to ensure that all
 * elements have been written successfully.
 *
 * When using Asynchronous CallBuffers, it is important to be aware that there is only one pool
 * for writing all Buffers accessed through a single TableWriter instance. This has advantages--
 * thread management is simpler to manage globally, but it is possible for some operations to have additional
 * latency when attempting to flush a buffer, as it must wait for a thread to process its information. This can
 * be monitored via JMX by looking at PendingBufferFlushes. If a spike occurs in that, then the number of pending
 * buffers may be too small; one can adjust it on the fly by setting maxFlushesPerBuffer.
 *
 * The following configuration settings are used to configure the default (and initial) execution strategy for
 * TableWriters:
 *
 * 1.<em>hbase.client.write.buffer</em>: The maximum heap size of any given CallBuffer, in bytes. Puts are measured
 * using {@code put.heapSize()}, while deletes are measured by the length of their row key. This can
 * be adjusted through JMX by using the "maxBufferHeapSize" settings.
 * 2. <em>hbase.client.write.buffer.maxentres</em>: The maximum number of of entries in any given CallBuffer. This
 * can be adjusted through JMX by using the "maxBufferEntries" settings.
 * 3. <em>hbase.client.write.buffer.maxflushes</em>: The maximum number of flushes which can occur concurrently
 * for any given buffer, before blocking the caller.
 * 4. <em>hbase.htable.threads.max</em>: The maximum number of writer threads to use. Tuning this too low
 * may result in higher latency, because fewer writes will be able to run concurrently, which will result in
 * more pending buffer flushes, resulting in more time spent waiting. On the other hand, tuning this too high
 * may result in stability issues due to the overcreation of threads and the overuse of memory in storing pending
 * buffer flushes. The default is not to bound the number of threads. Throttling still occurs because of
 * bounded buffer flush counts, but only at an individual operation level.
 * 5. <em>hbase.htable.regioncache.updateinterval</em>: The length of time (in milliseconds) to wait before
 * forcibly refreshing the region cache.
 * 6. <em>hbase.htable.regioncache.expiration</em>: The length of time (in seconds) after writing a table's
 * region information into the cache before expiring it. This won't have any effect if
 * hbase.htable.regioncache.updateinterval is set sufficiently lower than this setting, as the cache
 * updater will reset the region cache before it has a chance to expire entries on existing tables.
 * However, once a table is dropped, this value will be used to prevent memory overrun in the cache.
 *
 * @author Scott Fines
 * Created on: 3/18/13
 */
public class TableWriter extends SpliceConstants implements WriterStatus{
    private static final Logger LOG = Logger.getLogger(TableWriter.class);

    private static final Class<BatchProtocol> batchProtocolClass = BatchProtocol.class;
    @SuppressWarnings("unchecked")
    private static final Class<? extends CoprocessorProtocol>[] protoClassArray = new Class[]{batchProtocolClass};

    private final MonitoredThreadPool writerPool;
    private final HConnection connection;

//    private final LoadingCache<Integer,Set<HRegionInfo>> regionCache;
//    private final ScheduledExecutorService cacheUpdater;
    private final RegionCache regionCache;
    private final Configuration configuration;

    /*
     * Manageable state information about handing out buffers
     */
    private volatile long maxHeapSize;
    private volatile long pause;
    private volatile int maxPendingBuffers;

    private final AtomicInteger pendingBufferFlushes = new AtomicInteger(0);
    private final AtomicInteger executingBufferFlushes = new AtomicInteger(0);
    private final AtomicInteger outstandingCallBuffers = new AtomicInteger(0);
    private final AtomicLong totalBufferFlushes = new AtomicLong(0);
    private final AtomicInteger runningWrites = new AtomicInteger(0);


    public static TableWriter create(Configuration configuration) throws IOException {
        assert configuration!=null;

        HConnection connection= HConnectionManager.getConnection(configuration);

        MonitoredThreadPool writerPool = MonitoredThreadPool.create();
        RegionCache regionCache = null;
        if(enableRegionCache){
            regionCache = RegionCache.create(cacheExpirationPeriod,cacheUpdatePeriod);
        }


        long pause = configuration.getLong(HConstants.HBASE_CLIENT_PAUSE,
                HConstants.DEFAULT_HBASE_CLIENT_PAUSE);

        int maxPendingBuffers = config.getInt(SpliceConstants.CONFIG_WRITE_BUFFER_MAX_FLUSHES, SpliceConstants.DEFAULT_MAX_PENDING_BUFFERS);

        return new TableWriter(writerPool,connection,regionCache, maxPendingBuffers,writeBufferSize,pause,configuration);
    }

    private TableWriter( MonitoredThreadPool writerPool,
                        HConnection connection,
                        RegionCache regionCache,
                        int maxPendingBuffers,
                        long maxHeapSize,
                        long pause,
                        Configuration configuration) {
        this.writerPool = writerPool;
        this.connection = connection;
        this.regionCache = regionCache;
        this.configuration = configuration;
        this.maxHeapSize = maxHeapSize;
        this.pause = pause;
        this.maxPendingBuffers = maxPendingBuffers;
    }

    public void start(){
        if(regionCache!=null)
            regionCache.start();
    }

    public void shutdown(){
        if(regionCache!=null)
            regionCache.shutdown();
        writerPool.shutdown();
    }


    /**
     * A Watcher around a Buffer Flush.
     *
     * This watcher serves two distinct purposes: adding an additional filter level to
     * the flush, and defining the failure semantics of an individual flush.
     *
     * The Filtering aspect is useful for when one may add an element to a buffer optimistically,
     * but may later wish to not write that element (e.g. add an entry to a buffer for one index, but
     * then take it out of the buffer later when a different index fails a constraint).
     *
     * The Failure semantics control comes in two different flavors: Global and Partial errors. A
     * global error is managed when an exception is encountered which prevented the writer from getting
     * back any metadata at all from the server. This can be due to any number of issues, most notably
     * network-level failures, and the reaction is often different than otherwise.
     *
     * A partial failure, on the other hand, is one where some of the writes succeeded but others failed. This is
     * the situation when, for example, a Region splits in the middle of writing data. In this situation, a
     * response is returned which has more than one failed row write. In those cases, a different failure semantic
     * may be desirable.
     *
     * Note: Failures do not indicate a failure of the entire Buffer, but rather some subset of the Buffer
     * which was tied to a single network call. That is, if a Buffer spans two regions, then there are two
     * places where a failure may occur--one for each region that is written to.
     */
    public static interface FlushWatcher{
        /**
         * The response the TableWriter should use for a failure.
         */
        public static enum Response{
            /**
             * Throw an exception. When the failure is global, this will mean to throw the global exception. When
             * local, it will mean throwing a RetriesExhaustedWithDetailsException listing all the errors.
             */
            THROW_ERROR,
            /**
             * Retry the failed process.
             */
            RETRY,
            /**
             * Return without throwing an exception or retrying. This is useful when the failure semantics are
             * managed externally to the TableWriter.
             */
            RETURN
        }

        /**
         * Filter/transform the mutations about to be flushed in batch.
         *
         * @param mutations the mutations to be flushed before transformation
         * @return the final mutations to be flushed
         * @throws Exception if the transformation goes wrong.
         */
        List<Mutation> preFlush(List<Mutation> mutations) throws Exception;

        /**
         * Called when an unexpected "global" error occurs, such as Connection
         * Reset, or some form of error that left no information about the success
         * or failure of rows.
         *
         * If Response.THROW_ERROR is returned, then the throwable that was passed
         * into the method will be thrown back to the buffer.
         *
         * If Response.RETRY is returned, the entire batch will be retried. Note that the TableWriter will
         * only retry a batch for a set number of attempts before it will fail regardless of the returned value
         * of this method. In that situation, the TableWriter will throw an empty RetriesExhaustedWithDetailsException.
         *
         * If Response.RETURN is returned, then the batch will not attempt a retry, and will
         * not throw an error, it will just move on to the next batch.
         *
         * @param t the error that was thrown
         * @return the Response the TableWriter should use
         * @throws Exception if something goes wrong.
         */
        Response globalError(Throwable t) throws Exception;

        /**
         * Called when an "expected" "partial" error occurs.
         *
         * A Partial error occurs when there was no network problem between the writer and the destination
         * table, but that <em>at the destination</em> something went wrong which the destination was unable
         * to resolve. In that situation, a list of the failed rows, and a list of the rows which were not even
         * attempted will be returned to the writer, and this method is called to determine what to do with them.
         *
         * If Response.THROW_ERROR is returned, then a RetriesExhaustedWithDetailsException will be thrown
         * enumerating the different error messages in the failed rows.
         *
         * If Response.RETRY is returned, then the Failed and not-run rows will be rebatched and attempted again.
         * Note that the TableWriter has a maximum bound on the number of retries it will make, no matter what
         * the output of this method is. If the number of retries are exhausted, then the TableWriter will throw
         * an empty RetriesExhaustedWithDetailsException.
         *
         * If Response.RETURN is returned, then the batch will not attempt a retry, and will not throw an error,
         * it will just move on to the next batch in the buffer flush (if there are any remaining).
         *
         * @param request the original mutation request.
         * @param response the response with failed rows
         * @return a Response dictating the actions of the TableWriter
         * @throws Exception if something goes wrong.
         */
        Response partialFailure(MutationRequest request,MutationResponse response) throws Exception;
    }

    /**
     * Get an Asynchronous buffer pointing to the specified table with the default Flush semantics.
     *
     * The returned buffer will flush <em>on a different thread</em> than the writer, and errors
     * from individual flushes will accumulate until close() is called on the returned buffer. All
     * errors that occur during flush will then be thrown.
     *
     * The returned buffer itself is <em>not</em> thread-safe.
     *
     * @param tableName the name of the table to write to
     * @return an asynchronous write buffer with the default Flush semantics.
     */
    public CallBuffer<Mutation> writeBuffer(byte[] tableName){
        outstandingCallBuffers.incrementAndGet();
        final Writer writer = new Writer(tableName, maxPendingBuffers, defaultFlushWatcher);
        return new UnsafeCallBuffer<Mutation>(maxHeapSize,maxBufferEntries,writer){
            @Override
            public void close() throws Exception {
                writer.ensureFlushed();
                super.close();
                outstandingCallBuffers.decrementAndGet();
            }
        };
    }

    /**
     * Get an Asynchronous buffer pointing to the specified table with the specified flush semantics.
     *
     * The returned buffer will flush <em>on a different thread</em> than the writer, and errors
     * from individual flushes will accumulate until close() is called on the returned buffer. All
     * errors that occur during flush will then be thrown. For this reason, it is recommended that the
     * FlushWatcher be thread-safe, to avoid concurrency issues during buffer flushes.
     *
     * The returned buffer itself is <em>not</em> thread-safe.
     *
     * @param tableName the name of the table to write to
     * @return an asynchronous write buffer with the default Flush semantics.
     */
    public CallBuffer<Mutation> writeBuffer(byte[] tableName,final FlushWatcher preFlushListener) throws Exception{
        outstandingCallBuffers.incrementAndGet();
        final Writer writer = new Writer(tableName,maxPendingBuffers,preFlushListener);
        return new UnsafeCallBuffer<Mutation>(maxHeapSize,maxBufferEntries,writer){
            @Override
            public void close() throws Exception {
                writer.ensureFlushed();
                super.close();
                outstandingCallBuffers.decrementAndGet();
            }
        };
    }

    /**
     * Creates a synchronous write buffer for the specified table with the default Flush semantics.
     *
     * When the returned buffer flushes, it will do so <em>on the adding thread</em>. If the flush is
     * desired to by asynchronous, then use {@link #writeBuffer(byte[])} instead.
     *
     * The returned buffer is <em>not</em> thread-safe.
     *
     * @param tableName the name of the table to write to
     * @return a writer buffer to the specified table with the default flush semantics.
     */
    public CallBuffer<Mutation> synchronousWriteBuffer(byte[] tableName){
        outstandingCallBuffers.incrementAndGet();
        final SynchronousWriter writer = new SynchronousWriter(tableName, defaultFlushWatcher);
        return new UnsafeCallBuffer<Mutation>(maxHeapSize,maxBufferEntries,writer){
            @Override
            public void close() throws Exception {
                super.close();
                outstandingCallBuffers.decrementAndGet();
            }
        };
    }

    /**
     * Creates a synchronous write buffer for the specified table with the specified FlushWatcher attached.
     *
     * When the returned buffer flushes, it will do so <em>on the writing thread</em>. If the flush is desired
     * to be asynchronous, then use {@link #writeBuffer(byte[], TableWriter.FlushWatcher)}
     * instead.
     *
     * The returned buffer is <em>not</em> thread safe, and must be externally synchronized.
     *
     * @param tableName the name of the table to write to
     * @param flushWatcher the flushWatcher to watch flushes with. This determines the failure semantics of
     *                     the buffer flushes.
     * @return a synchronous Write buffer with the specified flush semantics.
     */
    public CallBuffer<Mutation> synchronousWriteBuffer(byte[] tableName,FlushWatcher flushWatcher){
        outstandingCallBuffers.incrementAndGet();
        final SynchronousWriter writer = new SynchronousWriter(tableName,flushWatcher);
        return new UnsafeCallBuffer<Mutation>(maxHeapSize,maxBufferEntries,writer){
            @Override
            public void close() throws Exception {
                super.close();
                outstandingCallBuffers.decrementAndGet();
            }
        };
    }

    public MonitoredThreadPool getThreadPool(){
        return writerPool;
    }

    /*
     * A Default implementation of a FlushWatcher. This attempts to mimic
     * the failure semantics of the HBase client, and will retry anything not explicitly
     * marked as a DoNotRetryIOException, and does nothing to the mutations as they are
     * passed into the buffer.
     */
    private static final FlushWatcher defaultFlushWatcher = new FlushWatcher() {
        @Override
        public List<Mutation> preFlush(List<Mutation> mutations) throws Exception {
            return mutations;
        }

        @Override
        public Response globalError(Throwable t) throws Exception {
            //retry anything that isn't Retryable
            t = Throwables.getRootCause(t);
            if (t instanceof DoNotRetryIOException)
                return Response.THROW_ERROR;
            return Response.RETRY;
        }

        @Override
        public Response partialFailure(MutationRequest request,MutationResponse response) throws Exception {
            for(MutationResult error : response.getFailedRows().values()){
                if(!error.isRetryable()){
                    return Response.THROW_ERROR;
                }
            }
            return Response.RETRY;
        }
    };

/******************************************************************************************************************/
    /*MBean methods for JMX management*/
    @Override public long getMaxBufferHeapSize() { return maxHeapSize; }
    @Override public void setMaxBufferHeapSize(long newMaxHeapSize) { this.maxHeapSize = newMaxHeapSize; }
    @Override public int getMaxBufferEntries() { return maxBufferEntries; }
    @Override public void setMaxBufferEntries(int newMaxBufferEntries) { this.maxBufferEntries = newMaxBufferEntries; }
    @Override public int getMaxFlushesPerBuffer() { return maxPendingBuffers; }
    @Override public void setMaxFlushesPerBuffer(int newMaxFlushesPerBuffer){this.maxPendingBuffers = newMaxFlushesPerBuffer;}
    @Override public int getOutstandingCallBuffers() { return outstandingCallBuffers.get(); }
    @Override public int getPendingBufferFlushes() { return pendingBufferFlushes.get(); }
    @Override public int getExecutingBufferFlushes() { return executingBufferFlushes.get(); }
    @Override public long getTotalBufferFlushes() { return totalBufferFlushes.get(); }
    @Override public int getRunningWriteThreads() { return runningWrites.get(); }
    @Override public long getNumCachedTables() { return regionCache.size(); }
    @Override public boolean getCompressWrites(){ return compressWrites; }
    @Override public void setCompressWrites(boolean compressWrites) { this.compressWrites = compressWrites; }
    @Override public long getCacheLastUpdatedTimeStamp() { return regionCache.getUpdateTimestamp(); }

    @Override
    public int getNumCachedRegions(String tableName) {
        Set<HRegionInfo> regions;
        try {
            regions = regionCache.getRegions(tableName.getBytes());
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
        if(regions==null) return 0;
        return regions.size();
    }


/********************************************************************************************************************/
    /*private helper methods*/
    private abstract class BufferListener implements CallBuffer.Listener<Mutation>{
        protected final byte[] tableName;
        protected final FlushWatcher flushWatcher;

        protected BufferListener(byte[] tableName, FlushWatcher flushWatcher) {
            this.tableName = tableName;
            this.flushWatcher = flushWatcher;
        }

        @Override
        public long heapSize(Mutation element) {
            if(element instanceof Put) return ((Put)element).heapSize();
            else return element.getRow().length;
        }

        public abstract void threadFinished(AtomicInteger counter);
    }

    private class SynchronousWriter extends BufferListener{

        protected SynchronousWriter(byte[] tableName,FlushWatcher flushWatcher) {
            super(tableName, flushWatcher);
        }

        @Override
        public void threadFinished(AtomicInteger counter) {
            //no-op, since we're waiting for them all anyway
        }

        @Override
        public void bufferFlushed(List<Mutation> entries,CallBuffer<Mutation> buffer) throws Exception {
            List<MutationRequest> mutationRequests = bucketMutations(tableName,entries);
            final List<Future<Void>> futures =new ArrayList<Future<Void>>(mutationRequests.size());
            for(MutationRequest mutationRequest:mutationRequests){
                futures.add(writerPool.submit(new BufferWrite(mutationRequest,tableName,this,new AtomicInteger(1),flushWatcher)));
            }
            for(Future<Void> future:futures){
                future.get();
            }
        }
    }

    private class Writer extends BufferListener{
        private final List<Future<Void>> futures = Lists.newArrayList();
        private final Semaphore pendingBuffersPermits;

        private Writer(byte[] tableName, int maxPendingBuffers,FlushWatcher flushWatcher) {
            super(tableName, flushWatcher);
            this.pendingBuffersPermits = new Semaphore(maxPendingBuffers);
        }

        @Override
        public void bufferFlushed(List<Mutation> entries,CallBuffer<Mutation> buffer) throws Exception {
            //check if any prior flushes failed
            for(Future<Void> future:futures){
                if(future.isDone()){
                    future.get();
                }
            }
            /* transform the entries to be flushed according to the FlushWatcher's opinion. */
            entries = flushWatcher.preFlush(entries);
            /*
             * The write path is as follows:
             *
             * 1. take the Mutations list, and split it into one list for each region
             * 2. For each region:
             *  3. submit a processExecs request to the region with the mutations destined
             *  for that region asynchronously
             *
             * Since this is asynchronous, in certain unfortunate circumstances, we could
             * overrun our memory space by submitting too many buffers for processing (e.g. the
             * submission happens much faster than the writes out can complete). As this is clearly
             * a bad thing, we want to bound the number of buffers that can be submitted at any
             * one point in time before we must wait for a previous buffer to complete.
             *
             * To accomplish this, we use a combination Semaphore + Count down-y-ness. We first
             * acquire a permit from a semaphore (blocking if the semaphore has no more available permits).
             * We then submit an asynchronous task for each individual region to batch mutate it's entries.
             * As each region completes its writes, it counts down, and when that count down reaches zero, the
             * semaphore permit is released.
             *
             */
            totalBufferFlushes.incrementAndGet();

            /*
             * Tell the world that we've entered the pendingBuffer stage. That way, if
             * the permits are too low, we will see a spike in pending Buffers.
             */
            pendingBufferFlushes.incrementAndGet();
            pendingBuffersPermits.acquire();

            /*
             * Tell the world that we've entered the executingBuffer stage. That way, if
             * the number of available threads are too low, we'll see a large spike in executing
             * buffers, followed by a spike in the pendingBuffer stage as writes back up.
             */
            pendingBufferFlushes.decrementAndGet();
            executingBufferFlushes.incrementAndGet();

            writeBuffer(entries,0,new CopyOnWriteArrayList<Throwable>());
        }

        private void writeBuffer(List<Mutation> entries, final int tries, final List<Throwable> retryExceptions) throws Exception {
            if(tries > numRetries)
               throw new RetriesExhaustedWithDetailsException(retryExceptions,Collections.<Row>emptyList(),Collections.<String>emptyList());

            List<MutationRequest> bucketedMutations = bucketMutations(tableName,entries);
            final AtomicInteger runningCounts = new AtomicInteger(bucketedMutations.size());
            for(MutationRequest mutationToWrite: bucketedMutations){
                futures.add(writerPool.submit(new BufferWrite(mutationToWrite,tableName,this,runningCounts,flushWatcher)));
            }
        }

        public void ensureFlushed() throws Exception{
            /*
             * Cycle through the submitted futures and wait for them to return,
             * of explode if one of them did.
             */
            for(Future<Void> future:futures){
                try{
                    future.get();
                }catch(ExecutionException ee){
                    @SuppressWarnings("ThrowableResultOfMethodCallIgnored") Throwable t = Throwables.getRootCause(ee);
                    if(t instanceof Exception)
                        throw (Exception)t;
                    else throw ee;
                }
            }
        }

        public void threadFinished(AtomicInteger runningCounts) {
            int threadsStillRunning = runningCounts.decrementAndGet();
            if(threadsStillRunning<=0){
                executingBufferFlushes.decrementAndGet();
                pendingBuffersPermits.release();
            }
        }
    }

    private Pair<byte[],List<Mutation>>[] getBucketedMutations(byte[] tableKey,Collection<Mutation> mutations, int tries) throws Exception{
        if(tries <=0){
            throw new NotServingRegionException("Unable to find a region for mutations "
                    +mutations.size()+" mutations, unable to write buffer");
        }
        Set<HRegionInfo> regionInfos = regionCache.getRegions(tableKey);
        if(regionInfos.size()<=0){
              /*
             * We have some regions that either weren't alive or were splitting or something during
             * the time when the region loaded (if it loaded). So we need to invalidate the region cache
             * and try again. But only retry numRetries times. After that, just give up since we can't
             * write these correctly.
             */
            //wait for a backoff period, then try again
            Thread.sleep(getWaitTime(numRetries-tries+1));
            regionCache.invalidate(tableKey);
            return getBucketedMutations(tableKey,mutations,tries-1);
        }

        Pair<byte[],List<Mutation>>[] buckets = new Pair[regionInfos.size()];
        int pos=0;
        for(HRegionInfo regionInfo:regionInfos){
            buckets[pos] = Pair.<byte[],List<Mutation>>newPair(regionInfo.getStartKey(),Lists.<Mutation>newArrayList());
            pos++;
        }
        List<Mutation> regionLessMutations = Lists.newArrayListWithExpectedSize(0);
        for(Mutation mutation:mutations){
            if(mutation==null)continue; //skip null mutations

            byte[] row = mutation.getRow();
            int i=0;
            boolean less;
            Pair<byte[],List<Mutation>> bucket = null;
            do{
                Pair<byte[],List<Mutation>> mutationPair = buckets[i];
                int compare = Bytes.compareTo(mutationPair.getFirst(),row);
                less = compare<0;
                if(compare==0 || less){
                    bucket = mutationPair;
                }
                i++;
            }while(i<buckets.length && less);

            if(bucket==null)
                regionLessMutations.add(mutation);
            else
                bucket.getSecond().add(mutation);
        }
        if(regionLessMutations.size()>0){
            /*
             * We have some regions that either weren't alive or were splitting or something during
             * the time when the region loaded (if it loaded). So we need to invalidate the region cache
             * and try again. But only retry numRetries times. After that, just give up since we can't
             * write these correctly.
             */
            //wait for a backoff period, then try again
            Thread.sleep(getWaitTime(numRetries-tries+1));
            regionCache.invalidate(tableKey);
            return getBucketedMutations(tableKey,mutations,tries-1);
        }

        //DEBUG
        //make sure that there are no duplicate entries
//        Map<byte[],byte[]> dupMutations = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
//        for(Pair<byte[],List<Mutation>> bucket:buckets){
//            byte[] regionStart = bucket.getFirst();
//            for(Mutation mutation:bucket.getSecond()){
//                byte[] row = mutation.getRow();
//                if(dupMutations.containsKey(row)){
//                    byte[] oldRegion =  dupMutations.get(row);
//                    SpliceLogUtils.warn(LOG,"Row %s is present in bucket for region %s and region %s",
//                            BytesUtil.toHex(row),BytesUtil.toHex(oldRegion),BytesUtil.toHex(regionStart));
//                }
//                dupMutations.put(row,regionStart);
//            }
//        }

        return buckets;
    }

    private long getWaitTime(int tryNum) {
        long retryWait;
        if(tryNum>=HConstants.RETRY_BACKOFF.length)
            retryWait = HConstants.RETRY_BACKOFF[HConstants.RETRY_BACKOFF.length-1];
        else
            retryWait = HConstants.RETRY_BACKOFF[tryNum];
        return retryWait*pause;
    }

    private List<MutationRequest> bucketMutations(byte[] tableName,Collection<Mutation> mutations) throws Exception{
        Pair<byte[],List<Mutation>>[] bucketsMutations = getBucketedMutations(tableName,mutations,numRetries);
        List<MutationRequest> mutationRequests = Lists.newArrayListWithCapacity(bucketsMutations.length);
        for(Pair<byte[],List<Mutation>> bucket:bucketsMutations){
            byte[] regionStart = bucket.getFirst();
            List<Mutation> bucketedMutations = bucket.getSecond();
            if(bucketedMutations.size()<=0) continue;

            MutationRequest request = compressWrites? new SnappyMutationRequest(regionStart): new UncompressedMutationRequest(regionStart);
            request.addAll(bucketedMutations);
            mutationRequests.add(request);
        }

        return mutationRequests;
    }



    private static final AtomicLong writeIdGen = new AtomicLong(0l);
    private class BufferWrite implements Callable<Void>{
        /*
         * represents a write of a Mutation request for a Buffer. One of these
         * is created for each thread which performs writes.
         */
        private List<MutationRequest> mutationsToWrite;
        private final List<Throwable> retryExceptions = new ArrayList<Throwable>(0);
        private final byte[] tableName;
        private final BufferListener writer;
        private final AtomicInteger runningCounts;
        private final FlushWatcher flushWatcher;

        private final long id;

        private BufferWrite(MutationRequest mutationsToWrite,
                            byte[] tableName,
                            BufferListener writer,
                            AtomicInteger runningCounts,
                            FlushWatcher flushWatcher) {
            this.mutationsToWrite = Lists.newArrayList(mutationsToWrite);
            this.tableName = tableName;
            this.writer = writer;
            this.runningCounts = runningCounts;
            this.flushWatcher = flushWatcher;

            id = writeIdGen.incrementAndGet();
        }

        public void tryWrite(int tries,List<MutationRequest> mutationsToWrite) throws Exception {
            SpliceLogUtils.trace(LOG,"[BufferWrite %d]: tryWrite (%d buckets, %d tries)",id,mutationsToWrite.size(),tries);
        /*
         * The workflow is as follows:
         *
         * 1. Attempt to write the MutationRequest as handed to you. This is the
         * optimistic scenario that nothing has happened to Table regions between
         * when the MutationRequest was constructed and when this code actually gets
         * called. If the request succeeds, yippee! we're done
         * 2. If the initial request fails with a retry-able error, we need to back off and try again. To
         * do this, we wait for an exponentially increasing amount of time, then invalidate the region cache
         * for the table, re-split the Mutation into different mutation requests based on the new shape of the
         * Table topology, and retry synchronously. If we go too many times, then we fail this thread, which
         * will in turn fail the buffer write.
         */
            if(tries<=0){
                throw new RetriesExhaustedWithDetailsException(retryExceptions,Collections.<Row>emptyList(),Collections.<String>emptyList());
            }

            for(MutationRequest request:mutationsToWrite){
                SpliceLogUtils.trace(LOG,"[BufferWrite %d]: tryWrite Request %s",id,request);
            }
            for(MutationRequest mutationRequest:mutationsToWrite){
                doRetry(tries, mutationRequest);
            }
        }

        private void doRetry(int tries, MutationRequest mutationRequest) throws Exception {
            NoRetryExecRPCInvoker invoker = new NoRetryExecRPCInvoker(configuration,connection,
                    batchProtocolClass,tableName,mutationRequest.getRegionStartKey(),tries<numRetries);
            BatchProtocol instance = (BatchProtocol) Proxy.newProxyInstance(configuration.getClassLoader(),
                    protoClassArray, invoker);
            boolean thrown=false;
            try{
                SpliceLogUtils.trace(LOG,"[BufferWrite %d]: writeAttempt %s",id,mutationRequest);
                MutationResponse response = instance.batchMutate(mutationRequest);
                SpliceLogUtils.trace(LOG,"[BufferWrite %d]: received response %s",id,response);
                //deal with rows which failed due to a NotServingRegionException or WrongRegionException
                Map<Integer,MutationResult> failedRows = response.getFailedRows();
                if(failedRows!=null&&failedRows.size()>0){
                    SpliceLogUtils.debug(LOG,"[BufferWrite %d]: PartialFailure response %s, request %s",id,response,mutationRequest);
                    FlushWatcher.Response  partialRetry = flushWatcher.partialFailure(mutationRequest,response);
                    switch (partialRetry) {
                        case THROW_ERROR:
                            thrown=true;
                            throw parseIntoException(response);
                        case RETRY:
                            SpliceLogUtils.trace(LOG,"[BufferWrite %d]: PartialRetry %s",id, mutationRequest);
                            doPartialRetry(tries,mutationRequest,response);
                        default:
                            //return
                    }
                }
            }catch(Exception e){
                //if it's thrown inside the cache block deliberately, then
                //we need to rethrow it directly
                if(thrown)
                    throw e;

                SpliceLogUtils.debug(LOG,"[BufferWrite %d], GlobalFailure (%s) ",e,id,mutationRequest);
                FlushWatcher.Response response = flushWatcher.globalError(e);
                switch (response) {
                    case THROW_ERROR:
                        throw e;
                    case RETRY:
                        Thread.sleep(getWaitTime(numRetries-tries+1));
                        regionCache.invalidate(tableName);
                        //needs to be a mutatable list
                        tryWrite(tries-1,Lists.newArrayList(mutationRequest));
                        break;
                }
            }
        }

        private Exception parseIntoException(MutationResponse response) {
            Map<Integer,MutationResult> failedRows = response.getFailedRows();
            List<Throwable> errors = Lists.newArrayList();
            for(Integer failedRow:failedRows.keySet()){
                errors.add(Exceptions.fromString(failedRows.get(failedRow)));
            }
            return new RetriesExhaustedWithDetailsException(errors,Collections.<Row>emptyList(),Collections.<String>emptyList());
        }

        private void doPartialRetry(int tries, MutationRequest mutationRequest, MutationResponse response)  throws Exception {
            List<Integer> notRunRows = response.getNotRunRows();
            Map<Integer,MutationResult> failedRows = response.getFailedRows();
            List<Integer> rowsToRetry = Lists.newArrayListWithExpectedSize(notRunRows.size() + failedRows.size());
            rowsToRetry.addAll(notRunRows);
            rowsToRetry.addAll(failedRows.keySet());

            //add to error list

            Collection<MutationResult> mutationResults = failedRows.values();
            List<String> errorMsgs = new LinkedList<String>();

            for(MutationResult result : mutationResults){
                errorMsgs.add(result.getErrorMsg());
            }

            retryExceptions.add(new WriteFailedException(errorMsgs));

            List<Mutation> allMutations = mutationRequest.getMutations();
            List<Mutation> failedMutations = Lists.newArrayListWithCapacity(rowsToRetry.size());
            for(Integer rowToRetry:rowsToRetry){
                failedMutations.add(allMutations.get(rowToRetry));
            }

            if(failedMutations.size()>0){
                Thread.sleep(getWaitTime(numRetries-tries+1));
                regionCache.invalidate(tableName);

                tryWrite(tries - 1, bucketMutations(tableName, failedMutations));
            }
        }

        @Override
        public Void call() throws Exception {
            runningWrites.incrementAndGet();
            try{
                tryWrite(numRetries,mutationsToWrite);
            }finally{
                writer.threadFinished(runningCounts);
                runningWrites.decrementAndGet();
            }
            return null;
        }
    }
}
