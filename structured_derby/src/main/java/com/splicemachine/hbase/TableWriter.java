package com.splicemachine.hbase;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.ExecRPCInvoker;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
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
 * region information, and uses the {@link BatchProtocol} coprocessor endpoint to perform writes.
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
public class TableWriter implements WriterStatus{
    private static final Logger LOG = Logger.getLogger(TableWriter.class);
    private static final Logger CACHE_LOG = Logger.getLogger(RegionCacheLoader.class);

    private static final Class<BatchProtocol> batchProtocolClass = BatchProtocol.class;
    private static final Class<? extends CoprocessorProtocol>[] protoClassArray = new Class[]{batchProtocolClass};

    private static final int DEFAULT_MAX_PENDING_BUFFERS = 10;
    private static final long DEFAULT_CACHE_UPDATE_PERIOD = 30000;
    private static final long DEFAULT_CACHE_EXPIRATION = 60;

    private final ThreadPoolExecutor writerPool;
    private final HConnection connection;

    private final LoadingCache<Integer,Set<HRegionInfo>> regionCache;
    private final ScheduledExecutorService cacheUpdater;
    private final long cacheUpdatePeriod;
    private final Configuration configuration;

    /*
     * Manageable state information about handing out buffers
     */
    private volatile long maxHeapSize;
    private volatile int maxBufferEntries;
    private volatile int maxPendingBuffers;
    private volatile int numRetries;
    private volatile long cacheUpdatedTimestamp;
    private volatile long pause;
    private volatile boolean compressWrites;

    private final AtomicInteger pendingBufferFlushes = new AtomicInteger(0);
    private final AtomicInteger executingBufferFlushes = new AtomicInteger(0);
    private final AtomicInteger outstandingCallBuffers = new AtomicInteger(0);
    private final AtomicLong totalBufferFlushes = new AtomicLong(0);
    private final AtomicInteger runningWrites = new AtomicInteger(0);


    public static TableWriter create(Configuration configuration) throws IOException {
        assert configuration!=null;

        HConnection connection= HConnectionManager.getConnection(configuration);

        long writeBufferSize = configuration.getLong("hbase.client.write.buffer", 2097152);
        int maxBufferEntries = configuration.getInt("hbase.client.write.buffer.maxentries", -1);
        int maxPendingBuffers = configuration.getInt("hbase.client.write.buffers.maxflushes",
                DEFAULT_MAX_PENDING_BUFFERS);

        int maxThreads = configuration.getInt("hbase.htable.threads.max",Integer.MAX_VALUE);
        if(maxThreads==0)maxThreads = 1;

        long threadKeepAlive = configuration.getLong("hbase.htable.threads.keepalivetime",60);

        int numRetries = configuration.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
                HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);

        ThreadFactory writerFactory = new ThreadFactoryBuilder()
                .setNameFormat("tablewriter-writerpool-%d")
                .setDaemon(true)
                .setPriority(Thread.NORM_PRIORITY).build();
        ThreadPoolExecutor writerPool = new ThreadPoolExecutor(1,maxThreads,threadKeepAlive,
                TimeUnit.SECONDS,new SynchronousQueue<Runnable>(),writerFactory);
        long cacheUpdatePeriod = configuration.getLong("hbase.htable.regioncache.updateinterval",DEFAULT_CACHE_UPDATE_PERIOD);
        ThreadFactory cacheFactory = new ThreadFactoryBuilder()
                .setNameFormat("tablewriter-cacheupdater-%d")
                .setDaemon(true)
                .setPriority(Thread.NORM_PRIORITY).build();
        ScheduledExecutorService cacheUpdater = Executors.newSingleThreadScheduledExecutor(cacheFactory);

        long cacheExpirationPeriod = configuration.getLong("hbase.htable.regioncache.expiration",DEFAULT_CACHE_EXPIRATION);
        LoadingCache<Integer,Set<HRegionInfo>> regionCache = CacheBuilder.newBuilder()
                .expireAfterWrite(cacheExpirationPeriod,TimeUnit.SECONDS)
                .build(new RegionLoader(configuration));

        boolean compressWrites = configuration.getBoolean("hbase.client.compress.writes",false);
        long pause = configuration.getLong(HConstants.HBASE_CLIENT_PAUSE,
                HConstants.DEFAULT_HBASE_CLIENT_PAUSE);

        return new TableWriter(writerPool,cacheUpdater,connection,regionCache,
                writeBufferSize,maxBufferEntries,maxPendingBuffers,cacheUpdatePeriod,numRetries,compressWrites,pause,configuration);
    }

    private TableWriter( ThreadPoolExecutor writerPool,
                         ScheduledExecutorService cacheUpdater,
                        HConnection connection,
                        LoadingCache<Integer, Set<HRegionInfo>> regionCache,
                        long maxHeapSize,
                        int maxBufferEntries,
                        int maxPendingBuffers,
                        long cacheUpdatePeriod,
                        int numRetries,
                        boolean compressWrites,
                        long pause,
                        Configuration configuration) {
        this.writerPool = writerPool;
        this.cacheUpdater = cacheUpdater;
        this.connection = connection;
        this.regionCache = regionCache;
        this.configuration = configuration;
        this.cacheUpdatePeriod = cacheUpdatePeriod;
        this.maxHeapSize = maxHeapSize;
        this.maxBufferEntries = maxBufferEntries;
        this.maxPendingBuffers = maxPendingBuffers;
        this.numRetries = numRetries;
        this.compressWrites = compressWrites;
        this.pause = pause;
    }

    public void start(){
        cacheUpdater.scheduleAtFixedRate(new RegionCacheLoader(), 0l, cacheUpdatePeriod, TimeUnit.MILLISECONDS);
    }

    public void shutdown(){
        cacheUpdater.shutdownNow();
        writerPool.shutdown();
    }

    public CallBuffer<Mutation> writeBuffer(byte[] tableName) throws Exception{
        outstandingCallBuffers.incrementAndGet();
        final Writer writer = new Writer(tableName, maxPendingBuffers);
        return new UnsafeCallBuffer<Mutation>(maxHeapSize,maxBufferEntries,writer){
            @Override
            public void close() throws Exception {
                writer.ensureFlushed();
                super.close();
                outstandingCallBuffers.decrementAndGet();
            }
        };
    }

    public CallBuffer<Mutation> synchronousWriteBuffer(byte[] tableName) throws Exception{
        outstandingCallBuffers.incrementAndGet();
        final SynchronousWriter writer = new SynchronousWriter(tableName);
        return new UnsafeCallBuffer<Mutation>(maxHeapSize,maxBufferEntries,writer){
            @Override
            public void close() throws Exception {
                super.close();
                outstandingCallBuffers.decrementAndGet();
            }
        };
    }

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
    @Override public long getCacheLastUpdatedTimeStamp() { return cacheUpdatedTimestamp; }

    @Override
    public int getNumCachedRegions(String tableName) {
        Set<HRegionInfo> regions = regionCache.getIfPresent(Bytes.mapKey(tableName.getBytes()));
        if(regions==null) return 0;
        return regions.size();
    }


/********************************************************************************************************************/
    /*private helper methods*/
    private abstract class BufferListener implements CallBuffer.Listener<Mutation>{
        protected final byte[] tableName;

        protected BufferListener(byte[] tableName) {
            this.tableName = tableName;
        }

        @Override
        public long heapSize(Mutation element) {
            if(element instanceof Put) return ((Put)element).heapSize();
            else return element.getRow().length;
        }

        public abstract void threadFinished(AtomicInteger counter);
    }

    private class SynchronousWriter extends BufferListener{

        protected SynchronousWriter(byte[] tableName) {
            super(tableName);
        }

        @Override
        public void threadFinished(AtomicInteger counter) {
            //no-op, since we're waiting for them all anyway
        }

        @Override
        public void bufferFlushed(List<Mutation> entries) throws Exception {
            List<MutationRequest> mutationRequests = bucketMutations(tableName,entries);
            final List<Future<Void>> futures =new ArrayList<Future<Void>>(mutationRequests.size());
            for(MutationRequest mutationRequest:mutationRequests){
                futures.add(writerPool.submit(new BufferWrite(mutationRequest,tableName,this,new AtomicInteger(1))));
            }
            for(Future<Void> future:futures){
                future.get();
            }
        }
    }

    private class Writer extends BufferListener{
        private final List<Future<Void>> futures = Lists.newArrayList();
        private final Semaphore pendingBuffersPermits;

        private Writer(byte[] tableName, int maxPendingBuffers) {
            super(tableName);
            this.pendingBuffersPermits = new Semaphore(maxPendingBuffers);
        }

        @Override
        public void bufferFlushed(List<Mutation> entries) throws Exception {
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
               throw new RetriesExhaustedWithDetailsException(retryExceptions,getRows(entries), Collections.<String>emptyList());

            List<MutationRequest> bucketedMutations = bucketMutations(tableName,entries);
            final AtomicInteger runningCounts = new AtomicInteger(bucketedMutations.size());
            for(MutationRequest mutationToWrite: bucketedMutations){
                futures.add(writerPool.submit(new BufferWrite(mutationToWrite,tableName,this,runningCounts)));
            }
        }

        private List<Row> getRows(List<Mutation> entries) {
            return Lists.transform(entries,new Function<Mutation, Row>() {
                @Override
                public Row apply(@Nullable Mutation input) {
                    if(input instanceof Row) return (Row)input;
                    return null;
                }
            });
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
                    Throwable t = Throwables.getRootCause(ee);
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

    private Multimap<byte[],Mutation> getBucketedMutations(Integer tableKey,Collection<Mutation> mutations, int tries) throws Exception{
        if(tries <=0){
            throw new NotServingRegionException("Unable to find a region for mutations "
                    +mutations.size()+" mutations, unable to write buffer");
        }
        Set<HRegionInfo> regionInfos = regionCache.get(tableKey);
        Multimap<byte[],Mutation> bucketsMutations = ArrayListMultimap.create();
        List<Mutation> regionLessMutations = Lists.newArrayListWithExpectedSize(0);
        for(Mutation mutation:mutations){
            byte[] row = mutation.getRow();
            boolean found = false;
            for(HRegionInfo region:regionInfos){
                if(HRegion.rowIsInRange(region,row)){
                    bucketsMutations.put(region.getStartKey(),mutation);
                    found =true;
                    break;
                }
            }
            if(!found)
                regionLessMutations.add(mutation);
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
            Multimap<byte[],Mutation> tryAgainMutationBuckets = getBucketedMutations(tableKey,mutations,tries-1);
            bucketsMutations.putAll(tryAgainMutationBuckets);
        }
        return bucketsMutations;
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
        Multimap<byte[],Mutation> bucketsMutations = getBucketedMutations(Bytes.mapKey(tableName),mutations,numRetries);
        List<MutationRequest> mutationRequests = Lists.newArrayListWithCapacity(bucketsMutations.size());
        for(byte[] regionStart:bucketsMutations.keySet()){
            MutationRequest request = compressWrites? new SnappyMutationRequest(regionStart): new UncompressedMutationRequest(regionStart);
            request.addAll(bucketsMutations.get(regionStart));
            mutationRequests.add(request);
        }

        return mutationRequests;
    }

    private class RegionCacheLoader  implements Runnable{

        @Override
        public void run() {
            SpliceLogUtils.debug(CACHE_LOG,"Refreshing Region cache for all tables");
            TableWriter.this.cacheUpdatedTimestamp = System.currentTimeMillis();
            final Map<byte[],Set<HRegionInfo>> regionInfos = Maps.newHashMap();
            MetaScanner.MetaScannerVisitor visitor = new MetaScanner.MetaScannerVisitor() {
                @Override
                public boolean processRow(Result rowResult) throws IOException {
                    byte[] bytes = rowResult.getValue(HConstants.CATALOG_FAMILY,HConstants.REGIONINFO_QUALIFIER);
                    if(bytes==null){
                        //TODO -sf- log a message here
                        return true;
                    }
                    HRegionInfo info = Writables.getHRegionInfo(bytes);
                    Set<HRegionInfo> regions = regionInfos.get(info.getTableName());
                    if(regions==null){
                        regions = new CopyOnWriteArraySet<HRegionInfo>();
                        regionInfos.put(info.getTableName(),regions);
                    }
                    if(!(info.isOffline()||info.isSplit()))
                        regions.add(info);
                    return true;
                }
            };

            try {
                MetaScanner.metaScan(configuration,visitor);
            } catch (IOException e) {
                SpliceLogUtils.error(CACHE_LOG,"Unable to update region cache",e);
            }
            for(byte[] table:regionInfos.keySet()){
                SpliceLogUtils.trace(CACHE_LOG,"Updating cache for "+ Bytes.toString(table));
                regionCache.put(Bytes.mapKey(table),regionInfos.get(table));
            }
        }
    }

    private static class RegionLoader extends CacheLoader<Integer, Set<HRegionInfo>> {
        private final Configuration configuration;

        public RegionLoader(Configuration configuration) {
            this.configuration = configuration;
        }

        @Override
        public Set<HRegionInfo> load(final Integer key) throws Exception {
            SpliceLogUtils.trace(CACHE_LOG,"Loading regions for key %d",key);
            final Set<HRegionInfo> regionInfos = new CopyOnWriteArraySet<HRegionInfo>();
            final MetaScanner.MetaScannerVisitor visitor = new MetaScanner.MetaScannerVisitor() {
                @Override
                public boolean processRow(Result rowResult) throws IOException {
                    byte[] bytes = rowResult.getValue(HConstants.CATALOG_FAMILY,HConstants.REGIONINFO_QUALIFIER);
                    if(bytes==null){
                        //TODO -sf- log a message here
                        return true;
                    }
                    HRegionInfo info = Writables.getHRegionInfo(bytes);
                    Integer tableKey = Bytes.mapKey(info.getTableName());
                    if(key.equals(tableKey)&& !(info.isOffline()||info.isSplit())){
                            regionInfos.add(info);
                    }
                    return true;
                }
            };

            try {
                MetaScanner.metaScan(configuration,visitor);
            } catch (IOException e) {
                SpliceLogUtils.error(LOG,"Unable to update region cache",e);
            }
            SpliceLogUtils.trace(CACHE_LOG,"loaded regions %s",regionInfos);
            return regionInfos;
        }
    }

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

        private BufferWrite(MutationRequest mutationsToWrite,
                            byte[] tableName,
                            BufferListener writer,
                            AtomicInteger runningCounts) {
            this.mutationsToWrite = Lists.newArrayList(mutationsToWrite);
            this.tableName = tableName;
            this.writer = writer;
            this.runningCounts = runningCounts;
        }

        public void tryWrite(int tries) throws Exception {
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
                throw new RetriesExhaustedWithDetailsException(retryExceptions,getBadRows(),Collections.<String>emptyList());
            }
            List<MutationRequest> failedMutations = Lists.newArrayListWithExpectedSize(0);

            Iterator<MutationRequest> mutations = mutationsToWrite.iterator();
            while(mutations.hasNext()){
                MutationRequest mutationRequest = mutations.next();
                try{
                    NoRetryExecRPCInvoker invoker = new NoRetryExecRPCInvoker(configuration,connection,
                            batchProtocolClass,tableName,mutationRequest.getRegionStartKey(),tries<numRetries);
                    BatchProtocol instance = (BatchProtocol) Proxy.newProxyInstance(configuration.getClassLoader(),
                            protoClassArray,invoker );
                    instance.batchMutate(mutationRequest);
                    mutations.remove();
                }catch(IOException ioe){
                    Throwable t = Throwables.getRootCause(ioe);
                    if(t instanceof RemoteException)
                        t= ((RemoteException)t).unwrapRemoteException();
                    if(Exceptions.shouldLogStackTrace(t))
                        LOG.info("Error received when trying to write buffer:"+ ioe.getMessage());
                    else
                        LOG.info("Received a retryable exception when trying to write buffer: "+ ioe.getClass().getSimpleName());
                    if(t instanceof DoNotRetryIOException) throw (IOException)t;
                    else{
                        retryExceptions.add(t);
                        failedMutations.add(mutationRequest);
                    }
                }
            }
            if(failedMutations.size()>0){
                //we had some mutations that failed due to a retry-able exception.
                //invalidate the cache, reset them, and try them again
                //wait for the backoff period before trying
                Thread.sleep(getWaitTime(numRetries-tries+1));
                regionCache.invalidate(Bytes.mapKey(tableName));

                resetMutations();
                tryWrite(tries - 1);
            }

        }

        /*
         * For error handling, convert a batch of failed mutations into a list of rows for error reporting.
         */
        private List<Row> getBadRows(){
            List<Row> badRows = Lists.newArrayList();
            for(MutationRequest mutationRequest:mutationsToWrite){
                badRows.addAll(Lists.transform(mutationRequest.getMutations(),new Function<Mutation, Row>() {
                    @Override
                    public Row apply(@Nullable Mutation input) {
                        return (Row)input;
                    }
                }));
            }
            return badRows;
        }

        /*
         * Re-bucket the mutations list into new MutationRequests based on a new cache topology for the
         * table.
         */
        private void resetMutations() throws Exception {
            List<MutationRequest> newMutations = Lists.newArrayList();
            for(MutationRequest remainingMutation:mutationsToWrite){
                List<MutationRequest> mutationRequests = bucketMutations(tableName,remainingMutation.getMutations());
                for(MutationRequest request:mutationRequests){
                    boolean found = false;
                    for(MutationRequest newMutation:newMutations){
                        if(Bytes.equals(newMutation.getRegionStartKey(), request.getRegionStartKey())){
                            newMutation.addAll(request.getMutations());
                            found=true;
                            break;
                        }
                    }
                    if(!found)
                        newMutations.add(request);
                }
            }
            this.mutationsToWrite = newMutations;
        }

        @Override
        public Void call() throws Exception {
            runningWrites.incrementAndGet();
            try{
                tryWrite(numRetries);
            }finally{
                writer.threadFinished(runningCounts);
                runningWrites.decrementAndGet();
            }
            return null;
        }
    }
}
