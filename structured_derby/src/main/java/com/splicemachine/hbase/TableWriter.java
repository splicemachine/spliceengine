package com.splicemachine.hbase;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.ExecRPCInvoker;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 * Created on: 3/18/13
 */
public class TableWriter {
    private static final Logger LOG = Logger.getLogger(TableWriter.class);
    private static final Logger CACHE_LOG = Logger.getLogger(RegionCacheLoader.class);

    private static final Class<BatchProtocol> batchProtocolClass = BatchProtocol.class;
    private static final Class<? extends CoprocessorProtocol>[] protoClassArray = new Class[]{batchProtocolClass};

    private static final int DEFAULT_MAX_PENDING_BUFFERS = 10;
    private static final long DEFAULT_CACHE_UPDATE_PERIOD = 30000;
    private static final long DEFAULT_CACHE_EXPIRATION = 60;

    private final ExecutorService writerPool;
    private final HConnection connection;

    private final LoadingCache<Integer,Set<HRegionInfo>> regionCache;
    private final ScheduledExecutorService cacheUpdater;
    private final long cacheUpdatePeriod;

    private volatile long maxHeapSize;
    private volatile int maxBufferEntries;
    private volatile int maxPendingBuffers;
    private final Configuration configuration;


    public static TableWriter create(Configuration configuration) throws IOException {
        assert configuration!=null;

        HConnection connection= HConnectionManager.getConnection(configuration);

        long writeBufferSize = configuration.getLong("hbase.client.write.buffer", 2097152);
        int maxBufferEntries = configuration.getInt("hbase.client.write.buffer.maxentries", -1);
        int maxPendingBuffers = configuration.getInt("hbase.client.write.buffers.maxpending",
                DEFAULT_MAX_PENDING_BUFFERS);

        int maxThreads = configuration.getInt("hbase.htable.threads.max",Integer.MAX_VALUE);
        if(maxThreads==0)maxThreads = 1;

        long threadKeepAlive = configuration.getLong("hbase.htable.threads.keepalivetime",60);

        ThreadFactory writerFactory = new ThreadFactoryBuilder()
                .setNameFormat("tablewriter-writerpool-%d")
                .setDaemon(true)
                .setPriority(Thread.NORM_PRIORITY).build();
        ExecutorService writerPool = new ThreadPoolExecutor(1,maxThreads,threadKeepAlive,
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

        return new TableWriter(writerPool,cacheUpdater,connection,regionCache,
                writeBufferSize,maxBufferEntries,maxPendingBuffers,cacheUpdatePeriod,configuration);
    }

    private TableWriter( ExecutorService writerPool,
                         ScheduledExecutorService cacheUpdater,
                        HConnection connection,
                        LoadingCache<Integer, Set<HRegionInfo>> regionCache,
                        long maxHeapSize,
                        int maxBufferEntries,
                        int maxPendingBuffers,
                        long cacheUpdatePeriod,
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
    }

    public void start(){
        cacheUpdater.scheduleAtFixedRate(new RegionCacheLoader(), 0l, cacheUpdatePeriod, TimeUnit.MILLISECONDS);
    }

    public void shutdown(){
        cacheUpdater.shutdownNow();
        writerPool.shutdown();
    }

    public CallBuffer<Mutation> writeBuffer(byte[] tableName) throws Exception{
        final Writer writer = new Writer(tableName, maxPendingBuffers);
        return new UnsafeCallBuffer<Mutation>(maxHeapSize,maxBufferEntries,writer){
            @Override
            public void close() throws Exception {
                writer.ensureFlushed();
                super.close();
            }
        };
    }

    public CallBuffer<Mutation> synchronousWriteBuffer(byte[] tableName) throws Exception{
        final SynchronousWriter writer = new SynchronousWriter(tableName);
        return new UnsafeCallBuffer<Mutation>(maxHeapSize,maxBufferEntries,writer);
    }


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
    }

    private class SynchronousWriter extends BufferListener{

        protected SynchronousWriter(byte[] tableName) {
            super(tableName);
        }

        @Override
        public void bufferFlushed(List<Mutation> entries) throws Exception {
            Multimap<byte[], Mutation> bucketedMutations = bucketMutations(tableName,entries);
            final List<Future<Void>> futures = Lists.newArrayListWithCapacity(bucketedMutations.size());
            for(byte[] regionStartKey:bucketedMutations.keySet()){
                final Collection<Mutation> mutationsToWrite = bucketedMutations.get(regionStartKey);
                final ExecRPCInvoker invoker = new ExecRPCInvoker(
                        configuration,connection,
                        batchProtocolClass,
                        tableName,regionStartKey);
                futures.add(writerPool.submit(new Callable<Void>(){
                    @Override
                    public Void call() throws Exception {
                        BatchProtocol instance = (BatchProtocol) Proxy.newProxyInstance(configuration.getClassLoader(),
                                protoClassArray,
                                invoker);
                        instance.batchMutate(mutationsToWrite);

                        return null;
                    }
                }));
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
            pendingBuffersPermits.acquire();

            Multimap<byte[], Mutation> bucketedMutations = bucketMutations(tableName,entries);
            final AtomicInteger runningCounts = new AtomicInteger(bucketedMutations.size());
            for(byte[] regionStartKey:bucketedMutations.keySet()){
                final Collection<Mutation> mutationsToWrite = bucketedMutations.get(regionStartKey);
                final ExecRPCInvoker invoker = new ExecRPCInvoker(
                        configuration,connection,
                        batchProtocolClass,
                        tableName,regionStartKey);
                futures.add(writerPool.submit(new Callable<Void>(){
                    @Override
                    public Void call() throws Exception {
                        BatchProtocol instance = (BatchProtocol) Proxy.newProxyInstance(configuration.getClassLoader(),
                                protoClassArray,
                                invoker);
                        instance.batchMutate(mutationsToWrite);

                        int threadsStillRunning = runningCounts.decrementAndGet();
                        if(threadsStillRunning<=0){
                            pendingBuffersPermits.release();
                        }
                        return null;
                    }
                }));
            }
        }

        public void ensureFlushed() throws Exception{
            /*
             * Cycle through the submitted futures and wait for them to return,
             * of explode if one of them did.
             */
            for(Future<Void> future:futures){
                future.get();
            }
        }
    }

    private Multimap<byte[], Mutation> bucketMutations(
            byte[] tableName, List<Mutation> mutations) throws Exception {
        Integer tableKey = Bytes.mapKey(tableName);
        Set<HRegionInfo> regionInfos = regionCache.get(tableKey);
        Multimap<byte[],Mutation> bucketedMutations = ArrayListMultimap.create();
        List<Mutation> regionLessMutations = Lists.newArrayListWithExpectedSize(0);
        for(Mutation mutation:mutations){
            byte[] row = mutation.getRow();
            boolean found=false;
            for(HRegionInfo region:regionInfos){
                byte[] regionStart = region.getStartKey();
                byte[] regionFinish = region.getEndKey();
                if(Bytes.equals(regionStart, HConstants.EMPTY_START_ROW) ||
                        Bytes.compareTo(regionStart,row)<=0){
                    if(Bytes.equals(regionFinish,HConstants.EMPTY_END_ROW)||
                            Bytes.compareTo(row,regionFinish)<0){
                        bucketedMutations.put(regionStart,mutation);
                        found=true;
                        break;
                    }
                }
            }
            if(!found)
                regionLessMutations.add(mutation);
        }

        //TODO -sf- should we deal with offline regions differently?
        for(HRegionInfo region:regionInfos){
            bucketedMutations.putAll(region.getStartKey(),regionLessMutations);
        }
        return bucketedMutations;
    }

    private class RegionCacheLoader  implements Runnable{

        @Override
        public void run() {
            SpliceLogUtils.debug(CACHE_LOG,"Refreshing Region cache for all tables");
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
                    if(key.equals(tableKey))
                        regionInfos.add(info);
                    return true;
                }
            };

            try {
                MetaScanner.metaScan(configuration,visitor);
            } catch (IOException e) {
                SpliceLogUtils.error(LOG,"Unable to update region cache",e);
            }
            return regionInfos;
        }
    }
}
