package com.splicemachine.hbase.writer;

import com.google.common.collect.Lists;
import com.splicemachine.hbase.RegionCache;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A CallBuffer which pre-maps entries to a separate buffer based on which region
 * the write belongs to.
 *
 * This implementation obeys any per-region bounds set in the passed in
 * {@link BufferConfiguration} entity.
 *
 * This class is <em>not</em> Thread-safe. It's use should be restricted to a
 * single thread. If that is not possible, then external synchronization is
 * necessary.
 *
 * @author Scott Fines
 * Created on: 8/27/13
 */
public class PipingWriteBuffer implements RecordingCallBuffer<KVPair>{
    private NavigableMap<byte[],PreMappedBuffer> regionToBufferMap;
    private final Writer writer;
    private final Writer synchronousWriter;
    private final byte[] tableName;
    private final String txnId;
    private final RegionCache regionCache;

    private long totalElementsAdded = 0l;
    private long totalBytesAddes = 0l;
    private long totalFlushes = 0l;

    /*
     * In the event of a Region split, we need a flag to indicate to us that the
     * regionBufferMap needs to be rebuilt. Because this flag may be set from other
     * threads than the owner, it needs to be volatile even though this class as
     * a whole is not thread-safe.
     */
    private volatile boolean rebuildBuffer = true; //use to initialize the map
    private final Writer.WriteConfiguration writeConfiguration;

    private long currentHeapSize;

    private final BufferConfiguration bufferConfiguration;
    private final WriteCoordinator.PreFlushHook preFlushHook;

    PipingWriteBuffer(byte[] tableName,
                              String txnId,
                              Writer writer,
                              Writer synchronousWriter,
                              RegionCache regionCache,
                              Writer.WriteConfiguration writeConfiguration,
                              BufferConfiguration bufferConfiguration) {
        this(tableName, txnId, writer, synchronousWriter, regionCache, WriteCoordinator.noOpFlushHook,writeConfiguration, bufferConfiguration);
    }

    PipingWriteBuffer(byte[] tableName,
                      String txnId,
                      Writer writer,
                      Writer synchronousWriter,
                      RegionCache regionCache,
                      WriteCoordinator.PreFlushHook preFlushHook,
                      Writer.WriteConfiguration writeConfiguration,
                      BufferConfiguration bufferConfiguration) {
        this.writer = writer;
        this.synchronousWriter = synchronousWriter;
        this.tableName = tableName;
        this.txnId = txnId;
        this.regionCache = regionCache;
        this.writeConfiguration = new UpdatingWriteConfiguration(writeConfiguration);
        this.regionToBufferMap = new TreeMap<byte[], PreMappedBuffer>(Bytes.BYTES_COMPARATOR);
        this.bufferConfiguration = bufferConfiguration;
        this.preFlushHook = preFlushHook;
    }

    @Override
    public void add(KVPair element) throws Exception {
        rebuildIfNecessary();
        Map.Entry<byte[],PreMappedBuffer> entry = regionToBufferMap.floorEntry(element.getRow());
        if(entry==null) entry = regionToBufferMap.firstEntry();

        assert entry!=null;
        PreMappedBuffer buffer = entry.getValue();
        //the buffer will handle local flush constraints (e.g. entries are full, etc)
        buffer.add(element);

        //determine if global constraints require a flush
        currentHeapSize+=element.getSize();
        if(currentHeapSize>=bufferConfiguration.getMaxHeapSize()){
            flushLargestBuffer();
        }
    }

    private void flushLargestBuffer() throws Exception {
        int maxSize = 0;
        PreMappedBuffer bufferToFlush = null;
        for (PreMappedBuffer buffer : regionToBufferMap.values()) {
            if (buffer.getHeapSize() > maxSize) {
                bufferToFlush = buffer;
                maxSize = buffer.getHeapSize();
            }
        }

        //shouldn't be null unless the regionMap is empty, which shouldn't happen
        //if we call rebuildIfNecessary() properly
        assert bufferToFlush!=null;

        //flush the buffer --it will adjust down the heap size as needed.
        bufferToFlush.flushBuffer();
    }

    private void rebuildIfNecessary() throws Exception {
        if(!rebuildBuffer&&regionToBufferMap.size()>0) return; //no need to rebuild the buffer
        /*
         * We need to rebuild the buffer. It's possible that there are
         * multiple buffer flushes in flight, some of whom may fail
         * and require a rebuilding as well, while we are in this method
         * call.
         *
         * However, recall that this is only expected to be used from one
         * thread, which means that we can safely operate here, knowing
         * that we block all new additions (and thus, all new buffer flushes),
         * until after the region map has been rebuilt.
         */
        SortedSet<HRegionInfo> regions = regionCache.getRegions(tableName);
        if(regions.size()<=0){
            int numTries=5;
            while(numTries>0){
                Thread.sleep(WriteUtils.getWaitTime(numTries,200));
                regionCache.invalidate(tableName);
                regions = regionCache.getRegions(tableName);
                if(regions.size()>0) break;
                numTries--;
            }
            if(regions.size()<0)
                throw new IOException("Unable to get region information for table "+ Bytes.toString(tableName));
        }

        for(HRegionInfo region:regions){
            //see if regionToBufferMap contains it. If not, add it in
            byte[] startKey = region.getStartKey();
            if(regionToBufferMap.containsKey(startKey)) continue;

            //we need to add it in
            Writer writeWrapper = new RegulatedWriter(writer,
                    new CountingHandler(new RegulatedWriter.OtherWriterHandler(synchronousWriter)),
                    bufferConfiguration.getMaxFlushesPerRegion());
            PreMappedBuffer newBuffer = new PreMappedBuffer(writeWrapper, startKey, preFlushHook, bufferConfiguration.getMaxEntries());
            regionToBufferMap.put(startKey,newBuffer);
            Map.Entry<byte[],PreMappedBuffer> parentRegion = regionToBufferMap.lowerEntry(startKey);
            if(parentRegion!=null){
                PreMappedBuffer oldBuffer = parentRegion.getValue();
                //move entries that are slated for the old region into the new region
                newBuffer.addAll(oldBuffer.removeAllAfter(startKey));
            }
        }
        rebuildBuffer=false;
    }

    @Override
    public void addAll(KVPair[] elements) throws Exception {
        for(KVPair element:elements)
            add(element);
    }

    @Override
    public void addAll(Collection<? extends KVPair> elements) throws Exception {
        for(KVPair element:elements)
            add(element);
    }

    @Override
    public void flushBuffer() throws Exception {
        //flush all buffers
        rebuildIfNecessary();
        for(PreMappedBuffer buffer:regionToBufferMap.values())
            buffer.flushBuffer();
    }

    @Override
    public void close() throws Exception {
        //close all buffers
        rebuildIfNecessary();
        for(PreMappedBuffer buffer:regionToBufferMap.values())
            buffer.close();
    }

    @Override public long getTotalElementsAdded() { return totalElementsAdded; }
    @Override public long getTotalBytesAdded() { return totalBytesAddes; }
    @Override public long getTotalFlushes() { return totalFlushes; }
    @Override public double getAverageEntriesPerFlush() { return ((double)totalElementsAdded)/totalFlushes; }
    @Override public double getAverageSizePerFlush() { return ((double)totalBytesAddes)/totalFlushes; }
    @Override public CallBuffer<KVPair> unwrap() { return this; }

    private class PreMappedBuffer implements CallBuffer<KVPair> {
        private final Writer writer;
        private final List<KVPair> buffer;
        private int heapSize;
        private final byte[] regionStartKey;
        private final List<Future<Void>> outstandingRequests = Lists.newArrayList();
        private final WriteCoordinator.PreFlushHook preFlushHook;

        private final int maxEntries;

        public PreMappedBuffer(Writer writer, byte[] regionStartKey, WriteCoordinator.PreFlushHook preFlushHook, int maxEntries) {
            this.writer = writer;
            this.regionStartKey = regionStartKey;
            this.preFlushHook = preFlushHook;
            this.maxEntries = maxEntries;
            this.buffer = Lists.newArrayListWithCapacity(maxEntries);
        }

        @Override
        public void add(KVPair element) throws Exception {
            buffer.add(element);
            heapSize+=element.getSize();

            if(buffer.size()>maxEntries)
                flushBuffer();
        }

        @Override
        public void addAll(KVPair[] elements) throws Exception {
            for(KVPair element:elements)
                add(element);
        }

        @Override
        public void addAll(Collection<? extends KVPair> elements) throws Exception {
            for(KVPair element:elements)
                add(element);
        }

        @Override
        public void flushBuffer() throws Exception {
            //check previously finished flushes for errors, and explode if any of them have failed
            Iterator<Future<Void>> futureIterator = outstandingRequests.iterator();
            while(futureIterator.hasNext()){
                Future<Void> future = futureIterator.next();
                if(future.isDone()){
                    future.get(); //check for errors
                    //if it gets this far, it succeeded--strip the reference
                    futureIterator.remove();
                }
            }
            if(buffer.size()>0){
                List<KVPair> copy = Lists.newArrayList(buffer);
                buffer.clear();
                //update heap size metrics
                PipingWriteBuffer.this.currentHeapSize-=heapSize;
                heapSize=0;

                copy = preFlushHook.transform(copy);
                BulkWrite write = new BulkWrite(copy,txnId,regionStartKey);
                outstandingRequests.add(writer.write(tableName,write, writeConfiguration));
            }
        }

        @Override
        public void close() throws Exception {
            flushBuffer();
            //make sure all outstanding buffers complete before returning
            for(Future<Void> outstandingCall:outstandingRequests){
                outstandingCall.get(); //wait for errors and/or completion
            }
        }

        public List<KVPair> removeAllAfter(final byte[] startKey) {
            List<KVPair> removed = Lists.newArrayList();
            Iterator<KVPair> iterator = buffer.iterator();
            while(iterator.hasNext()){
                KVPair pair = iterator.next();
                if(Bytes.compareTo(startKey,pair.getRow())<=0){
                    removed.add(pair);
                    iterator.remove();
                }
            }
            return removed;
        }

        public int getHeapSize() {
            return heapSize;
        }
    }

    private class UpdatingWriteConfiguration implements Writer.WriteConfiguration {
        private final Writer.WriteConfiguration delegate;

        private UpdatingWriteConfiguration(Writer.WriteConfiguration delegate) {
            this.delegate = delegate;
        }

        @Override public long getPause() { return delegate.getPause(); }
        @Override public int getMaximumRetries() { return delegate.getMaximumRetries(); }

        @Override
        public Writer.WriteResponse globalError(Throwable t) throws ExecutionException {
            if(t instanceof NotServingRegionException || t instanceof WrongRegionException){
               PipingWriteBuffer.this.rebuildBuffer = true;
            }
            return delegate.globalError(t);
        }

        @Override
        public Writer.WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
            for(WriteResult writeResult:result.getFailedRows().values()){
                switch (writeResult.getCode()) {
                    case NOT_SERVING_REGION:
                    case WRONG_REGION:
                        PipingWriteBuffer.this.rebuildBuffer=true;
                        break;
                }
            }
            return delegate.partialFailure(result,request);
        }

        @Override
        public void writeComplete() {
            delegate.writeComplete();
        }
    }

    private class CountingHandler implements RegulatedWriter.WriteRejectedHandler{
        private final RegulatedWriter.WriteRejectedHandler otherWriterHandler;

        public CountingHandler(RegulatedWriter.WriteRejectedHandler otherWriterHandler)  {
            this.otherWriterHandler = otherWriterHandler;
        }

        @Override
        public Future<Void> writeRejected(byte[] tableName, BulkWrite action, Writer.WriteConfiguration writeConfiguration) throws ExecutionException {
            bufferConfiguration.writeRejected();
            return otherWriterHandler.writeRejected(tableName,action,writeConfiguration);
        }
    }
}
