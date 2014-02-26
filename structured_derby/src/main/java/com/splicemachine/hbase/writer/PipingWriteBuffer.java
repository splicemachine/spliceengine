package com.splicemachine.hbase.writer;

import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.collect.Lists;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.RegionCache;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.stats.Metrics;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

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
    private static final Logger LOG = Logger.getLogger(PipingWriteBuffer.class);
    private NavigableMap<byte[],PreMappedBuffer> regionToBufferMap;
    private final Writer writer;
    private final Writer synchronousWriter;
    private final byte[] tableName;
    private final String txnId;
    private final RegionCache regionCache;

    private long totalElementsAdded = 0l;
    private long totalBytesAdded = 0l;
    private long totalFlushes = 0l;

		private final MergingWriteStats writeStats;

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

    public PipingWriteBuffer(byte[] tableName,
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

				MetricFactory metricFactory = writeConfiguration!=null? writeConfiguration.getMetricFactory(): Metrics.noOpMetricFactory();

				writeStats = new MergingWriteStats(metricFactory);

    }

    @Override
    public void add(KVPair element) throws Exception {
				totalElementsAdded++;
				totalBytesAdded +=(element.getSize());
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
            if(regions.size()<=0)
                throw new IOException("Unable to get region information for table "+ Bytes.toString(tableName));
        }

        for(HRegionInfo region:regions){
            //see if regionToBufferMap contains it. If not, add it in
            byte[] startKey = region.getStartKey();
            if(regionToBufferMap.containsKey(startKey)) continue;

            //we need to add it in
            RegulatedWriter writeWrapper = new RegulatedWriter(writer,
                    new CountingHandler(new RegulatedWriter.OtherWriterHandler(synchronousWriter)),
                    bufferConfiguration.getMaxFlushesPerRegion());
            PreMappedBuffer newBuffer = new PreMappedBuffer(writeWrapper, startKey, preFlushHook, bufferConfiguration.getMaxEntries());
            regionToBufferMap.put(startKey,newBuffer);
            Map.Entry<byte[],PreMappedBuffer> parentRegion = regionToBufferMap.lowerEntry(startKey);
            if(parentRegion!=null){
                PreMappedBuffer oldBuffer = parentRegion.getValue();
                //use the same size as the oldBuffer as our initial starting point
                int oldMaxFlushes = ((RegulatedWriter)oldBuffer.getWriter()).getCurrentMaxFlushes();
                writeWrapper.setCurrentMaxFlushes(oldMaxFlushes);
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
    public void addAll(ObjectArrayList<KVPair> elements) throws Exception {
    	Object[] elementArray = elements.buffer;
    	int size = elements.size();
    	for (int i = 0; i< size; i++) {
            add((KVPair)elementArray[i]);        		
    	}
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
    @Override public long getTotalBytesAdded() { return totalBytesAdded; }
    @Override public long getTotalFlushes() { return totalFlushes; }
    @Override public double getAverageEntriesPerFlush() { return ((double)totalElementsAdded)/totalFlushes; }
    @Override public double getAverageSizePerFlush() { return ((double) totalBytesAdded)/totalFlushes; }
    @Override public CallBuffer<KVPair> unwrap() { return this; }
		@Override public WriteStats getWriteStats() { return writeStats; }

		// For testing purpose
    void setBuildBuffer() {rebuildBuffer = true;}
    TreeMap<Integer, Integer> getRegionToBufferCount() {
        TreeMap<Integer,Integer> map = new TreeMap<Integer, Integer>();
        for(Map.Entry<byte[],PreMappedBuffer> entry: regionToBufferMap.entrySet()) {
            Integer key = Bytes.mapKey(entry.getKey());
            PreMappedBuffer buffer = entry.getValue();
            int size = buffer.getBufferSize();
            map.put(key, size);
        }
        return map;
    }

    private class PreMappedBuffer implements CallBuffer<KVPair> {
        private final Writer writer;
        private ObjectArrayList<KVPair> buffer;
        private int heapSize;
        private final byte[] regionStartKey;
        private final List<Future<WriteStats>> outstandingRequests = Lists.newArrayList();
        private final WriteCoordinator.PreFlushHook preFlushHook;

        private final int maxEntries;

        public PreMappedBuffer(Writer writer, byte[] regionStartKey, WriteCoordinator.PreFlushHook preFlushHook, int maxEntries) {
            this.writer = writer;
            this.regionStartKey = regionStartKey;
            this.preFlushHook = preFlushHook;
            this.maxEntries = maxEntries;
            if(maxEntries<0)
                this.buffer = ObjectArrayList.newInstance();
            else
                this.buffer = ObjectArrayList.newInstanceWithCapacity(maxEntries);
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
        public void addAll(ObjectArrayList<KVPair> elements) throws Exception {
        	Object[] elementArray = elements.buffer;
        	int size = elements.size();
        	for (int i = 0; i< size; i++) {
                add((KVPair)elementArray[i]);        		
        	}
        }

        @Override
				public void flushBuffer() throws Exception {
						if(buffer.size()<=0) return; //don't do anything if we don't need to flush

						//check previously finished flushes for errors, and explode if any of them have failed
						Iterator<Future<WriteStats>> futureIterator = outstandingRequests.iterator();
						while(futureIterator.hasNext()){
								Future<WriteStats> future = futureIterator.next();
								if(future.isDone()){
										WriteStats retStats = future.get();//check for errors

										//if it gets this far, it succeeded--strip the reference
										futureIterator.remove();
										writeStats.merge(retStats);
								}
						}
						ObjectArrayList<KVPair> newBuffer = ObjectArrayList.newInstance();
						ObjectArrayList<KVPair> copy = buffer;
						buffer = newBuffer;

						//update heap size metrics
						if(LOG.isTraceEnabled())
								LOG.trace("flushing "+ copy.size()+" entries");
						PipingWriteBuffer.this.currentHeapSize-=heapSize;
						heapSize=0;

						copy = preFlushHook.transform(copy);
						BulkWrite write = new BulkWrite(copy,txnId,regionStartKey);
						outstandingRequests.add(writer.write(tableName,write, writeConfiguration));
				}

        @Override
        public void close() throws Exception {
            flushBuffer();
            //make sure all outstanding buffers complete before returning
            for(Future<WriteStats> outstandingCall:outstandingRequests){
								WriteStats retStats = outstandingCall.get();//wait for errors and/or completion
								writeStats.merge(retStats);
						}
        }

        public ObjectArrayList<KVPair> removeAllAfter(final byte[] startKey) {
            ObjectArrayList<KVPair> removed = ObjectArrayList.newInstance();
            int size = buffer.size();

            Object[] array = buffer.buffer;
            boolean needsCompact = false;
            for (int i = 0; i< size;i++) {
                KVPair pair = (KVPair) array[i];
                if(Bytes.compareTo(startKey,pair.getRow())<=0){
                    removed.add(pair);
                    array[i] = null;
                    needsCompact = true;
                }
            }
            if (needsCompact) {
                compactBuffer(size);
            }
            return removed;
        }

        private void compactBuffer(int size) {
            ObjectArrayList<KVPair> newBuffer = ObjectArrayList.newInstance();
						Object[] bValues = buffer.buffer;
            for (int i = 0; i < size; ++i) {
                if (bValues[i] != null) {
                    newBuffer.add((KVPair) bValues[i]);
                }
            }
						buffer = newBuffer;
        }

        public int getHeapSize() {
            return heapSize;
        }

        public Writer getWriter() {
            return writer;
        }

        public int getBufferSize() { return buffer.size(); }
    }

		private class UpdatingWriteConfiguration extends ForwardingWriteConfiguration{
				protected UpdatingWriteConfiguration(Writer.WriteConfiguration delegate) { super(delegate); }

				@Override
				public Writer.WriteResponse globalError(Throwable t) throws ExecutionException {
						if(t instanceof NotServingRegionException || t instanceof WrongRegionException){
								PipingWriteBuffer.this.rebuildBuffer = true;
						}
						return super.globalError(t);
				}

				@Override
				public Writer.WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
						for(IntObjectCursor<WriteResult> cursor:result.getFailedRows()){
								switch (cursor.value.getCode()) {
										case NOT_SERVING_REGION:
										case WRONG_REGION:
												PipingWriteBuffer.this.rebuildBuffer=true;
												break;
								}
						}
						return super.partialFailure(result,request);
				}
		}

    private class CountingHandler implements RegulatedWriter.WriteRejectedHandler{
        private final RegulatedWriter.WriteRejectedHandler otherWriterHandler;

        public CountingHandler(RegulatedWriter.WriteRejectedHandler otherWriterHandler)  {
            this.otherWriterHandler = otherWriterHandler;
        }

        @Override
        public Future<WriteStats> writeRejected(byte[] tableName, BulkWrite action, Writer.WriteConfiguration writeConfiguration) throws ExecutionException {
            bufferConfiguration.writeRejected();
            return otherWriterHandler.writeRejected(tableName,action,writeConfiguration);
        }
    }
}
