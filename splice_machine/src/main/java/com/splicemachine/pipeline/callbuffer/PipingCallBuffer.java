package com.splicemachine.pipeline.callbuffer;

import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.regioninfocache.RegionCache;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.CanRebuild;
import com.splicemachine.pipeline.api.PreFlushHook;
import com.splicemachine.pipeline.api.BufferConfiguration;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.api.Writer;
import com.splicemachine.pipeline.impl.BulkWrites;
import com.splicemachine.pipeline.impl.MergingWriteStats;
import com.splicemachine.pipeline.utils.PipelineUtils;
import com.splicemachine.pipeline.writeconfiguration.UpdatingWriteConfiguration;
import com.splicemachine.pipeline.writer.RegulatedWriter;
import com.splicemachine.pipeline.writerejectedhandler.CountingHandler;
import com.splicemachine.pipeline.writerejectedhandler.OtherWriteHandler;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import java.util.*;
import java.util.Map.Entry;

/**
 * A CallBuffer which pre-maps entries to a separate buffer based on which region
 * the write belongs to.  These pre-maps are incorporated into
 *
 * This implementation obeys any per-region bounds set in the passed in
 * {@link BufferConfiguration} entity.
 *
 * This class is <em>not</em> Thread-safe. It's use should be restricted to a
 * single thread. If that is not possible, then external synchronization is
 * necessary.
 *
 * @author Scott Fines
 * 
 * 
 * 
 * 
 * Created on: 8/27/13
 */
public class PipingCallBuffer implements RecordingCallBuffer<KVPair>, CanRebuild {
    private static final Logger LOG = Logger.getLogger(PipingCallBuffer.class);
    private NavigableMap<byte[],Pair<RegionCallBuffer,ServerName>> startKeyToBufferMap;
    private NavigableMap<ServerName,RegionServerCallBuffer> serverToRSBufferMap;
    private final Writer writer;
    private final byte[] tableName;
    private final TxnView txn;
    private final RegionCache regionCache;
    private long totalElementsAdded = 0l;
    private long totalBytesAdded = 0l;
    private long totalFlushes = 0l;
	private final MergingWriteStats writeStats;
    private volatile boolean rebuildBuffer = true; // rebuild from region cache
    private final WriteConfiguration writeConfiguration;
    private long currentHeapSize;
    private long currentKVPairSize;
    private final BufferConfiguration bufferConfiguration;
    private final PreFlushHook preFlushHook;
    private boolean record = true;
    
    public PipingCallBuffer(byte[] tableName,
                      TxnView txn,
                      Writer writer,
                      RegionCache regionCache,
                      PreFlushHook preFlushHook,
                      WriteConfiguration writeConfiguration,
                      BufferConfiguration bufferConfiguration) {
        this.writer = writer;
        this.tableName = tableName;
        this.txn = txn;
        this.regionCache = regionCache;
        this.writeConfiguration = new UpdatingWriteConfiguration(writeConfiguration,this); 
        this.startKeyToBufferMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        this.serverToRSBufferMap = new TreeMap<>();
        this.bufferConfiguration = bufferConfiguration;
        this.preFlushHook = preFlushHook;
		MetricFactory metricFactory = writeConfiguration!=null? writeConfiguration.getMetricFactory(): Metrics.noOpMetricFactory();
		writeStats = new MergingWriteStats(metricFactory);
    }

    @Override
    public void add(KVPair element) throws Exception {
        assert element!=null: "Cannot add a non-null element!";
    	SpliceLogUtils.trace(LOG, "add %s",element);
        rebuildIfNecessary();
        Map.Entry<byte[],Pair<RegionCallBuffer,ServerName>> entry = startKeyToBufferMap.floorEntry(element.getRow());
        if(entry==null) entry = startKeyToBufferMap.firstEntry();
        assert entry!=null;
        entry.getValue().getFirst().add(element);
		long size = element.getSize();
        currentHeapSize+=size;
        currentKVPairSize++;   
        if (record) {
	        totalElementsAdded++;
			totalBytesAdded +=size;
        }
        if(writer!=null && (currentHeapSize>=bufferConfiguration.getMaxHeapSize()
        		|| currentKVPairSize >= bufferConfiguration.getMaxEntries())) {
            flushLargestBuffer();
        }
    }

    private void flushLargestBuffer() throws Exception {
    	int maxSize = 0;
        RegionServerCallBuffer bufferToFlush = null;
        for (RegionServerCallBuffer buffer : serverToRSBufferMap.values()) {
            if (buffer.getHeapSize() > maxSize) {
                bufferToFlush = buffer;
                maxSize = buffer.getHeapSize();
            }
        }
        assert bufferToFlush!=null;
        currentHeapSize-=maxSize;
        currentKVPairSize-=bufferToFlush.getKVPairSize();
        if (LOG.isDebugEnabled())
        	SpliceLogUtils.debug(LOG, "flushLargestBuffer {table=%s, size=%d, rows=%d}",Bytes.toString(this.tableName),bufferToFlush.getHeapSize(),bufferToFlush.getKVPairSize());
        bufferToFlush.flushBuffer();
    }

    private void rebuildIfNecessary() throws Exception {
        if(!rebuildBuffer && startKeyToBufferMap != null && startKeyToBufferMap.size()>0) return; //no need to rebuild the buffer
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
        ObjectArrayList<KVPair> items = getKVPairs();
        assert items != null;
        if(startKeyToBufferMap!=null) {
            for (Pair<RegionCallBuffer, ServerName> buffer : startKeyToBufferMap.values())
                buffer.getFirst().flushBuffer();
            for (RegionServerCallBuffer buffer : serverToRSBufferMap.values()) {
                assert (buffer.getBulkWrites().getKVPairSize() == 0);
                buffer.close();
            }
        }
        this.startKeyToBufferMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        this.serverToRSBufferMap = new TreeMap<>();
        currentHeapSize=0;
        currentKVPairSize=0;

        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "rebuilding region map %s",Bytes.toString(tableName));
        SortedSet<Pair<HRegionInfo,ServerName>> regions = PipelineUtils.getRegions(regionCache, tableName);
        if (LOG.isDebugEnabled()) {
            for (Pair<HRegionInfo,ServerName> pair: regions) {
                SpliceLogUtils.debug(LOG, "region %s on server %s",pair.getFirst().getRegionNameAsString(), pair.getSecond().getServerName());
            }
        }
        for(Pair<HRegionInfo,ServerName> pair:regions){
            HRegionInfo region = pair.getFirst();
            ServerName serverName = pair.getSecond();
            byte[] startKey = region.getStartKey();
            RegionServerCallBuffer rsc = this.serverToRSBufferMap.get(pair.getSecond());
            // Do we have this RS already?
            if (rsc == null) {
                SpliceLogUtils.debug(LOG, "adding RSC %s", pair.getSecond());
                rsc = new RegionServerCallBuffer(tableName,writeConfiguration,pair.getSecond(),
                        writer != null? new RegulatedWriter(writer,new CountingHandler(new OtherWriteHandler(writer), bufferConfiguration),
                                bufferConfiguration.getMaxFlushesPerRegion()):null,
                        writeStats);
                serverToRSBufferMap.put(pair.getSecond(), rsc);
            }
            Entry<byte[], Pair<RegionCallBuffer, ServerName>> startKeyToBuffer = this.startKeyToBufferMap.floorEntry(startKey);
            // Total Miss
            RegionCallBuffer rcb = null;
            if (startKeyToBuffer != null)
                rcb = startKeyToBuffer.getValue().getFirst();
            if (startKeyToBuffer == null || rcb.keyOutsideBuffer(startKey)) {
                if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(LOG, "lower startKey %s", startKeyToBuffer);
                    if (rcb!=null) {
                        SpliceLogUtils.debug(LOG, "region %s", rcb.getHregionInfo().getRegionNameAsString());
                        SpliceLogUtils.debug(LOG, "region startKey %s", rcb.getHregionInfo().getStartKey());
                        SpliceLogUtils.debug(LOG, "region endKey %s", rcb.getHregionInfo().getEndKey());
                        SpliceLogUtils.debug(LOG, "comparison %d", Bytes.compareTo(startKey, rcb.getHregionInfo().getStartKey()));
                    }
                    SpliceLogUtils.debug(LOG, "startKey %s", startKey);
                    SpliceLogUtils.debug(LOG, "key outside buffer, add new region (suspect) %s", region.getRegionNameAsString());
                }
                RegionCallBuffer newBuffer = new RegionCallBuffer(rsc,region,txn,preFlushHook);
                startKeyToBufferMap.put(startKey,Pair.newPair(newBuffer,pair.getSecond()));
                rsc.add(Pair.newPair(startKey, newBuffer));
            } else {
                throw new RuntimeException("Not Functional Path");

            }

        }
        rebuildBuffer=false;
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "Adding Items Backs %s", items.size());
        record = false;
        assert items != null;
        this.addAll(items);
        items.release();
        items = null; // dereference
        record = true;
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
    	SpliceLogUtils.debug(LOG, "flushBuffer");
        if (serverToRSBufferMap == null) return;
    	//flush all buffers
        rebuildIfNecessary();
        for(RegionServerCallBuffer buffer:serverToRSBufferMap.values()) {
        	if (LOG.isDebugEnabled())
        		SpliceLogUtils.debug(LOG, "flushBuffer {table=%s, server=%s, rows=%d ",Bytes.toString(tableName),buffer.getServerName(),buffer.getKVPairSize());
            buffer.flushBuffer();
        }
        currentHeapSize=0;
        currentKVPairSize=0;
    }

    @Override
    public void close() throws Exception {
    	SpliceLogUtils.debug(LOG, "close");
    	//close all buffers
        if (serverToRSBufferMap == null) return;
        rebuildIfNecessary();
        for(RegionServerCallBuffer buffer:serverToRSBufferMap.values()) {
        	if (LOG.isDebugEnabled())
        		SpliceLogUtils.debug(LOG, "Closing {table=%s, server=%s}",Bytes.toString(tableName),buffer.getServerName());
            buffer.close();
        }
        
        for(Pair<RegionCallBuffer,ServerName> buffer:startKeyToBufferMap.values())
        	buffer.getFirst().close();        
        serverToRSBufferMap = null;
        startKeyToBufferMap = null;
        currentHeapSize = 0;
        currentKVPairSize = 0;
    }

    @Override public long getTotalElementsAdded() { return totalElementsAdded; }
    @Override public long getTotalBytesAdded() { return totalBytesAdded; }
    @Override public long getTotalFlushes() { return totalFlushes; }
    @Override public double getAverageEntriesPerFlush() { return ((double)totalElementsAdded)/totalFlushes; }
    @Override public double getAverageSizePerFlush() { return ((double) totalBytesAdded)/totalFlushes; }
    @Override public CallBuffer<KVPair> unwrap() { return this; }
	@Override public WriteStats getWriteStats() { return writeStats; }

	@Override
	public void incrementHeap(long heap) throws Exception {
	}

	@Override
	public void incrementCount(int count) throws Exception {
	}
	
	public List<BulkWrites> getBulkWrites() throws Exception {
		SpliceLogUtils.trace(LOG, "getBulkWrites");
        rebuildIfNecessary();
        List<BulkWrites> writes = new ArrayList<>();
        for(RegionServerCallBuffer buffer:serverToRSBufferMap.values())
        	writes.add(buffer.getBulkWrites());
        return writes;
	}
	
	public ObjectArrayList<KVPair> getKVPairs() throws Exception {
		SpliceLogUtils.trace(LOG, "getKVPairs");
		ObjectArrayList<KVPair> kvPairs = new ObjectArrayList<>();
        for(Pair<RegionCallBuffer,ServerName> buffer:startKeyToBufferMap.values()) {
        	kvPairs.addAll(buffer.getFirst().getBuffer());
        }
        return kvPairs;
	}
	
	@Override
	public void rebuildBuffer() {
		rebuildBuffer = true;
	}

	@Override
	public PreFlushHook getPreFlushHook() {
		return preFlushHook;
	}

	@Override
	public WriteConfiguration getWriteConfiguration() {
		return writeConfiguration;
	}
		
}

