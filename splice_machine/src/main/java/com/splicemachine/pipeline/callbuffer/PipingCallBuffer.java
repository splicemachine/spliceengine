package com.splicemachine.pipeline.callbuffer;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.regioninfocache.RegionCache;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.*;
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
 * A CallBuffer which pre-maps (or pipes) entries to a separate buffer based on which region
 * the write belongs to.  These "pre-mapped pipes" are incorporated into ???
 *
 * This implementation obeys any per-region bounds set in the passed in
 * {@link BufferConfiguration} entity.
 *
 * This class is <em>not</em> Thread-safe. Its use should be restricted to a
 * single thread. If that is not possible, then external synchronization is
 * necessary.
 *
 * @author Scott Fines
 * Created on: 8/27/13
 */
public class PipingCallBuffer implements RecordingCallBuffer<KVPair>, CanRebuild {
    private static final Logger LOG = Logger.getLogger(PipingCallBuffer.class);

    /**
     * Map from the region's starting row key to a pair consisting of the region's call buffer and server name.
     */
    private NavigableMap<byte[],Pair<RegionCallBuffer,ServerName>> startKeyToRegionCBMap;

    /**
     * Map from a server name to the region server's call buffer.
     */
    private NavigableMap<ServerName,RegionServerCallBuffer> serverNameToRegionServerCBMap;

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
        this.startKeyToRegionCBMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        this.serverNameToRegionServerCBMap = new TreeMap<>();
        this.bufferConfiguration = bufferConfiguration;
        this.preFlushHook = preFlushHook;
        MetricFactory metricFactory = writeConfiguration!=null? writeConfiguration.getMetricFactory(): Metrics.noOpMetricFactory();
        writeStats = new MergingWriteStats(metricFactory);
    }

    /**
     * Add a KVPair object ("Splice mutation") to the call buffer.
     * This method will "pipe" (set) the mutation into the correct region's call buffer for later flushing.
     */
    @Override
    public void add(KVPair element) throws Exception {
        assert element!=null: "Cannot add a non-null element!";
        rebuildIfNecessary();
        Map.Entry<byte[],Pair<RegionCallBuffer,ServerName>> entry = startKeyToRegionCBMap.floorEntry(element.getRowKey());
        if(entry==null) entry = startKeyToRegionCBMap.firstEntry();
        assert entry!=null;
        RegionCallBuffer regionCB = entry.getValue().getFirst();
        if (LOG.isTraceEnabled())
        	SpliceLogUtils.trace(LOG, "Adding KVPair object (Splice mutation) %s to the call buffer for the region %s",
        			element, regionCB.getHregionInfo().getRegionNameAsString());
        regionCB.add(element);
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
        for (RegionServerCallBuffer buffer : serverNameToRegionServerCBMap.values()) {
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
        totalFlushes++;
    }

    private void rebuildIfNecessary() throws Exception {
        if (!rebuildBuffer && startKeyToRegionCBMap != null && !startKeyToRegionCBMap.isEmpty()) {
            return; //no need to rebuild the buffer
        }

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

        // Get all of the "Splice mutations" that need to be performed on this table.
        Collection<KVPair> items = getKVPairs();  // KVPairs are simple, just a row key, value, and "Splice mutation" type.
        assert items != null;

        // The following block of code flushes the region and region server call buffers.
        if(startKeyToRegionCBMap!=null) {
            for (Pair<RegionCallBuffer, ServerName> buffer : startKeyToRegionCBMap.values())
                buffer.getFirst().flushBuffer();
            for (RegionServerCallBuffer buffer : serverNameToRegionServerCBMap.values()) {
                assert (buffer.getBulkWrites().numEntries() == 0);  // This asserts that there are not any outstanding RegionCallBuffers for the region server that need to be flushed still.
                buffer.close();
            }
        }
        this.startKeyToRegionCBMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        this.serverNameToRegionServerCBMap = new TreeMap<>();
        currentHeapSize=0;
        currentKVPairSize=0;

        // Get all of the regions for the table and the servers that the regions reside on.
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "rebuilding region map for table %s", Bytes.toString(tableName));
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
            RegionServerCallBuffer regionServerCB = this.serverNameToRegionServerCBMap.get(serverName);

            // Do we have this RS call buffer already?
            if (regionServerCB == null) {
                SpliceLogUtils.debug(LOG, "adding RegionServerCallBuffer for server %s and table %s", serverName, tableName);
                regionServerCB = new RegionServerCallBuffer(tableName,
                        txn,
                        writeConfiguration,
                        serverName,
                        (writer != null ? new RegulatedWriter(writer) : null),
                        writeStats);
                serverNameToRegionServerCBMap.put(serverName, regionServerCB);
            }

            // Attempt to get the call buffer for the correct region that contains this row key.
            Entry<byte[], Pair<RegionCallBuffer, ServerName>> startKeyToRegionCBEntry = this.startKeyToRegionCBMap.floorEntry(startKey);
            RegionCallBuffer regionCB = null;
            if (startKeyToRegionCBEntry != null)
                regionCB = startKeyToRegionCBEntry.getValue().getFirst();

            // Check if the region call buffer does not exist or if the row is outside of this region (comes after it).
            if (startKeyToRegionCBEntry == null || regionCB.keyOutsideBuffer(startKey)) {

            	// Debug logging stuff.
            	if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(LOG, "lower startKey %s", startKeyToRegionCBEntry);
                    if (regionCB!=null) {
                        HRegionInfo info = regionCB.getHregionInfo();
                        SpliceLogUtils.debug(LOG, "region %s", info.getRegionNameAsString());
                        SpliceLogUtils.debug(LOG, "region startKey %s", new Object[]{info.getStartKey()});
                        SpliceLogUtils.debug(LOG, "region endKey %s", new Object[]{info.getEndKey()});
                        SpliceLogUtils.debug(LOG, "comparison %d", Bytes.compareTo(startKey, info.getStartKey()));
                    }
                    SpliceLogUtils.debug(LOG, "startKey %s", new Object[]{startKey});
                    SpliceLogUtils.debug(LOG, "key outside buffer, add new region (suspect) %s", region.getRegionNameAsString());
                }

            	// Create a new RegionCallBuffer, add it to the map, and add it to the RegionServerCallBuffer.
                RegionCallBuffer newBuffer = new RegionCallBuffer(region, preFlushHook);
                startKeyToRegionCBMap.put(startKey, Pair.newPair(newBuffer, serverName));
                regionServerCB.add(Pair.newPair(startKey, newBuffer));
            } else {
                throw new RuntimeException("Not Functional Path");
            }
        }

        rebuildBuffer=false;
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "Adding %s KVPair objects ('Splice mutations') back into the appropriate region call buffers", items.size());
        record = false;

        // Add all of the KVPairs (Splice mutations) to the correct region call buffers.
        this.addAll(items);
        record = true;
    }

    /**
     * Add a bunch of KVPairs ("Splice mutations") to the call buffers.
     * This method will "pipe" (set) the mutations into the correct region's call buffers for later flushing.
     */
    @Override
    public void addAll(KVPair[] elements) throws Exception {
        for(KVPair element:elements)
            add(element);
    }

    /**
     * Add a bunch of KVPairs ("Splice mutations") to the call buffers.
     * This method will "pipe" (set) the mutations into the correct region's call buffers for later flushing.
     */
    @Override
    public void addAll(Iterable<KVPair> elements) throws Exception {
        for(KVPair element:elements){
            add(element);
        }
    }

    @Override
    public void flushBuffer() throws Exception {
    	SpliceLogUtils.debug(LOG, "flushBuffer");
        if (serverNameToRegionServerCBMap == null) return;
    	//flush all buffers
        rebuildIfNecessary();
        for(RegionServerCallBuffer buffer:serverNameToRegionServerCBMap.values()) {
        	if (LOG.isDebugEnabled())
        		SpliceLogUtils.debug(LOG, "flushBuffer {table=%s, server=%s, rows=%d ",Bytes.toString(tableName),buffer.getServerName(),buffer.getKVPairSize());
            buffer.flushBuffer();
        }
        currentHeapSize=0;
        currentKVPairSize=0;
        totalFlushes++;
    }

    @Override
    public void close() throws Exception {
    	SpliceLogUtils.debug(LOG, "close");
    	//close all buffers
        if (serverNameToRegionServerCBMap == null) return;
        rebuildIfNecessary();
        for(RegionServerCallBuffer buffer:serverNameToRegionServerCBMap.values()) {
        	if (LOG.isDebugEnabled())
        		SpliceLogUtils.debug(LOG, "Closing {table=%s, server=%s}",Bytes.toString(tableName),buffer.getServerName());
            buffer.close();
        }
        
        for(Pair<RegionCallBuffer,ServerName> buffer:startKeyToRegionCBMap.values())
        	buffer.getFirst().close();        
        serverNameToRegionServerCBMap = null;
        startKeyToRegionCBMap = null;
        currentHeapSize = 0;
        currentKVPairSize = 0;
    }

    @Override public long getTotalElementsAdded() { return totalElementsAdded; }
    @Override public long getTotalBytesAdded() { return totalBytesAdded; }
    @Override public long getTotalFlushes() {  return totalFlushes;  }
    @Override public double getAverageEntriesPerFlush() { return ((double)totalElementsAdded)/getTotalFlushes(); }
    @Override public double getAverageSizePerFlush() { return ((double) totalBytesAdded)/getTotalFlushes(); }
    @Override public CallBuffer<KVPair> unwrap() { return this; }
    @Override public WriteStats getWriteStats() { return writeStats; }

    public List<BulkWrites> getBulkWrites() throws Exception {
        SpliceLogUtils.trace(LOG, "getBulkWrites");
        rebuildIfNecessary();
        List<BulkWrites> writes = new ArrayList<>(serverNameToRegionServerCBMap.size());
        for(RegionServerCallBuffer buffer:serverNameToRegionServerCBMap.values())
            writes.add(buffer.getBulkWrites());
        return writes;
    }

	/**
	 * Return the KVPairs ("Splice mutations") which are buffered for all regions on all servers for the specific table associated with this PipingCallBuffer.
	 * @return list of all "Splice mutations" that are buffered for the table
	 * @throws Exception
	 */
    public Collection<KVPair> getKVPairs() throws Exception {
        SpliceLogUtils.trace(LOG, "getKVPairs");
        Collection<KVPair> kvPairs = new ArrayList<>();
        for(Pair<RegionCallBuffer,ServerName> buffer:startKeyToRegionCBMap.values()) {
            kvPairs.addAll(buffer.getFirst().getBuffer());
        }
        return kvPairs;
    }

	/**
	 * Mark the buffer to be rebuilt.
	 * <em>Please Note:</em> This method does not actually rebuild the buffer.  It only marks it to be rebuilt later.
	 */
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
