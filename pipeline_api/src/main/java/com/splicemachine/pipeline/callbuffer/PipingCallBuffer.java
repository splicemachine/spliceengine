/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.pipeline.callbuffer;

import com.splicemachine.access.util.ByteComparisons;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.*;
import com.splicemachine.pipeline.client.BulkWrites;
import com.splicemachine.pipeline.client.MergingWriteStats;
import com.splicemachine.pipeline.config.UpdatingWriteConfiguration;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.pipeline.writer.RegulatedWriter;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionServer;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import java.util.*;

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
public class PipingCallBuffer implements RecordingCallBuffer<KVPair>, Rebuildable{
    private static final Logger LOG = Logger.getLogger(PipingCallBuffer.class);

    /**
     * Map from the region's starting row key to the region's call buffer.
     */
    private NavigableMap<byte[], PartitionBuffer> startKeyToRegionCBMap;

    /**
     * Map from a server name to the region server's call buffer.
     */
    private Map<PartitionServer,ServerCallBuffer> serverNameToRegionServerCBMap;

    private final Writer writer;
    private final boolean skipIndexWrites;
    private final TxnView txn;
    private final byte[] token;
    //private final RegionCache regionCache;
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
    private final Partition table;
    private KVPair lastKvPair;
    private boolean autoFlush = true;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
    public PipingCallBuffer(Partition table,
                            TxnView txn,
                            byte[] token,
                            Writer writer,
                            PreFlushHook preFlushHook,
                            WriteConfiguration writeConfiguration,
                            BufferConfiguration bufferConfiguration,
                            boolean skipIndexWrites) {
        this.writer = writer;
        this.table = table;
        this.token = token;
        this.skipIndexWrites = skipIndexWrites;
        this.txn = txn;
        this.writeConfiguration = new UpdatingWriteConfiguration(writeConfiguration,this);
        this.startKeyToRegionCBMap = new TreeMap<>(ByteComparisons.comparator());
        this.serverNameToRegionServerCBMap = new HashMap<>();
        this.bufferConfiguration = bufferConfiguration;
        this.preFlushHook = preFlushHook;
        MetricFactory metricFactory = writeConfiguration!=null? writeConfiguration.getMetricFactory(): Metrics.noOpMetricFactory();
        writeStats = new MergingWriteStats(metricFactory);
    }

    public PipingCallBuffer(Partition table,
                            TxnView txn,
                            byte[] token,
                            Writer writer,
                            PreFlushHook preFlushHook,
                            WriteConfiguration writeConfiguration,
                            BufferConfiguration bufferConfiguration,
                            boolean skipIndexWrites,
                            boolean autoFlush) {
        this(table, txn, token, writer, preFlushHook, writeConfiguration, bufferConfiguration, skipIndexWrites);
        this.autoFlush = autoFlush;
    }
    /**
     * Add a KVPair object ("Splice mutation") to the call buffer.
     * This method will "pipe" (set) the mutation into the correct region's call buffer for later flushing.
     */
    @Override
    public void add(KVPair element) throws Exception {
        assert element!=null: "Cannot add a non-null element!";
        lastKvPair = element;
        rebuildIfNecessary();
        Map.Entry<byte[], PartitionBuffer> entry = startKeyToRegionCBMap.floorEntry(element.getRowKey());
        if(entry==null) entry = startKeyToRegionCBMap.firstEntry();
        assert entry!=null;
        PartitionBuffer regionCB = entry.getValue();
        if (LOG.isTraceEnabled())
        	SpliceLogUtils.trace(LOG, "Adding KVPair object (Splice mutation) %s to the call buffer for the region %s",
        			element, regionCB.partition().getName());
        regionCB.add(element);
		long size = element.getSize();
        currentHeapSize+=size;
        currentKVPairSize++;
        if (record) {
            totalElementsAdded++;
            totalBytesAdded +=size;
        }
        if(writer!=null && (currentHeapSize>=bufferConfiguration.getMaxHeapSize()
                || currentKVPairSize >= bufferConfiguration.getMaxEntries()) && autoFlush) {
            flushLargestBuffer();
        }
    }

    private void flushLargestBuffer() throws Exception {
        int maxSize = 0;
        ServerCallBuffer bufferToFlush = null;
        for (ServerCallBuffer buffer : serverNameToRegionServerCBMap.values()) {
            if (buffer.getHeapSize() > maxSize) {
                bufferToFlush = buffer;
                maxSize = buffer.getHeapSize();
            }
        }
        assert bufferToFlush!=null;
        currentHeapSize-=maxSize;
        currentKVPairSize-=bufferToFlush.getKVPairSize();
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "flushLargestBuffer {table=%s, size=%d, rows=%d}",table.getTableName(),bufferToFlush.getHeapSize(),bufferToFlush.getKVPairSize());
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
        if (startKeyToRegionCBMap != null) {
            for (PartitionBuffer buffer : startKeyToRegionCBMap.values()) {
                buffer.clear();
            }
            for (ServerCallBuffer buffer : serverNameToRegionServerCBMap.values()) {
                assert buffer.getBulkWrites()==null || (buffer.getBulkWrites().numEntries() == 0);  // This asserts that there are not any outstanding RegionCallBuffers for the region server that need to be flushed still.
                buffer.close();
            }
        }
        startKeyToRegionCBMap = new TreeMap<>(ByteComparisons.comparator());
        serverNameToRegionServerCBMap = new HashMap<>();
        currentHeapSize=0;
        currentKVPairSize=0;

        // Get all of the regions for the table and the servers that the regions reside on.
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"rebuilding region map for table %s",table.getTableName());
        List<Partition> regions = table.subPartitions();
        if (LOG.isDebugEnabled()) {
            for (Partition region: regions) {
                SpliceLogUtils.debug(LOG, "region %s on server %s",region.getName(), region.owningServer());
            }
        }

        for(Partition region: regions){
            PartitionServer server = region.owningServer();
            ServerCallBuffer regionServerCB = serverNameToRegionServerCBMap.get(server);
            if (regionServerCB == null) {
                SpliceLogUtils.debug(LOG, "adding ServerCallBuffer for server %s and table %s", server, table.getTableName());
                regionServerCB = new ServerCallBuffer(Bytes.toBytes(table.getName()),
                        txn,
                        token,
                        writeConfiguration,
                        server,
                        (writer != null ? new RegulatedWriter(writer) : null),
                        writeStats);
                serverNameToRegionServerCBMap.put(server, regionServerCB);
            }

            // Attempt to get the call buffer for the correct region that contains this row key.
            byte[] startKey = region.getStartKey();
            PartitionBuffer buffer = startKeyToRegionCBMap.get(startKey);
            if (buffer == null) {
                buffer = new PartitionBuffer(region, preFlushHook, skipIndexWrites,
                        writeConfiguration.skipConflictDetection(), writeConfiguration.skipWAL(), writeConfiguration.rollForward());
                startKeyToRegionCBMap.put(startKey, buffer);
                regionServerCB.add(Pair.newPair(startKey, buffer));
            }
            else {
                String oldHostAndPort = buffer.partition().owningServer().getHostAndPort();
                String newHostAndPort = server.getHostAndPort();
                if (!oldHostAndPort.equals(newHostAndPort)) {
                    SpliceLogUtils.warn(LOG, "different locations for the same key: region %s at location %s (ignored), region %s at location %s (used)",
                            region.getName(), newHostAndPort, buffer.partition().getName(), oldHostAndPort);
                }
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
        if (serverNameToRegionServerCBMap == null) {
            return;
        }
        // flush all buffers
        rebuildIfNecessary();
        for (ServerCallBuffer buffer : serverNameToRegionServerCBMap.values()) {
            buffer.flushBuffer();
        }
        currentHeapSize = 0;
        currentKVPairSize = 0;
        totalFlushes++;
    }

    @Override
    public void flushBufferAndWait() throws Exception {
        flushBuffer();
        if (serverNameToRegionServerCBMap != null)
            for (ServerCallBuffer buffer : serverNameToRegionServerCBMap.values()) {
                buffer.flushBufferAndWait();
            }
    }

    @Override
    public void close() throws Exception {
        // close all buffers
        if (serverNameToRegionServerCBMap == null) {
            return;
        }
        rebuildIfNecessary();

        // Table
        try {
            table.close();
        } catch (Exception e) {
            LOG.warn("Exception while closing table", e);
        }

        // Server
        for (ServerCallBuffer buffer : serverNameToRegionServerCBMap.values()) {
            buffer.close();
        }
        // Region
        for (PartitionBuffer buffer : startKeyToRegionCBMap.values()) {
            buffer.close();
        }

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
        rebuildIfNecessary();
        List<BulkWrites> writes = new ArrayList<>(serverNameToRegionServerCBMap.size());
        for(ServerCallBuffer buffer:serverNameToRegionServerCBMap.values()) {
            BulkWrites addedWrite = buffer.getBulkWrites();
            if (addedWrite!=null)
                writes.add(addedWrite);
        }
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
        for(PartitionBuffer buffer : startKeyToRegionCBMap.values()) {
            kvPairs.addAll(buffer.getBuffer());
        }
        return kvPairs;
    }

	/**
	 * Mark the buffer to be rebuilt.
	 * <em>Please Note:</em> This method does not actually rebuild the buffer.  It only marks it to be rebuilt later.
	 */
    @Override
    public void rebuild() {
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

    @Override
    public TxnView getTxn() {
        return txn;
    }

    @Override
    public Partition destinationPartition(){
        return table;
    }

    @Override
    public KVPair lastElement(){
        return lastKvPair;
    }
}
