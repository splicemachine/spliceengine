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
import com.splicemachine.pipeline.api.*;
import com.splicemachine.pipeline.client.BulkWrite;
import com.splicemachine.pipeline.client.BulkWrites;
import com.splicemachine.pipeline.client.MergingWriteStats;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionServer;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import splice.com.google.common.collect.Lists;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Future;

/**
 * This is a data structure to buffer HBase RPC calls to a specific region server.
 * The call buffers are partitioned (organized) by region.  The region is identified by the starting row key for the region.
 * This means the entries that are buffered by this class are pairs consisting of the starting row key and the call buffer for the region.
 * <em>Please Note:</em> This data structure also contains a table name.  So there will be multiple instances of this class
 * for each region server, since each table and region server combination will have an instance of this class.
 */
class ServerCallBuffer implements CallBuffer<Pair<byte[], PartitionBuffer>> {

    private static final Logger LOG = Logger.getLogger(ServerCallBuffer.class);

    private final PartitionServer server;
    private final Writer writer;

    /**
     * Map of all the call buffers for each region on this region server.
     * The map is keyed by the starting row key for the regions.
     * And the value (entry) stored in the map is the buffer of calls for the region.
     * These calls are HBase RPC calls.
     * Regions are basically defined by their table name and the starting row key for the region.
     * If you know that information, you can get the call buffer to the region.
     */
    private final NavigableMap<byte[], PartitionBuffer> buffers;
    private final List<Future<WriteStats>> outstandingRequests = Lists.newArrayList();
    private final MergingWriteStats writeStats;
    private final WriteConfiguration writeConfiguration;
    private final byte[] tableName;
    private final TxnView txn;
    private final byte[] token;
    private Pair<byte[], PartitionBuffer> lastElement;

    public ServerCallBuffer(byte[] tableName,
                            TxnView txn,
                            byte[] token,
                            WriteConfiguration writeConfiguration,
                            PartitionServer server,
                            Writer writer,
                            final MergingWriteStats writeStats) {
        this.txn = txn;
        this.token = token;
        this.writeConfiguration = writeConfiguration;
        this.tableName = tableName;
        this.writeStats = writeStats;
        this.server= server;
        this.writer = writer;
        this.buffers = new TreeMap<>(ByteComparisons.comparator());
    }

    /**
     * Add a buffer of region calls to this region server's call buffers.
     *
     * @param element a pair that consists of the starting row key and the call buffer for the region
     */
    @Override
    public void add(Pair<byte[], PartitionBuffer> element) throws Exception {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "add {%s, %s}", Bytes.toString(element.getFirst()), element);
        buffers.put(element.getFirst(), element.getSecond());
        lastElement = element;
    }

    @Override
    public void addAll(Pair<byte[], PartitionBuffer>[] elements) throws Exception {
        for (Pair<byte[], PartitionBuffer> element : elements)
            add(element);
    }

    @Override
    public void addAll(Iterable<Pair<byte[], PartitionBuffer>> elements) throws Exception {
        for (Pair<byte[], PartitionBuffer> element : elements) {
            add(element);
        }
    }

    /**
     * Send buffered BulkWrites and check for (but do not wait for) writes in progress.
     */
    @Override
    public void flushBuffer() throws Exception {
        SpliceLogUtils.trace(LOG, "flushBuffer %s", this.server);
        if (writer == null || buffers.isEmpty()) {
            return;
        }
        flushBufferCheckPrevious();
        BulkWrites bulkWrites = getBulkWrites();
        if (bulkWrites!=null && bulkWrites.numEntries() != 0) {
            Future<WriteStats> write = writer.write(tableName, bulkWrites, writeConfiguration);
            outstandingRequests.add(write);
        }
    }

    @Override
    public void flushBufferAndWait() throws Exception {
        flushBuffer();
        //make sure all outstanding buffers complete before returning
        Iterator<Future<WriteStats>> futureIterator = outstandingRequests.iterator();
        while (futureIterator.hasNext()) {
            Future<WriteStats> future = futureIterator.next();
            futureIterator.remove();
            WriteStats retStats = future.get();//check for errors
            writeStats.merge(retStats);
        }
    }

    @Override
    public void close() throws Exception {
        if (writer == null) {// No Op Buffer
            return;
        }
        flushBufferAndWait();
    }

    @Override public PreFlushHook getPreFlushHook() { return null; }
    @Override public WriteConfiguration getWriteConfiguration() { return writeConfiguration; }
    @Override public TxnView getTxn() { return txn; }

    @Override
    public Partition destinationPartition(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    public BulkWrites getBulkWrites() throws Exception {
        Set<Entry<byte[], PartitionBuffer>> entries = this.buffers.entrySet();
        Collection<BulkWrite> bws = new ArrayList<>(entries.size());
        for (Entry<byte[], PartitionBuffer> regionEntry : this.buffers.entrySet()) {
            PartitionBuffer value = regionEntry.getValue();
            if (value.isEmpty()) continue;
            bws.add(value.getBulkWrite());
            value.clear(); // zero out
        }
        if(bws.isEmpty()) return null;
        else
            return new BulkWrites(bws, this.txn, this.buffers.lastKey(),token);
    }

    public int getHeapSize() {
        int heapSize = 0;
        for (Entry<byte[], PartitionBuffer> element : buffers.entrySet())
            heapSize += element.getValue().getHeapSize();
        return heapSize;
    }

    public int getKVPairSize() {
        int size = 0;
        for (Entry<byte[], PartitionBuffer> element : buffers.entrySet())
            size += element.getValue().getBufferSize();
        return size;
    }

    public Writer getWriter() { return writer; }
    public PartitionServer getServer() { return server; }

    @Override
    public Pair<byte[], PartitionBuffer> lastElement(){
        return lastElement;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void flushBufferCheckPrevious() throws Exception {
        Iterator<Future<WriteStats>> futureIterator = outstandingRequests.iterator();
        while (futureIterator.hasNext()) {
            Future<WriteStats> future = futureIterator.next();
            if (future.isDone()) {
                futureIterator.remove();
                WriteStats retStats = future.get();//check for errors
                writeStats.merge(retStats);
            }
        }
    }
}
