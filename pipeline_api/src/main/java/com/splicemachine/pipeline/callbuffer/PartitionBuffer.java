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

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.client.BulkWrite;
import com.splicemachine.storage.Partition;

import java.util.ArrayList;
import java.util.Collection;

/**
 * This is a data structure to buffer RPC calls to a specific partition.
 * The RPC call is a Splice specific type of operation (or mutation), which is called a KVPair.
 * A KVPair is a CRUD (INSERT, UPDATE, DELETE, or UPSERT) operation.
 * This class also adds a transactional context to each operation (call).
 */
public class PartitionBuffer{

    private Collection<KVPair> buffer;
    private int heapSize;
    private Partition partition;
    private PreFlushHook preFlushHook;
    private boolean skipIndexWrites;
    private boolean skipConflictDetection;
    private boolean skipWAL;
    private boolean rollforward;

    public PartitionBuffer(Partition partition, PreFlushHook preFlushHook, boolean skipIndexWrites, boolean skipConflictDetection, boolean skipWAL, boolean rollforward) {
        this.partition=partition;
        this.buffer = new ArrayList<>();
        this.preFlushHook = preFlushHook;
        this.skipIndexWrites = skipIndexWrites;
        this.skipConflictDetection = skipConflictDetection;
        this.skipWAL = skipWAL;
        this.rollforward = rollforward;
    }

    public void add(KVPair element) throws Exception {
        buffer.add(element);
        heapSize += element.getSize();
    }

    /**
     * In other words, calls are not executed.  The buffer's entries are all removed.
     */
    public void clear() throws Exception {
        heapSize = 0;
        buffer.clear(); // Can we do it faster via note on this method and then torch reference later?
    }

    public void close() throws Exception {
        buffer = null;
    }

    public boolean isEmpty() {
        return buffer.isEmpty();
    }

    public int getHeapSize() {
        return heapSize;
    }

    public int getBufferSize() {
        return buffer.size();
    }

    public BulkWrite getBulkWrite() throws Exception {
        return new BulkWrite(heapSize, preFlushHook.transform(buffer), partition.getName(), skipIndexWrites, skipConflictDetection, skipWAL, rollforward);
    }

    /**
     * Returns a boolean whether the row key is not contained by (is outside of) the region of this call buffer.
     *
     * @param key row key
     * @return true if the row key is not contained by (is outside of) the region of this call buffer
     */
    public boolean keyOutsideBuffer(byte[] key) {
        return !this.partition.containsRow(key);
    }

    public Collection<KVPair> getBuffer() {
        return buffer;
    }

    public Partition partition(){
        return partition;
    }

    @Override
    public String toString() {
        return "PartitionBuffer{" +
                "buffer=" + buffer +
                ", heapSize=" + heapSize +
                ", partition=" + partition +
                ", preFlushHook=" + preFlushHook +
                ", skipIndexWrites=" + skipIndexWrites +
                ", skipConflictDetection=" + skipConflictDetection +
                ", skipWAL=" + skipWAL +
                ", rollforward=" + rollforward +
                '}';
    }
}
