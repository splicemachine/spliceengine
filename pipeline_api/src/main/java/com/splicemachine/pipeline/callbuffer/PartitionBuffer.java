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

    public PartitionBuffer(Partition partition,PreFlushHook preFlushHook,boolean skipIndexWrites) {
        this.partition=partition;
        this.buffer = new ArrayList<>();
        this.preFlushHook = preFlushHook;
        this.skipIndexWrites = skipIndexWrites;
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
        return new BulkWrite(heapSize, preFlushHook.transform(buffer), partition.getName(), skipIndexWrites);
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
}
