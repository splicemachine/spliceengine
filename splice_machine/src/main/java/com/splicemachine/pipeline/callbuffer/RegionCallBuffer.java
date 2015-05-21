package com.splicemachine.pipeline.callbuffer;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.PreFlushHook;
import com.splicemachine.pipeline.impl.BulkWrite;
import org.apache.hadoop.hbase.HRegionInfo;

import java.util.ArrayList;
import java.util.Collection;

/**
 * This is a data structure to buffer HBase RPC calls to a specific region.
 * The RPC call is a Splice specific type of operation (or mutation), which is called a KVPair.
 * A KVPair is a CRUD (INSERT, UPDATE, DELETE, or UPSERT) operation.
 * This class also adds a transactional context to each operation (call).
 */
public class RegionCallBuffer {

    private Collection<KVPair> buffer;
    private int heapSize;
    private HRegionInfo hregionInfo;
    private PreFlushHook preFlushHook;

    public RegionCallBuffer(HRegionInfo hregionInfo, PreFlushHook preFlushHook) {
        this.hregionInfo = hregionInfo;
        this.buffer = new ArrayList<>();
        this.preFlushHook = preFlushHook;
    }

    public void add(KVPair element) throws Exception {
        buffer.add(element);
        heapSize += element.getSize();
    }

    public void addAll(KVPair[] elements) throws Exception {
        for (KVPair element : elements)
            add(element);
    }

    public void addAll(Iterable<KVPair> elements) throws Exception {
        for (KVPair element : elements) {
            add(element);
        }
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
        return new BulkWrite(heapSize, preFlushHook.transform(buffer), hregionInfo.getEncodedName());
    }

    /**
     * Returns a boolean whether the row key is not contained by (is outside of) the region of this call buffer.
     *
     * @param key row key
     * @return true if the row key is not contained by (is outside of) the region of this call buffer
     */
    public boolean keyOutsideBuffer(byte[] key) {
        return !this.hregionInfo.containsRow(key);
    }

    public HRegionInfo getHregionInfo() {
        return hregionInfo;
    }

    public Collection<KVPair> getBuffer() {
        return buffer;
    }
}
