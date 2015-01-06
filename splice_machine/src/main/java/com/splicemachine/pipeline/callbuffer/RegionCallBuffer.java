package com.splicemachine.pipeline.callbuffer;

import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.PreFlushHook;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.HRegionInfo;

public class RegionCallBuffer implements CallBuffer<KVPair> {

    private ObjectArrayList<KVPair> buffer;
    private int heapSize;
    private final HRegionInfo hregionInfo;
    private final TxnView txn;
    private final PreFlushHook preFlushHook;

    public RegionCallBuffer(HRegionInfo hregionInfo, TxnView txn, PreFlushHook preFlushHook) {
        this.hregionInfo = hregionInfo;
        this.buffer = ObjectArrayList.newInstance();
        this.txn = txn;
        this.preFlushHook = preFlushHook;
    }

    @Override
    public void add(KVPair element) throws Exception {
        buffer.add(element);
        heapSize += element.getSize();
    }

    @Override
    public void addAll(KVPair[] elements) throws Exception {
        for (KVPair element : elements)
            add(element);
    }

    @Override
    public void addAll(ObjectArrayList<KVPair> elements) throws Exception {
        Object[] elementArray = elements.buffer;
        int size = elements.size();
        for (int i = 0; i < size; i++) {
            add((KVPair) elementArray[i]);
        }
    }

    @Override
    public void flushBuffer() throws Exception {
        heapSize = 0;
        buffer.clear(); // Can we do it faster via note on this method and then torch reference later?
    }

    @Override
    public void close() throws Exception {
        buffer.clear();
        buffer.release();
        buffer = null;
    }

    public int getHeapSize() {
        return heapSize;
    }

    public int getBufferSize() {
        return buffer.size();
    }

    public BulkWrite getBulkWrite() throws Exception {
        ObjectArrayList<KVPair> transform = preFlushHook.transform(buffer);
        return new BulkWrite(transform, txn, hregionInfo.getStartKey(), hregionInfo.getEncodedName());
    }

    public boolean keyOutsideBuffer(byte[] key) {
        return !this.hregionInfo.containsRow(key);
    }

    public HRegionInfo getHregionInfo() {
        return hregionInfo;
    }

    @Override
    public PreFlushHook getPreFlushHook() {
        return preFlushHook;
    }

    @Override
    public WriteConfiguration getWriteConfiguration() {
        return null;
    }

    public ObjectArrayList<KVPair> getBuffer() {
        return buffer;
    }

}

