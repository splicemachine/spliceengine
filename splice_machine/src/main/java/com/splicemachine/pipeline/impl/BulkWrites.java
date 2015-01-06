package com.splicemachine.pipeline.impl;

import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.hbase.KVPair;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Extension of BulkWrites to wrap for a region server.
 *
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class BulkWrites implements Externalizable {
    private static final long serialVersionUID = 1l;
    public ObjectArrayList<BulkWrite> bulkWrites;

    public BulkWrites() {
        bulkWrites = new ObjectArrayList<>();
    }

    public BulkWrites(ObjectArrayList<BulkWrite> bulkWrites) {
        this.bulkWrites = bulkWrites;
    }

    public byte[] getRegionKey() {
        assert bulkWrites != null && bulkWrites.size() > 0;
        return bulkWrites.get(0).getRegionKey();
    }

    public ObjectArrayList<BulkWrite> getBulkWrites() {
        return bulkWrites;
    }

    public void addBulkWrite(BulkWrite bulkWrite) {
        bulkWrites.add(bulkWrite);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BulkWrites{");
        Object[] buffer = bulkWrites.buffer;
        int iBuffer = bulkWrites.size();
        for (int i = 0; i < iBuffer; i++) {
            sb.append(buffer[i]);
            if (i != iBuffer - 1)
                sb.append(",");
        }
        sb.append("}");
        return sb.toString();
    }

    public long getKVPairSize() {
        long size = 0;
        Object[] buffer = bulkWrites.buffer;
        int iBuffer = bulkWrites.size();
        for (int i = 0; i < iBuffer; i++) {
            BulkWrite bulkWrite = (BulkWrite) buffer[i];
            size += bulkWrite.getSize();
        }
        return size;
    }

    public long getBufferHeapSize() {
        long heap = 0l;
        Object[] buffer = bulkWrites.buffer;
        int iBuffer = bulkWrites.size();
        for (int i = 0; i < iBuffer; i++) {
            BulkWrite bulkWrite = (BulkWrite) buffer[i];
            heap += bulkWrite.getBufferSize();
        }
        return heap;
    }

    public ObjectArrayList<KVPair> getAllCombinedKeyValuePairs() {
        ObjectArrayList<KVPair> pairs = new ObjectArrayList<>();
        Object[] buffer = bulkWrites.buffer;
        int iBuffer = bulkWrites.size();
        for (int i = 0; i < iBuffer; i++) {
            BulkWrite bulkWrite = (BulkWrite) buffer[i];
            pairs.addAll(bulkWrite.getMutations());
        }
        return pairs;
    }

    public Object[] getBuffer() {
        return bulkWrites.buffer;
    }

    /**
     * @return the number of rows in the bulk write
     */
    public int numEntries() {
        int numEntries = 0;
        int size = bulkWrites.size();
        Object[] buffer = bulkWrites.buffer;
        for (int i = 0; i < size; i++) {
            BulkWrite bw = (BulkWrite) buffer[i];
            numEntries += bw.getSize();
        }
        return numEntries;
    }

    /**
     * @return the number of regions in this write
     */
    public int numRegions() {
        return bulkWrites.size();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        Object[] buffer = bulkWrites.buffer;
        int iBuffer = bulkWrites.size();
        out.writeInt(iBuffer);
        for (int i = 0; i < iBuffer; i++) {
            out.writeObject(buffer[i]);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        bulkWrites = ObjectArrayList.newInstanceWithCapacity(size);
        for (int i = 0; i < size; i++) {
            bulkWrites.add((BulkWrite) in.readObject());
        }
    }

    @Override
    public boolean equals(Object obj) {
        return (this == obj) || (obj instanceof BulkWrites) && this.bulkWrites.equals(((BulkWrites) obj).bulkWrites);
    }

    public void close() {
        int isize = bulkWrites.size();
        Object[] buffer = bulkWrites.buffer;
        for (int i = 0; i < isize; i++) {
            BulkWrite bulkWrite = (BulkWrite) buffer[i];
            bulkWrite.mutations = null;
            bulkWrite = null;
        }
        bulkWrites = null;
    }

}