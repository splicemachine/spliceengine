package com.splicemachine.hbase.writer;

import com.carrotsearch.hppc.ObjectArrayList;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class BulkWrite implements Externalizable {
    private static final long serialVersionUID = 1l;

    private ObjectArrayList<KVPair> mutations;
    private String txnId;
    private byte[] regionKey;
    private long bufferSize = -1;

    public BulkWrite() { }

    public BulkWrite(ObjectArrayList<KVPair> mutations, String txnId,byte[] regionKey) {
        this.mutations = ObjectArrayList.from(mutations); // Is this needed?
        this.txnId = txnId;
        this.regionKey = regionKey;
    }

    public BulkWrite(String txnId, byte[] regionKey){
        this.txnId = txnId;
        this.regionKey = regionKey;
        this.mutations = ObjectArrayList.newInstance();
    }

    public ObjectArrayList<KVPair> getMutations() {
        return ObjectArrayList.from(mutations);			// Is this needed?
    }

    public String getTxnId() {
        return txnId;
    }

    public byte[] getRegionKey() {
        return regionKey;
    }

    public void addWrite(KVPair write){
        mutations.add(write);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(txnId);
        Object[] buffer = mutations.buffer;
        int iBuffer = mutations.size();
        out.writeInt(iBuffer);
        for (int i = 0; i< iBuffer; i++) {
        	out.writeObject((KVPair) buffer[i]);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        txnId = in.readUTF();
        int size = in.readInt();
        mutations = ObjectArrayList.newInstanceWithCapacity(size);
        for(int i=0;i<size;i++){
            mutations.add((KVPair)in.readObject());
        }
    }

    @Override
    public String toString() {
        return "BulkWrite{" +
                "txnId='" + txnId + '\'' +
                ", regionKey=" + regionKey +
                ", rows="+mutations.size()+
                '}';
    }

    public long getBufferSize() {
        if(bufferSize<0){
            long heap = 0l;
            Object[] buffer = mutations.buffer;
            int iBuffer = mutations.size();
            for (int i = 0; i< iBuffer; i++) {
            KVPair kvPair = (KVPair) buffer[i];
                heap+=kvPair.getSize();
            }
            bufferSize= heap;
        }
        return bufferSize;
    }
    
    public Object[] getBuffer() {
    	return mutations.buffer;
    }

    public int getSize() {
    	return mutations.size();
    }
    
    
}
