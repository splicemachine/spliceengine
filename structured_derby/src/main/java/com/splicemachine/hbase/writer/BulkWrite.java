package com.splicemachine.hbase.writer;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Set;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class BulkWrite implements Externalizable {
    private static final long serialVersionUID = 1l;

    private Set<KVPair> mutations;
    private String txnId;
    private byte[] regionKey;
    private long bufferSize = -1;

    public BulkWrite() { }

    public BulkWrite(List<KVPair> mutations, String txnId,byte[] regionKey) {
        this.mutations = Sets.newHashSet(mutations);
        this.txnId = txnId;
        this.regionKey = regionKey;
    }

    public BulkWrite(String txnId, byte[] regionKey){
        this.txnId = txnId;
        this.regionKey = regionKey;
        this.mutations = Sets.newHashSet();
    }

    public List<KVPair> getMutations() {
        return Lists.newArrayList(mutations);
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
        out.writeInt(mutations.size());
        for(KVPair kvPair:mutations)
            out.writeObject(kvPair);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        txnId = in.readUTF();
        int size = in.readInt();
        mutations = Sets.newHashSetWithExpectedSize(size);
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
            for(KVPair kvPair:mutations){
                heap+=kvPair.getSize();
            }
            bufferSize= heap;
        }
        return bufferSize;
    }
}
