package com.splicemachine.hbase.writer;

import com.google.common.collect.Lists;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class BulkWrite implements Externalizable {
    private static final long serialVersionUID = 1l;

    private List<KVPair> mutations;
    private String txnId;
    private byte[] regionKey;

    public BulkWrite() { }

    public BulkWrite(List<KVPair> mutations, String txnId,byte[] regionKey) {
        this.mutations = mutations;
        this.txnId = txnId;
        this.regionKey = regionKey;
    }

    public BulkWrite(String txnId, byte[] regionKey){
        this.txnId = txnId;
        this.regionKey = regionKey;
        this.mutations = Lists.newArrayList();
    }

    public List<KVPair> getMutations() {
        return mutations;
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
        mutations = Lists.newArrayListWithCapacity(size);
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
}
