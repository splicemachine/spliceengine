package com.splicemachine.pipeline.impl;

import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.WriteSemaphore;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class BulkWrite implements Externalizable {
    private static final long serialVersionUID = 1l;
    public ObjectArrayList<KVPair> mutations;
    private long bufferSize = -1;
    private String regionIdentifier;
    public WriteSemaphore.Status status;
    public int initialSize;

    public BulkWrite() { }

    public BulkWrite(ObjectArrayList<KVPair> mutations,String regionIdentifier) {
        assert regionIdentifier != null;
        this.mutations = mutations;
        this.regionIdentifier = regionIdentifier;
    }

    public BulkWrite(String regionIdentifier){
        assert regionIdentifier != null;
        this.mutations = ObjectArrayList.newInstance();
        this.regionIdentifier = regionIdentifier;
    }

    public ObjectArrayList<KVPair> getMutations() {
        return mutations;
    }

    public String getRegionIdentifier() {
        return regionIdentifier;
    }

    public void setRegionIdentifier(String encodedRegionName) {
        this.regionIdentifier = encodedRegionName;
    }

    public void addWrite(KVPair write){
        mutations.add(write);
    }

    @Override
    public String toString() {
        return "BulkWrite{" +
                ", regionIdentifier='" + regionIdentifier + '\'' +
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

    public byte[] encode() throws IOException{
        byte[] regionIdBytes = regionIdentifier.getBytes(); //we know this is UTF-8 encoding anyway
        /*
         * Encode KVPairs as follows:
         *
         * 1 byte Type Id:
         * All KV Pairs of this type
         *
         * There are 5 possible Types.
         */
        List<byte[]>[] kvPairBytes = new List[5]; //create one entry in the array for each Type of KVPair
        int foundTypes = 0;
        Object[] buffer = mutations.buffer;
        int encodedSize = 0;
        for(int i=0;i<mutations.size();i++){
            KVPair pair = (KVPair)buffer[i];
            byte typeByte = pair.getType().asByte();
            List<byte[]> typeList = kvPairBytes[typeByte];
            if(typeList==null) {
                typeList = new ArrayList<>();
                kvPairBytes[typeByte] = typeList;
                foundTypes++;
            }
            byte[] encodedKV = pair.encodeKeyValue();
            typeList.add(encodedKV);
            encodedSize+=encodedKV.length;
        }

        //encoded version is <txnLength><txn><regionIdLength><regionId><Encoding KVs>
        //where Encoding KVs = <Type><length><kvs><Type><length><kvs>...
        int finalByteSize = 4+regionIdBytes.length+foundTypes*5+encodedSize;

        byte[] finalBytes = new byte[finalByteSize];//might be a humongous allocation, but probably is small enough
        int offset = 0;
        BytesUtil.intToBytes(regionIdBytes.length,finalBytes,offset);
        offset+=4;
        System.arraycopy(regionIdBytes,0,finalBytes,offset,regionIdBytes.length);
        offset+=regionIdBytes.length;
        for(int i=0;i<kvPairBytes.length;i++){
            byte typeByte = (byte)i;
            List<byte[]> bytes = kvPairBytes[i];
            if(bytes!=null) {
                finalBytes[offset] = typeByte;
                offset++;
                BytesUtil.intToBytes(bytes.size(), finalBytes, offset);
                offset += 4;
                for (byte[] b : bytes) {
                    System.arraycopy(b, 0, finalBytes, offset, b.length);
                    offset += b.length;
                }
            }
        }
        return finalBytes;

    }

    public static BulkWrite decode(byte[] data, int offset,int length,long[] lengthHolder){
        int len=0;
        byte[] regionId = new byte[BytesUtil.bytesToInt(data,offset)];
        offset+=4;
        len+=4;
        System.arraycopy(data,offset,regionId,0,regionId.length);
        offset+=regionId.length;
        len+=regionId.length;
        ObjectArrayList<KVPair> mutations = null;
        while(len<length){
            KVPair.Type type = KVPair.Type.decode(data[offset]);
            offset++;
            len++;
            int kvPairSize = BytesUtil.bytesToInt(data,offset);
            offset+=4;
            len+=4;
            if(mutations==null)
                mutations = new ObjectArrayList<>(kvPairSize);
            for(int i=0;i<kvPairSize;i++) {
                mutations.add(KVPair.decode(type, data, offset, lengthHolder));
                offset += (int) lengthHolder[0];
                len+=(int)lengthHolder[0];
            }
        }
        lengthHolder[0] = len;
        return new BulkWrite(mutations, Bytes.toString(regionId));
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
//        TransactionOperations.getOperationFactory().writeTxn(txn, out);
        out.writeUTF(regionIdentifier);
        Object[] buffer = mutations.buffer;
        int iBuffer = mutations.size();
        out.writeInt(iBuffer);
        for (int i = 0; i< iBuffer; i++) {
            out.writeObject(buffer[i]);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
//        txn = TransactionOperations.getOperationFactory().readTxn(in);
        regionIdentifier = in.readUTF();
        int size = in.readInt();
        mutations = ObjectArrayList.newInstanceWithCapacity(size);
        for(int i=0;i<size;i++){
            mutations.add((KVPair)in.readObject());
        }
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof BulkWrite)) return false;
        BulkWrite write = (BulkWrite) obj;
        if (!this.regionIdentifier.equals(write.regionIdentifier)
                || !this.mutations.equals(write.mutations))
            return false;
        return true;
    }

}
