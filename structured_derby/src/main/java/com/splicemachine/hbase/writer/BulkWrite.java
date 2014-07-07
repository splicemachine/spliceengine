package com.splicemachine.hbase.writer;

import com.carrotsearch.hppc.ObjectArrayList;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TransactionStorage;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.impl.ActiveWriteTxn;
import com.splicemachine.si.impl.InheritingTxnView;
import com.splicemachine.si.impl.LazyTxn;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.SnappyCodec;

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
    private static SnappyCodec snappy;
    private ObjectArrayList<KVPair> mutations;
//    private String txnId;
		private Txn txn;
    private byte[] regionKey;
    private long bufferSize = -1;

    static {
    	snappy = new SnappyCodec();
    }
    
    public BulkWrite() { }

    public BulkWrite(ObjectArrayList<KVPair> mutations, Txn txn,byte[] regionKey) {
        this.mutations = mutations;
        this.regionKey = regionKey;
				this.txn = txn;
    }

    public BulkWrite(Txn txn, byte[] regionKey){
        this.regionKey = regionKey;
        this.mutations = ObjectArrayList.newInstance();
				this.txn = txn;
    }

    public ObjectArrayList<KVPair> getMutations() {
        return mutations;
    }

//    public String getTxnId() {
//        return txnId;
//    }

		public Txn getTxn(){ return txn; }

    public byte[] getRegionKey() {
        return regionKey;
    }

    public void addWrite(KVPair write){
        mutations.add(write);
    }

    @Override
    public String toString() {
        return "BulkWrite{" +
                "txn='" + txn + '\'' +
                ", regionKey=" + Bytes.toStringBinary(regionKey) +
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
		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				out.writeLong(txn.getTxnId());
				out.writeLong(txn.getEffectiveBeginTimestamp());
				out.writeBoolean(txn.isAdditive());
				Object[] buffer = mutations.buffer;
				int iBuffer = mutations.size();
				out.writeInt(iBuffer);
				for (int i = 0; i< iBuffer; i++) {
						out.writeObject(buffer[i]);
				}
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				long txnId = in.readLong();
				long beginTs = in.readLong();
				boolean additive = in.readBoolean();

				int size = in.readInt();
				mutations = ObjectArrayList.newInstanceWithCapacity(size);
				for(int i=0;i<size;i++){
						mutations.add((KVPair)in.readObject());
				}
		}

		public byte[] toBytes() throws IOException {
				Output output = new Output(1024,-1);
				output.writeLong(txn.getTxnId());
				output.writeLong(txn.getBeginTimestamp());
				output.writeLong(txn.getParentTransaction().getTxnId());
				output.writeBoolean(txn.isAdditive());
				Object[] buffer = mutations.buffer;
				int size = mutations.size();
				for(int i=0;i< size;i++){
						KVPair pair = (KVPair)buffer[i];
						pair.toBytes(output);
				}
				output.flush();
				return output.toBytes();
		}

		public static BulkWrite fromBytes(byte[] bulkWriteBytes) throws IOException {
				Input input = new Input(bulkWriteBytes);

				long txnId = input.readLong();
				long beginTs = input.readLong();
				long parentTxnId = input.readLong();
				boolean additive = input.readBoolean();

        Txn parentTxn;
        if(parentTxnId>=0) parentTxn = new ActiveWriteTxn(parentTxnId,parentTxnId);
        else parentTxn = Txn.ROOT_TRANSACTION;

        Txn writeTxn = new ActiveWriteTxn(txnId,beginTs,parentTxn,additive);
				ObjectArrayList<KVPair> mutations = new ObjectArrayList<KVPair>();
				while(input.available()>0){
						mutations.add(KVPair.fromBytes(input));
				}
				return new BulkWrite(mutations,writeTxn,null);
		}
}
