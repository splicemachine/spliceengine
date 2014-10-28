package com.splicemachine.pipeline.impl;

import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.derby.hbase.SpliceWriteControl;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.util.Bytes;
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
	private TxnView txn;
    public ObjectArrayList<KVPair> mutations;
    private byte[] regionKey;
    private long bufferSize = -1;
    private String encodedStringName;
    public SpliceWriteControl.Status status;
    public int initialSize;
    
    public BulkWrite() { }

    public BulkWrite(ObjectArrayList<KVPair> mutations, TxnView txn,byte[] regionKey,String encodedStringName) {
    	assert txn != null; 
        assert regionKey != null; 
        assert encodedStringName != null;
    	this.mutations = mutations;
        this.regionKey = regionKey;
		this.txn = txn;
		this.encodedStringName = encodedStringName;
    }

    public BulkWrite(TxnView txn, byte[] regionKey, String encodedStringName){
    	assert txn != null; 
        assert regionKey != null; 
        assert encodedStringName != null;
        this.txn = txn;
    	this.mutations = ObjectArrayList.newInstance();
        this.regionKey = regionKey;
        this.encodedStringName = encodedStringName;
    }

    public ObjectArrayList<KVPair> getMutations() {
        return mutations;
    }


	public TxnView getTxn(){ 
		return txn; 
	}

    public byte[] getRegionKey() {
        return regionKey;
    }
    
    public String getEncodedStringName() {
		return encodedStringName;
	}

	public void setEncodedStringName(String encodedStringName) {
		this.encodedStringName = encodedStringName;
	}


    public void addWrite(KVPair write){
        mutations.add(write);
    }

	@Override
    public String toString() {
        return "BulkWrite{" +
                "txn'" + txn + '\'' +
                ", encodedStringName='" + encodedStringName + '\'' +                
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
				out.writeObject(txn);				
				out.writeUTF(encodedStringName);
				out.writeInt(regionKey.length);
				out.write(regionKey);
				Object[] buffer = mutations.buffer;
				int iBuffer = mutations.size();
				out.writeInt(iBuffer);
				for (int i = 0; i< iBuffer; i++) {
						out.writeObject(buffer[i]);
				}
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				txn = (TxnView) in.readObject();
				encodedStringName = in.readUTF();
				regionKey = new byte[in.readInt()];
				in.read(regionKey);
				int size = in.readInt();
				mutations = ObjectArrayList.newInstanceWithCapacity(size);
				for(int i=0;i<size;i++){
						mutations.add((KVPair)in.readObject());
				}
		}

		@Override
		public boolean equals(Object obj) {
			BulkWrite write = (BulkWrite) obj;
			if (Bytes.compareTo(this.regionKey, write.regionKey) != 0 
					|| !this.encodedStringName.equals(write.encodedStringName)
					|| !this.txn.equals(write.txn)
					|| !this.mutations.equals(write.mutations))
				return false;
			return true;
		}	
		
	
}
