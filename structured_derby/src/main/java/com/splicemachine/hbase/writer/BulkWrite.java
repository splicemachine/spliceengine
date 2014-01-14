package com.splicemachine.hbase.writer;

import com.carrotsearch.hppc.ObjectArrayList;

import java.io.*;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.encoding.Encoding;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.xerial.snappy.Snappy;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class BulkWrite implements Externalizable {
    private static final long serialVersionUID = 1l;
    private static SnappyCodec snappy;
    private ObjectArrayList<KVPair> mutations;
    private String txnId;
    private byte[] regionKey;
    private long bufferSize = -1;

    static {
    	snappy = new SnappyCodec();
    }
    
    public BulkWrite() { }

    public BulkWrite(ObjectArrayList<KVPair> mutations, String txnId,byte[] regionKey) {
        this.mutations = mutations;
        this.txnId = txnId;
        this.regionKey = regionKey;
    }

    public BulkWrite(String txnId, byte[] regionKey){
        this.txnId = txnId;
        this.regionKey = regionKey;
        this.mutations = ObjectArrayList.newInstance();
    }

    public ObjectArrayList<KVPair> getMutations() {
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

		public byte[] toBytes() throws IOException {
//				ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//				CompressionCodec snappy = SpliceUtils.getSnappyCodec();
//				byteArrayOutputStream.write(Encoding.encode(snappy!=null));
//				OutputStream stream;
//				if(snappy!=null)
//						stream = snappy.createOutputStream(byteArrayOutputStream);
//				else
//					stream = byteArrayOutputStream;
				Output output = new Output(1024,-1);
//				output.setOutputStream(stream);
				output.writeString(txnId);

				List<KVPair> inserts = Lists.newArrayList();
				List<KVPair> updates = Lists.newArrayList();
				List<KVPair> deletes = Lists.newArrayList();
				Object[] buffer = mutations.buffer;
				int size = mutations.size();
				for(int i=0;i< size;i++){
						KVPair pair = (KVPair)buffer[i];
						switch (pair.getType()) {
								case INSERT:
										inserts.add(pair);
										break;
								case UPDATE:
										updates.add(pair);
										break;
								case DELETE:
										deletes.add(pair);
										break;
						}
				}
				if(inserts.size()>0){
						output.writeByte(KVPair.Type.INSERT.asByte());
						writeKvs(output, inserts);
				}
				if(updates.size()>0){
						output.writeByte(KVPair.Type.UPDATE.asByte());
						writeKvs(output,updates);
				}
				if(deletes.size()>0){
						output.writeByte(KVPair.Type.DELETE.asByte());
						writeKvs(output,deletes);
				}
				output.flush();
				return output.toBytes();
//				return byteArrayOutputStream.toByteArray();
		}

		private void writeKvs(Output output, List<KVPair> kvs) {
				output.writeInt(kvs.size());
				for(KVPair kvPair:kvs){
						byte[] key = kvPair.getRow();
						byte[] value = kvPair.getValue();
						output.writeInt(key.length);
						output.write(key);
						output.writeInt(value.length);
						output.write(value);
				}
		}

		public static BulkWrite fromBytes(byte[] bulkWriteBytes) throws IOException {
//				boolean compressed = Encoding.decodeBoolean(bulkWriteBytes);
//				InputStream stream = new ByteArrayInputStream(bulkWriteBytes,1,bulkWriteBytes.length);
//				if(compressed){
//					stream = SpliceUtils.getSnappyCodec().createInputStream(stream);
//				}
				Input input = new Input(bulkWriteBytes);

				String txnId = input.readString();
				ObjectArrayList<KVPair> mutations = new ObjectArrayList<KVPair>();
				while(input.available()>0){
						byte typeByte = input.readByte();
						int length = input.readInt();
						readKvs(input, length, mutations, KVPair.Type.decode(typeByte));
				}
				return new BulkWrite(mutations,txnId,null);
		}

		private static void readKvs(Input input, int insertLength, ObjectArrayList<KVPair> mutations, KVPair.Type type) {
				for(int i=0;i<insertLength;i++){
						byte[] key = new byte[input.readInt()];
						input.read(key);
						byte[] row = new byte[input.readInt()];
						input.read(row);
						mutations.add(new KVPair(key,row, type));
				}
		}
}
