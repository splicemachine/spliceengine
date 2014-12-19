package com.splicemachine.pipeline.impl;

import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.collect.Lists;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.TxnView;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 *
 * Extension of BulkWrites to wrap for a region server.
 *
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class BulkWrites implements Externalizable {
		private static final long serialVersionUID = 1l;
		public ObjectArrayList<BulkWrite> bulkWrites;
		private TxnView txn;

		/*Used to indicate which region server receives the BulkWrite*/
		private transient byte[] regionKey;

		public BulkWrites() {
				bulkWrites = new ObjectArrayList<>();
		}

		public BulkWrites(TxnView txn){
				this.txn = txn;
		}

		public BulkWrites(ObjectArrayList<BulkWrite> bulkWrites) {
				this.bulkWrites = bulkWrites;
		}

		public void setRegionKey(byte[] regionKey){
				this.regionKey = regionKey;
		}

		public byte[] getRegionKey() {
				return regionKey;
		}

		public TxnView getTxn() {
				return txn;
		}

		public void setTxn(TxnView txn) {
				this.txn = txn;
		}

		public ObjectArrayList<BulkWrite> getBulkWrites() {
				return bulkWrites;
		}


		public void addBulkWrite(BulkWrite bulkWrite){
				bulkWrites.add(bulkWrite);
		}

		@Override
		public String toString() {
				StringBuffer sb = new StringBuffer();
				sb.append("BulkWrites{");
				Object[] buffer = bulkWrites.buffer;
				int iBuffer = bulkWrites.size();
				for (int i = 0; i< iBuffer; i++) {
						sb.append(buffer[i]);
						if (i!= iBuffer-1)
								sb.append(",");
				}
				sb.append("}");
				return sb.toString();
		}

		public long getKVPairSize() {
				long size = 0;
				Object[] buffer = bulkWrites.buffer;
				int iBuffer = bulkWrites.size();
				for (int i = 0; i< iBuffer; i++) {
						BulkWrite bulkWrite = (BulkWrite) buffer[i];
						size+=bulkWrite.getSize();
				}
				return size;
		}

		public long getBufferHeapSize() {
				long heap = 0l;
				Object[] buffer = bulkWrites.buffer;
				int iBuffer = bulkWrites.size();
				for (int i = 0; i< iBuffer; i++) {
						BulkWrite bulkWrite = (BulkWrite) buffer[i];
						heap+=bulkWrite.getBufferSize();
				}
				return heap;
		}

		public ObjectArrayList<KVPair> getAllCombinedKeyValuePairs() {
				ObjectArrayList<KVPair> pairs = new ObjectArrayList<KVPair>();
				Object[] buffer = bulkWrites.buffer;
				int iBuffer = bulkWrites.size();
				for (int i = 0; i< iBuffer; i++) {
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
				for(int i=0;i<size;i++){
						BulkWrite bw = (BulkWrite)buffer[i];
						numEntries+=bw.getSize();
				}
				return numEntries;
		}

		/**
		 * @return the number of regions in this write
		 */
		public int numRegions(){
				return bulkWrites.size();
		}


		public byte[] encode() throws IOException {
				byte[] txnBytes = TransactionOperations.getOperationFactory().encodeWriteTxn(txn);
				List<byte[]> encodedBulkWrites = Lists.newArrayListWithCapacity(bulkWrites.size());
				Object[] buffer = bulkWrites.buffer;
				int encodedSize = 0;
				for(int i=0;i<bulkWrites.size();i++){
						byte[] encode = ((BulkWrite) buffer[i]).encode();
						encodedBulkWrites.add(encode);
						encodedSize+=encode.length+4;
				}
				byte[] finalBytes = new byte[encodedSize+txnBytes.length+4];
				int offset = 0;
				BytesUtil.intToBytes(txnBytes.length,finalBytes,offset);
				offset+=4;
				System.arraycopy(txnBytes,0,finalBytes,offset,txnBytes.length);
				offset+=txnBytes.length;
				for(byte[] b:encodedBulkWrites){
						BytesUtil.intToBytes(b.length,finalBytes,offset);
						offset+=4;
						System.arraycopy(b,0,finalBytes,offset,b.length);
						offset+=b.length;
				}
				return finalBytes;
		}

		public static BulkWrites decode(byte[] data) throws IOException{
				int offset=0;
				byte[] txnBytes = new byte[BytesUtil.bytesToInt(data,offset)];
				offset+=4;
				TxnView txn = TransactionOperations.getOperationFactory().decodeTxn(data,offset,txnBytes.length);
				offset+=txnBytes.length;
				long[] lengthHolder = new long[2];
				//TODO -sf- probably wasting memory here
				ObjectArrayList<BulkWrite> bws  = ObjectArrayList.newInstance();
				while(offset<data.length){
						int bwLength = BytesUtil.bytesToInt(data,offset);
						offset+=4;
						BulkWrite bw = BulkWrite.decode(data,offset,bwLength,lengthHolder);
						offset+=bwLength;
						bws.add(bw);
				}
				BulkWrites allWrites = new BulkWrites(bws);
				allWrites.setTxn(txn);
				return allWrites;
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				Object[] buffer = bulkWrites.buffer;
				int iBuffer = bulkWrites.size();
				out.writeInt(iBuffer);
				for (int i = 0; i< iBuffer; i++) {
						out.writeObject(buffer[i]);
				}
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				int size = in.readInt();
				bulkWrites = ObjectArrayList.newInstanceWithCapacity(size);
				for(int i=0;i<size;i++){
						bulkWrites.add((BulkWrite)in.readObject());
				}
		}

		@Override
		public boolean equals(Object obj) {
				return this.bulkWrites.equals(((BulkWrites)obj).bulkWrites);
		}

		public void close () {
				int isize = bulkWrites.size();
				Object[] buffer = bulkWrites.buffer;
				for (int i=0; i<isize;i++) {
						BulkWrite bulkWrite = (BulkWrite) buffer[i];
						bulkWrite.mutations = null;
						bulkWrite = null;
				}
				bulkWrites = null;
		}

		public int smallestBulkWriteSize() {
				if(bulkWrites.size()<=0) return 0;
				int min = Integer.MAX_VALUE;
				Object[] buffer = bulkWrites.buffer;
				for(int i=0;i<bulkWrites.size();i++){
						BulkWrite bulkWrite = (BulkWrite)buffer[i];
						if(bulkWrite.getSize()<min)
								min = bulkWrite.getSize();
				}
				return min;
		}

}
