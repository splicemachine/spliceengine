package com.splicemachine.pipeline.impl;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.procedures.IntObjectProcedure;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.pipeline.api.WriteContext;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 * Created on: 8/8/13 
 */
public class BulkWriteResult implements Externalizable {
	private WriteResult globalStatus;
    private IntArrayList notRunRows;
	private IntObjectOpenHashMap<WriteResult> failedRows;
	private WriteContext writeContext;

    private transient int position;

    public BulkWriteResult() {
        notRunRows = new IntArrayList();
        failedRows = new IntObjectOpenHashMap<WriteResult>();
    }
	public BulkWriteResult(WriteContext writeContext, WriteResult globalStatus) {
        notRunRows = new IntArrayList();
        failedRows = new IntObjectOpenHashMap<WriteResult>();
        this.writeContext = writeContext;
        this.globalStatus = globalStatus;
    }

	public BulkWriteResult(WriteResult globalStatus, IntArrayList notRunRows,IntObjectOpenHashMap<WriteResult> failedRows){
			this.notRunRows = notRunRows;
			this.failedRows = failedRows;
			this.globalStatus = globalStatus;
		}

		public BulkWriteResult(WriteResult globalStatus){
			this();
			this.globalStatus = globalStatus;
		}

    public IntObjectOpenHashMap<WriteResult> getFailedRows() {
        return failedRows;
    }

    public IntArrayList getNotRunRows() {
        return notRunRows;
    }

    public void addResult(int pos, WriteResult result) {
        switch (result.getCode()) {
			case SUCCESS:
				return; //return nothing for success
            case NOT_RUN:
                notRunRows.add(pos);
                break;
            default:
				failedRows.put(pos,result);
        }
    }

    @Override
    public String toString() {
        return "BulkWriteResult{" +
        		"globalStatus=" + (globalStatus==null?"null":globalStatus.toString()) + 
                "notRunRows=" + (notRunRows==null?"null":notRunRows.size()) +
                ", failedRows=" + (failedRows==null?"null":failedRows.size()) +
                '}';
    }

		@Override
		public void writeExternal(final ObjectOutput out) throws IOException {
				out.writeObject(globalStatus);
				out.writeInt(notRunRows.size());
				int size = notRunRows.size();
				int[] notRunBuffer = notRunRows.buffer;
				for(int i=0;i<size;i++){
						int row = notRunBuffer[i];
						out.writeInt(row);
				}
				out.writeInt(failedRows.size());
				failedRows.forEach(new IntObjectProcedure<WriteResult>() {
						@Override
						public void apply(int key, WriteResult value) {
								try {
										out.writeInt(key);
										out.writeBoolean(value!=null);
										if(value!=null)
												out.writeObject(value);
								} catch (IOException e) {
										throw new RuntimeException(e); //shouldn't happen, because we only go to byte[]
								}
						}
				});
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				globalStatus = (WriteResult) in.readObject();
				int notRunSize = in.readInt();
				notRunRows = IntArrayList.newInstanceWithCapacity(notRunSize);
				for(int i=0;i<notRunSize;i++){
						notRunRows.add(in.readInt());
				}

				int failedSize = in.readInt();
				failedRows = new IntObjectOpenHashMap<WriteResult>(failedSize);
				for(int i=0;i<failedSize;i++){
						int rowNum = in.readInt();
						if(in.readBoolean()){
								failedRows.put(rowNum,(WriteResult)in.readObject());
						}
				}
		}

		public WriteResult getGlobalResult() {
				return globalStatus;
		}
		
		public void setGlobalStatus(WriteResult globalStatus) {
			this.globalStatus = globalStatus;
		}

		public void setFailedRows(IntObjectOpenHashMap<WriteResult> failedRows) {
				this.failedRows = failedRows;
		}
		public WriteContext getWriteContext() {
			return writeContext;
		}
		public void setWriteContext(WriteContext writeContext) {
			this.writeContext = writeContext;
		}
		
		public boolean hasWriteContext() {
			return writeContext != null;
		}

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

		public byte[] encode() {
				int totalLength = getEncodedLength();

				/*
				 * We now have the length of the final byte array, so create it, then make a pass through
				 * the data to fill the result
				 */
				byte[] finalBytes = new byte[totalLength];
				int offset = 0;
				offset += globalStatus.encodeInto(finalBytes,offset);
				offset = encodeNotRun(finalBytes, offset);


				//encode the failed rows
				for(IntObjectCursor<WriteResult> failedEntry:failedRows){
						int rowNum = failedEntry.key;
						WriteResult result = failedEntry.value;
						offset+=Encoding.encode(rowNum,finalBytes,offset,false);
						offset+=result.encodeInto(finalBytes,offset);
				}

				return finalBytes;
		}

		private int encodeNotRun(byte[] finalBytes, int offset) {
				//encode the notRunRows
				int notRunSize = notRunRows.size();
				int[] notRunBuffer = notRunRows.buffer;
				offset += Encoding.encode(notRunSize, finalBytes, offset, false);
				for(int i=0;i<notRunSize;i++){
						offset+=Encoding.encode(notRunBuffer[i],finalBytes,offset,false);
				}
				return offset;
		}


		public static BulkWriteResult decode(byte[] data ,int offset, int length,long[] lengthHolder) throws IOException{
				int len =0;
				WriteResult globalResult = WriteResult.decode(data,offset,lengthHolder);
				offset+=lengthHolder[0];
				len+=lengthHolder[0];
				Encoding.decodeLongWithLength(data,offset,false,lengthHolder);
				offset+=lengthHolder[1];
				len+=lengthHolder[1];
				int notRunSize = (int) lengthHolder[0];
				IntArrayList notRunRows =IntArrayList.newInstanceWithCapacity(notRunSize);
				for(int i=0;i<notRunSize;i++){
						Encoding.decodeLongWithLength(data,offset,false,lengthHolder);
						notRunRows.add((int)lengthHolder[0]);
						offset+=lengthHolder[1];
						len+=lengthHolder[1];
				}

				if(len>=length)
						return new BulkWriteResult(globalResult,notRunRows,IntObjectOpenHashMap.<WriteResult>newInstance(0,0.9f));

				IntObjectOpenHashMap<WriteResult> failedMap = IntObjectOpenHashMap.newInstance();
				while(len<length){
						Encoding.decodeLongWithLength(data,offset,false,lengthHolder);
						offset+=lengthHolder[1];
						len+=lengthHolder[1];
						int key =(int)lengthHolder[0];
						WriteResult result = WriteResult.decode(data,offset,lengthHolder);
						offset+=lengthHolder[0];
						len+=lengthHolder[0];
						failedMap.put(key,result);
				}
				return new BulkWriteResult(globalResult,notRunRows,failedMap);
		}

		private int getEncodedLength() {
				int totalLength = globalStatus.encodedLength();
				//get the number of bytes occupied by the notRunRows buffer
				int notRunSize = notRunRows.size();
				int[] notRunBuffer = notRunRows.buffer;
				totalLength+= Encoding.encodedLength(notRunSize);
				for(int i=0;i<notRunRows.size();i++){
						totalLength+= Encoding.encodedLength(notRunBuffer[i]);
				}

				//get the length of the failed bytes
				for(IntObjectCursor<WriteResult> failedEntry:failedRows){
						int rowNum = failedEntry.key;
						WriteResult writeResult = failedEntry.value;
						totalLength+=Encoding.encodedLength(rowNum)+writeResult.encodedLength();
				}
				return totalLength;
		}
}