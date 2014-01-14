package com.splicemachine.hbase.writer;

import com.carrotsearch.hppc.IntArrayList;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.encoding.Encoding;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.jruby.util.collections.IntHashMap;

import java.io.*;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class BulkWriteResult implements Externalizable {
    private IntArrayList notRunRows;
		private IntHashMap<WriteResult> failedRows;

    public BulkWriteResult() {
        notRunRows = new IntArrayList();
        failedRows = new IntHashMap<WriteResult>();
    }

		public BulkWriteResult(IntArrayList notRunRows,IntHashMap<WriteResult> failedRows){
				this.notRunRows = notRunRows;
				this.failedRows = failedRows;
		}

    public IntHashMap<WriteResult> getFailedRows() {
        return failedRows;
    }

    public IntArrayList getNotRunRows() {
        return notRunRows;
    }

    public void addResult(int pos, WriteResult result) {
        if(result.getCode()== WriteResult.Code.SUCCESS) return; //nothing to do
        switch (result.getCode()) {
            case FAILED:
            case WRITE_CONFLICT:
            case PRIMARY_KEY_VIOLATION:
            case UNIQUE_VIOLATION:
            case FOREIGN_KEY_VIOLATION:
            case CHECK_VIOLATION:
            case NOT_SERVING_REGION:
            case WRONG_REGION:
                failedRows.put(pos,result);
                break;
            case NOT_RUN:
                notRunRows.add(pos);
                break;
            default:
                //no-op
        }
    }

    @Override
    public String toString() {
        return "BulkWriteResult{" +
                "notRunRows=" + notRunRows.size() +
                ", failedRows=" + failedRows.size() +
                '}';
    }

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				out.writeInt(notRunRows.size());

				int size = notRunRows.size();
				int[] notRunBuffer = notRunRows.buffer;
				for(int i=0;i<size;i++){
						int row = notRunBuffer[i];
						out.writeInt(row);
				}

				out.writeInt(failedRows.size());
				for(Integer row:failedRows.keySet()){
						out.writeInt(row);
						WriteResult result = failedRows.get(row);
						out.writeBoolean(result!=null);
						if(result!=null)
								out.writeObject(result);
				}
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				int notRunSize = in.readInt();
				notRunRows = IntArrayList.newInstanceWithCapacity(notRunSize);
				for(int i=0;i<notRunSize;i++){
						notRunRows.add(in.readInt());
				}

				int failedSize = in.readInt();
				failedRows = new IntHashMap<WriteResult>(failedSize);
				for(int i=0;i<failedSize;i++){
						int rowNum = in.readInt();
						if(in.readBoolean()){
								failedRows.put(rowNum,(WriteResult)in.readObject());
						}else{
								failedRows.put(rowNum,new WriteResult(WriteResult.Code.FAILED, "Unknown exception"));
						}
				}
		}

		public byte[] toBytes() throws IOException {
//				ByteArrayOutputStream baos = new ByteArrayOutputStream();
//				CompressionCodec codec = SpliceUtils.getSnappyCodec();
//				baos.write(Encoding.encode(codec!=null));
//
//				OutputStream os = codec==null?baos:codec.createOutputStream(baos);
				Output out = new Output(1024,-1);
//				out.setOutputStream(os);
				int size = notRunRows.size();
				int[] notRunBuffer = notRunRows.buffer;
				out.writeInt(size);
				for(int i=0;i<size;i++){
						int row = notRunBuffer[i];
						out.writeInt(row);
				}
				out.writeInt(failedRows.size());
				for(int row:failedRows.keySet()){
						out.writeInt(row);
						WriteResult result = failedRows.get(row);
						result.write(out);
				}
				out.flush();
				return out.toBytes();
		}

		public static BulkWriteResult fromBytes(byte[] bytes) throws IOException {
//				boolean encoded = Encoding.decodeBoolean(bytes);
//				InputStream is = new ByteArrayInputStream(bytes,1,bytes.length);
//				is = encoded?SpliceUtils.getSnappyCodec().createInputStream(is):is;

				Input input = new Input(bytes);
				int notRunSize = input.readInt();
				IntArrayList notRunRows = IntArrayList.newInstanceWithCapacity(notRunSize);
				for(int i=0;i<notRunSize;i++){
						notRunRows.add(input.readInt());
				}

				int failedSize = input.readInt();
				IntHashMap<WriteResult> failedRows = new IntHashMap<WriteResult>(failedSize);
				for(int i=0;i<failedSize;i++){
						failedRows.put(input.readInt(), WriteResult.fromBytes(input));
				}
				return new BulkWriteResult(notRunRows,failedRows);
		}
}
