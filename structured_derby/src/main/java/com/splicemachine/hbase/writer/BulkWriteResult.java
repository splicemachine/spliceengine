package com.splicemachine.hbase.writer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class BulkWriteResult implements Externalizable {
    private List<Integer> notRunRows;
    private Map<Integer,WriteResult> failedRows;

    public BulkWriteResult() {
        notRunRows = Lists.newArrayListWithExpectedSize(0);
        failedRows = Maps.newHashMapWithExpectedSize(0);
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(notRunRows.size());
        for(Integer row:notRunRows){
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
        notRunRows = Lists.newArrayListWithCapacity(notRunSize);
        for(int i=0;i<notRunSize;i++){
            notRunRows.add(in.readInt());
        }

        int failedSize = in.readInt();
        failedRows = Maps.newHashMapWithExpectedSize(failedSize);
        for(int i=0;i<failedSize;i++){
            int rowNum = in.readInt();
            if(in.readBoolean()){
                failedRows.put(rowNum,(WriteResult)in.readObject());
            }else{
                failedRows.put(rowNum,new WriteResult(WriteResult.Code.FAILED, "Unknown exception"));
            }
        }
    }

    public Map<Integer, WriteResult> getFailedRows() {
        return failedRows;
    }

    public List<Integer> getNotRunRows() {
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

		public byte[] toBytes() {
				return new byte[0];  //To change body of created methods use File | Settings | File Templates.
		}

		public static BulkWriteResult fromBytes(byte[] bytes) {
				return null;
		}
}
