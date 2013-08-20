package com.splicemachine.hbase.writer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public class MutationResponse implements Externalizable{
    private static final long serialVersionUID = 2l;
    private static final Logger LOG = Logger.getLogger(MutationResponse.class);

    private List<Integer> notRunRows;
    private Map<Integer,MutationResult> failedRows;

    public MutationResponse() {
        notRunRows = Lists.newArrayListWithExpectedSize(0);
        failedRows = Maps.newHashMapWithExpectedSize(0);
    }

    public List<Integer> getNotRunRows() {
        return notRunRows;
    }

    public void setNotRunRows(List<Integer> notRunRows) {
        this.notRunRows = notRunRows;
    }

    public Map<Integer, MutationResult> getFailedRows() {
        return failedRows;
    }

    public void setFailedRows(Map<Integer, MutationResult> failedRows) {
        this.failedRows = failedRows;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(notRunRows.size()) ;
        for(Integer row:notRunRows){
            out.writeInt(row);
        }

        out.writeInt(failedRows.size());
        for(Integer row:failedRows.keySet()){
            out.writeInt(row);
            MutationResult result = failedRows.get(row);
            out.writeBoolean(result != null);
            if(result != null){
                out.writeObject(result);
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int notRunSize = in.readInt();
        notRunRows = Lists.newArrayListWithExpectedSize(notRunSize);
        for(int i=0;i<notRunSize;i++){
            notRunRows.add(in.readInt());
        }

        int failedSize = in.readInt();
        failedRows = Maps.newHashMapWithExpectedSize(failedSize);
        for(int i=0;i<failedSize;i++){
            int rowNum = in.readInt();
            if(in.readBoolean()){
                failedRows.put(rowNum, (MutationResult) in.readObject());
            }else{
                failedRows.put(rowNum, new MutationResult(MutationResult.Code.FAILED, "UnknownException"));
            }
        }
    }

    public void addResult(int pos, MutationResult result) {
        if(result.getCode()== MutationResult.Code.SUCCESS) return; //nothing to do

        switch (result.getCode()) {
            case PRIMARY_KEY_VIOLATION:
            case UNIQUE_VIOLATION:
            case FOREIGN_KEY_VIOLATION:
            case CHECK_VIOLATION:
            case WRITE_CONFLICT:
                failedRows.put(pos,result);
                break;
            case FAILED:
                failedRows.put(pos,result);
                break;
            case NOT_RUN:
                notRunRows.add(pos);
                break;
        }
    }

    @Override
    public String toString() {
        return "MutationResponse{notRun="+notRunRows.size()+",failed="+failedRows.size()+"}";
    }
}
