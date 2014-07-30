package com.splicemachine.derby.impl.job.operation;

import com.google.common.collect.Lists;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 5/24/13
 */
public class SuccessFilter extends FilterBase {
    private static final Logger LOG = Logger.getLogger(SuccessFilter.class);
    @SuppressWarnings("unused")
	private static final long serialVersionUID = 1l;
    private List<byte[]> failedTasks;
    private int postfixOffset;

    private boolean filterRow;

    public SuccessFilter() {
        super();
    }

    public SuccessFilter(List<byte[]> failedTasks) {
        this.failedTasks = failedTasks;
    }

    @Override
    public void reset() {
        filterRow = false;
    }
    
    
    /**
     * 
     * Used to filter row key.  Focuses on not forcing a reseek.
     * 
     */
    @Override
	public ReturnCode filterKeyValue(KeyValue keyValue) {
    	postfixOffset = keyValue.getRowOffset()+keyValue.getRowLength();
        for(byte[] failedTask:failedTasks){
            int postOffset = postfixOffset-failedTask.length;
            if(Bytes.compareTo(keyValue.getBuffer(),postOffset,failedTask.length,failedTask,0,failedTask.length)==0){
                SpliceLogUtils.trace(LOG,"Found a row from a failed task, skipping");
                //we have a task which failed
                return ReturnCode.SKIP;
            }
        }
        return ReturnCode.INCLUDE;
	}

    /* DO NOT USE: Forces a reseek.
	@Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) {
        int postfixOffset = offset+length;
        for(byte[] failedTask:failedTasks){
            int postOffset = postfixOffset-failedTask.length;
            if(Bytes.compareTo(buffer,postOffset,failedTask.length,failedTask,0,failedTask.length)==0){
                SpliceLogUtils.trace(LOG,"Found a row from a failed task, skipping");
                //we have a task which failed
                filterRow=true;
                break;
            }
        }

        return filterRow;
    }
	*/
    @Override
    public boolean filterRow() {
        return this.filterRow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(failedTasks.size());
        for(byte[] failedTask: failedTasks){
            out.writeInt(failedTask.length);
            out.write(failedTask);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        failedTasks = Lists.newArrayListWithExpectedSize(size);
        for(int i=0;i<size;i++){
            byte[] next = new byte[in.readInt()];
            in.readFully(next);
            failedTasks.add(next);
        }
    }

    public List<byte[]> getTaskList() {
        return failedTasks;
    }
}
