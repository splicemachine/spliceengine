package com.splicemachine.derby.impl.job.operation;

import com.google.common.collect.Lists;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 5/24/13
 */
public abstract class BaseSuccessFilter<Data> extends FilterBase implements Writable {
	private static final SDataLib dataLib = SIFactoryDriver.siFactory.getDataLib();
    private static final Logger LOG = Logger.getLogger(BaseSuccessFilter.class);
    @SuppressWarnings("unused")
	private static final long serialVersionUID = 1l;
    protected List<byte[]> failedTasks;
    private int postfixOffset;

    private boolean filterRow;

    public BaseSuccessFilter() {
        super();
    }

    public BaseSuccessFilter(List<byte[]> failedTasks) {
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
	public ReturnCode internalFilter(Data keyValue) {
    	postfixOffset = dataLib.getDataRowOffset(keyValue)+dataLib.getDataRowlength(keyValue);
    	int failedTasksSize = failedTasks != null?failedTasks.size():0;
    	for (int i = 0; i< failedTasksSize; i++) {
    		byte[] failedTask = failedTasks.get(i);
            int postOffset = postfixOffset-failedTask.length;
            if(Bytes.compareTo(dataLib.getDataRowBuffer(keyValue),postOffset,failedTask.length,failedTask,0,failedTask.length)==0){
                SpliceLogUtils.trace(LOG,"Found a row from a failed task, skipping");
                //we have a task which failed
                return ReturnCode.SKIP;
            }    		
    	}
        return ReturnCode.INCLUDE;
	}


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
