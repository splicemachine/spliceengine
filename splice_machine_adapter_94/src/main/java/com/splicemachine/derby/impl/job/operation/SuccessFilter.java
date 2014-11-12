package com.splicemachine.derby.impl.job.operation;

import org.apache.hadoop.hbase.KeyValue;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 5/24/13
 */
public class SuccessFilter extends BaseSuccessFilter<KeyValue> {
    public SuccessFilter() {
        super();
    }

    public SuccessFilter(List<byte[]> failedTasks) {
    	super(failedTasks);
    }

    /**
     * 
     * Used to filter row key.  Focuses on not forcing a reseek.
     * 
     */
    @Override
	public ReturnCode filterKeyValue(KeyValue keyValue) {
    	return internalFilter(keyValue);
	}
}
