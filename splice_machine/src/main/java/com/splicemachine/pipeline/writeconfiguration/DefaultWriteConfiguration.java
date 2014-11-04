package com.splicemachine.pipeline.writeconfiguration;

import java.util.concurrent.ExecutionException;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.pipeline.impl.BulkWriteResult;
import com.splicemachine.pipeline.impl.Monitor;
import com.splicemachine.pipeline.impl.WriteResult;

public class DefaultWriteConfiguration extends BaseWriteConfiguration {
    private Monitor monitor;
    	public DefaultWriteConfiguration(Monitor monitor) {
    		this.monitor = monitor;
    	}
        @Override public int getMaximumRetries() { 
        	return monitor.getMaximumRetries(); 
        }
        @Override public long getPause() { 
        	return monitor.getPauseTime();
        }
		@Override public void writeComplete(long timeTakenMs, long numRecordsWritten) { 
			
		} //no-op
		
		@Override public MetricFactory getMetricFactory() { 
			return Metrics.noOpMetricFactory(); 
		}

        @Override
        public WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
            IntObjectOpenHashMap<WriteResult> failedRows = result.getFailedRows();
            for(IntObjectCursor<WriteResult> cursor:failedRows){
                if(!cursor.value.canRetry())
                    return WriteResponse.THROW_ERROR;
            }
            return WriteResponse.RETRY;
        }
        
    	@Override
    	public String toString() {
    		return "DefaultWriteConfiguration{}";
    	}


}
