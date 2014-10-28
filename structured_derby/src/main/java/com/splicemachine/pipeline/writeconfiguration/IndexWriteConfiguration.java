package com.splicemachine.pipeline.writeconfiguration;

import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.pipeline.impl.BulkWriteResult;
import com.splicemachine.pipeline.impl.WriteResult;

public class IndexWriteConfiguration extends BaseWriteConfiguration {
    private static final Logger LOG = Logger.getLogger(IndexWriteConfiguration.class);
	WriteContext ctx;
	ObjectObjectOpenHashMap<KVPair,KVPair> indexToMainMutationMap;
	public IndexWriteConfiguration(WriteContext ctx, ObjectObjectOpenHashMap<KVPair,KVPair> indexToMainMutationMap) {
		this.ctx = ctx;
		this.indexToMainMutationMap = indexToMainMutationMap;		
	}

	@Override
    public int getMaximumRetries() {
        return SpliceConstants.numRetries;
    }

    @Override
    public WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
        IntObjectOpenHashMap<WriteResult> failedRows = result.getFailedRows();
        boolean canRetry = true;
        boolean regionTooBusy = false;
        for(IntObjectCursor<WriteResult> cursor:failedRows){
            if(!cursor.value.canRetry()){
                canRetry=false;
                break;
            }if(cursor.value.getCode()== Code.REGION_TOO_BUSY)
                regionTooBusy = true;
        }

        if(regionTooBusy){
            try{
                Thread.sleep(2*getPause());
            } catch (InterruptedException e) {
                LOG.info("Interrupted while waiting due to a RegionTooBusyException",e);
            }
            return WriteResponse.RETRY;
        }
        if(canRetry) return WriteResponse.RETRY;
        else{
            ObjectArrayList<KVPair> indexMutations = request.getMutations();
            for(IntObjectCursor<WriteResult> cursor:failedRows){
										int row = cursor.key;
                KVPair kvPair = indexMutations.get(row);
										KVPair put = indexToMainMutationMap.get(kvPair);
										WriteResult mutationResult = cursor.value;
										ctx.failed(put, mutationResult);
            }
            return WriteResponse.IGNORE;
        }
    }

    @Override public long getPause() {
        return SpliceConstants.pause;
    }
	@Override public void writeComplete(long timeTakenMs, long numRecordsWritten) { 
		
	}

	//TODO -sf- return a metric factory which can contribute to utilization metrics
	@Override public MetricFactory getMetricFactory() { 
		return Metrics.noOpMetricFactory(); 
	}
	
	@Override
	public String toString() {
		return String.format("IndexWriteConfiguration{}");
	}
	
}

