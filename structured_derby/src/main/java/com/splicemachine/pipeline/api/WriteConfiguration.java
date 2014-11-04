package com.splicemachine.pipeline.api;

import java.util.concurrent.ExecutionException;
import com.splicemachine.metrics.MetricFactory;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.pipeline.impl.BulkWriteResult;
/**
 * A Handler..
 * 
 *
 */
public interface WriteConfiguration {

    int getMaximumRetries();

    WriteResponse processGlobalResult(BulkWriteResult bulkWriteResult) throws Throwable;
    
    WriteResponse globalError(Throwable t) throws ExecutionException;

    WriteResponse partialFailure(BulkWriteResult result,BulkWrite request) throws ExecutionException;

    long getPause();

    void writeComplete(long timeTakenMs,long numRecordsWritten);

	MetricFactory getMetricFactory();
	
	void registerContext(WriteContext context,ObjectObjectOpenHashMap<KVPair,KVPair> indexToMainMutationMap);
	
}
