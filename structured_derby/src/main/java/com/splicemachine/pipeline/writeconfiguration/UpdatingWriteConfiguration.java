package com.splicemachine.pipeline.writeconfiguration;

import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.splicemachine.pipeline.api.CanRebuild;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.pipeline.impl.BulkWriteResult;
import com.splicemachine.pipeline.impl.WriteResult;

public class UpdatingWriteConfiguration extends ForwardingWriteConfiguration{
	CanRebuild canRebuild;
	public UpdatingWriteConfiguration(WriteConfiguration delegate, CanRebuild canRebuild) { 
		super(delegate); 
		this.canRebuild = canRebuild;
	}

	@Override
	public WriteResponse globalError(Throwable t) throws ExecutionException {
			if(t instanceof NotServingRegionException || t instanceof WrongRegionException){
					canRebuild.rebuildBuffer();
			}
			return super.globalError(t);
	}

	@Override
	public WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
			for(IntObjectCursor<WriteResult> cursor:result.getFailedRows()){
					switch (cursor.value.getCode()) {
							case NOT_SERVING_REGION:
							case WRONG_REGION:
								canRebuild.rebuildBuffer();
									break;
					}
			}
			return super.partialFailure(result,request);
	}

	@Override
	public WriteResponse processGlobalResult(BulkWriteResult bulkWriteResult)
			throws Throwable {
		if (bulkWriteResult.getGlobalResult().refreshCache())
			canRebuild.rebuildBuffer();
		return super.processGlobalResult(bulkWriteResult);
	}

	@Override
	public String toString() {
		return String.format("UpdatingWriteConfiguration{delegate=%s}",delegate);
	}
	
	
}

