package com.splicemachine.pipeline.config;

import java.util.concurrent.ExecutionException;


import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.splicemachine.access.api.NotServingPartitionException;
import com.splicemachine.access.api.WrongPartitionException;
import com.splicemachine.pipeline.api.*;
import com.splicemachine.pipeline.callbuffer.Rebuildable;
import com.splicemachine.pipeline.client.BulkWrite;
import com.splicemachine.pipeline.client.BulkWriteResult;
import com.splicemachine.pipeline.client.WriteResult;

public class UpdatingWriteConfiguration extends ForwardingWriteConfiguration{
	Rebuildable rebuildable;

	public UpdatingWriteConfiguration(WriteConfiguration delegate,
									  Rebuildable rebuildable) {
		super(delegate); 
		this.rebuildable=rebuildable;
	}

	@Override
	public WriteResponse globalError(Throwable t) throws ExecutionException {
		t = getExceptionFactory().processPipelineException(t);
		if(t instanceof NotServingPartitionException || t instanceof WrongPartitionException)
			rebuildable.rebuild();

		return super.globalError(t);
	}

	@Override
	public WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
			for(IntObjectCursor<WriteResult> cursor:result.getFailedRows()){
					switch (cursor.value.getCode()) {
							case NOT_SERVING_REGION:
							case WRONG_REGION:
								rebuildable.rebuild();
									break;
					}
			}
			return super.partialFailure(result,request);
	}

	@Override
	public WriteResponse processGlobalResult(BulkWriteResult bulkWriteResult)
			throws Throwable {
		if (bulkWriteResult.getGlobalResult().refreshCache())
			rebuildable.rebuild();
		return super.processGlobalResult(bulkWriteResult);
	}

	@Override
	public String toString() {
		return String.format("UpdatingWriteConfiguration{delegate=%s}",delegate);
	}
	
	
}

