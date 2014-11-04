package com.splicemachine.pipeline.coprocessor;

import java.io.IOException;

import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.pipeline.impl.BulkWriteResult;
import com.splicemachine.pipeline.impl.BulkWrites;
import com.splicemachine.pipeline.impl.BulkWritesResult;

public interface LocalBatchProtocol {
	  public BulkWritesResult bulkWrite(BulkWrites bulkWrites) throws IOException;
	  public BulkWriteResult bulkWrite(BulkWrite bulkWrite) throws IOException;
	
}
