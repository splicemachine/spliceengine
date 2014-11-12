package com.splicemachine.derby.hbase;

import java.io.IOException;
import com.splicemachine.pipeline.impl.BulkWrites;
import com.splicemachine.pipeline.impl.BulkWritesResult;

public interface IndexEndpoint {
	public BulkWritesResult bulkWrite(BulkWrites bulkWrites) throws IOException;
	public SpliceBaseIndexEndpoint getBaseIndexEndpoint();
}
