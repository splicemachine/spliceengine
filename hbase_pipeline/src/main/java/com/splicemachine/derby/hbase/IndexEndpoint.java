package com.splicemachine.derby.hbase;

import java.io.IOException;
import com.splicemachine.pipeline.client.BulkWrites;
import com.splicemachine.pipeline.client.BulkWritesResult;

public interface IndexEndpoint {

    /**
     * Perform the actual bulk writes.
     * The logic is as follows: Determine whether the writes are independent or dependent.  Get a "permit" for the writes.
     * And then perform the writes through the region's write pipeline.
     *
     * @param bulkWrites the bulks writes to perform on the region
     * @return the results of the bulk write operation
     * @throws IOException
     */
	BulkWritesResult bulkWrite(BulkWrites bulkWrites) throws IOException;

	SpliceIndexEndpoint getBaseIndexEndpoint();
}
