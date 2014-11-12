package com.splicemachine.pipeline.api;

import java.io.IOException;

import com.splicemachine.pipeline.impl.BulkWrites;
import com.splicemachine.pipeline.impl.BulkWritesResult;

/**
 * @author Scott Fines
 * Date: 1/31/14
 */
public interface BulkWritesInvoker {
		/**
		 * 
		 * Interface for invoking BulkWrites.  The refresh cache will trigger a refresh on the 
		 * underlying connection.
		 * 
		 * @param write
		 * @param refreshCache
		 * @return
		 * @throws IOException
		 */
		BulkWritesResult invoke(BulkWrites write,boolean refreshCache) throws IOException;

		public static interface Factory{
				BulkWritesInvoker newInstance();
		}
}
