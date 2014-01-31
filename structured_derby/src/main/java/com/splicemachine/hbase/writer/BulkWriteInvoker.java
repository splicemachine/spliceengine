package com.splicemachine.hbase.writer;

import org.apache.hadoop.hbase.client.HConnection;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 1/31/14
 */
public interface BulkWriteInvoker {

		BulkWriteResult invoke(BulkWrite write,boolean refreshCache) throws IOException;

		public static interface Factory{

				BulkWriteInvoker newInstance();
		}
}
