package com.splicemachine.si.impl.readresolve;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.ThreadSafe;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Read-Resolver which resolves elements synchronously on the calling thread.
 *
 * This is unlikely to be efficient in a production environment, since it will impose
 * write latency during reads; however, it's simple implementation makes it useful for testing
 * and for simple applications which do not require low latency (e.g. pre-emptive read resolution).
 *
 * It can also be used as the basis for more sophisticated read resolution strategies.
 *
 * @author Scott Fines
 * Date: 7/2/14
 */
@ThreadSafe
public class SynchronousReadResolver {
		private static final Logger LOG = Logger.getLogger(SynchronousReadResolver.class);

		private SynchronousReadResolver(){}

		public static SynchronousReadResolver INSTANCE = new SynchronousReadResolver();

		public void resolveCommitted(HRegion region,ByteSlice rowKey, long txnId, long commitTimestamp) {
			if(region.isClosed()||region.isClosing())
					return; //do nothing if we are closing

				Put put = new Put(rowKey.getByteCopy());
				put.add(SIConstants.DEFAULT_FAMILY_BYTES,
								SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,txnId,
								Bytes.toBytes(commitTimestamp));
				put.setWriteToWAL(false);
				try{
						region.put(put,false);
				} catch (IOException e) {
						LOG.info("Exception encountered when attempting to resolve a row as rolled back",e);
				}
		}

		public void resolveRolledback(HRegion region,ByteSlice rowKey, long txnId) {
				if(region.isClosed()||region.isClosing())
						return; //do nothing if we are closing

				Delete delete = new Delete(rowKey.getByteCopy(),txnId);
				delete.deleteColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES,txnId); //delete all the columns for our family only
				delete.deleteColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,txnId); //delete all the columns for our family only
				delete.deleteColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES,txnId); //delete all the columns for our family only
				delete.setWriteToWAL(false); //avoid writing to the WAL for performance
				try{
						region.delete(delete,false);
				}catch(IOException ioe){
						LOG.info("Exception encountered when attempting to resolve a row as rolled back",ioe);
				}
		}

		public @ThreadSafe ReadResolver getResolver(final HRegion region){
				return new ReadResolver() {
						@Override
						public void resolveCommitted(ByteSlice rowKey, long txnId, long commitTimestamp) {
								SynchronousReadResolver.this.resolveCommitted(region, rowKey, txnId, commitTimestamp);
						}

						@Override
						public void resolveRolledback(ByteSlice rowKey, long txnId) {
							SynchronousReadResolver.this.resolveRolledback(region, rowKey, txnId);
						}
				};
		}
}
