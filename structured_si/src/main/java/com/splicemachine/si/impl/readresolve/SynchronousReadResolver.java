package com.splicemachine.si.impl.readresolve;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;
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
        put.setAttribute(SIConstants.SI_EXEMPT,SIConstants.TRUE_BYTES);
        put.setAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
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
        delete.setAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
				try{
						region.delete(delete,false);
				}catch(IOException ioe){
						LOG.info("Exception encountered when attempting to resolve a row as rolled back",ioe);
				}
		}

		public @ThreadSafe ReadResolver getResolver(final HRegion region,final TxnSupplier txnSupplier){
				return new ReadResolver() {

            @Override
            public void resolve(ByteSlice rowKey, long txnId) {
                SynchronousReadResolver.INSTANCE.resolve(region,rowKey,txnId,txnSupplier);
            }
        };
		}

    public void resolve(HRegion region, ByteSlice rowKey, long txnId, TxnSupplier supplier) {
        try {
            Txn transaction = supplier.getTransaction(txnId);
            if(transaction.getEffectiveState()== Txn.State.ROLLEDBACK){
                SynchronousReadResolver.INSTANCE.resolveRolledback(region, rowKey, txnId);
            }else{
                Txn t = transaction;
                while(t.getState()== Txn.State.COMMITTED){
                    t = t.getParentTransaction();
                }
                if(t==Txn.ROOT_TRANSACTION)
                    SynchronousReadResolver.INSTANCE.resolveCommitted(region,rowKey,txnId,transaction.getEffectiveCommitTimestamp());
            }
        } catch (IOException e) {
            LOG.info("Unable to fetch transaction for id "+ txnId+", will not resolve",e);
        }
    }
}
