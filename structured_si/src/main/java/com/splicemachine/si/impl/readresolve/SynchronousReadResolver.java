package com.splicemachine.si.impl.readresolve;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.si.impl.rollforward.RollForwardStatus;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.annotations.ThreadSafe;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Read-Resolver which resolves elements synchronously on the calling thread.
 *

 * @author Scott Fines
 * Date: 7/2/14
 */
@ThreadSafe
public class SynchronousReadResolver {
		private static final Logger LOG = Logger.getLogger(SynchronousReadResolver.class);

    //don't instantiate me, I'm a singleton!
		private SynchronousReadResolver(){}

		public static SynchronousReadResolver INSTANCE = new SynchronousReadResolver();

    /**
     * @param region the region for the relevant read resolver
     * @param txnSupplier a Transaction Supplier for fetching transaction information
     * @return a ReadResolver which uses Synchronous Read Resolution under the hood.
     */
    public static @ThreadSafe ReadResolver getResolver(final HRegion region,final TxnSupplier txnSupplier,final RollForwardStatus status){
       return getResolver(region,txnSupplier,status,false);
    }

    public static @ThreadSafe ReadResolver getResolver(final HRegion region,final TxnSupplier txnSupplier,final RollForwardStatus status, final boolean failOnError){
        return new ReadResolver() {
            @Override
            public void resolve(ByteSlice rowKey, long txnId) {
                SynchronousReadResolver.INSTANCE.resolve(region,rowKey,txnId,txnSupplier,status,failOnError);
            }
        };
    }

    boolean resolve(HRegion region, ByteSlice rowKey, long txnId, TxnSupplier supplier,RollForwardStatus status,boolean failOnError) {
        try {
            TxnView transaction = supplier.getTransaction(txnId);
            boolean resolved = false;
            if(transaction.getEffectiveState()== Txn.State.ROLLEDBACK){
                SynchronousReadResolver.INSTANCE.resolveRolledback(region, rowKey, txnId,failOnError);
                resolved = true;
            }else{
                TxnView t = transaction;
                while(t.getState()== Txn.State.COMMITTED){
                    t = t.getParentTxnView();
                }
                if(t==Txn.ROOT_TRANSACTION){
                    SynchronousReadResolver.INSTANCE.resolveCommitted(region,rowKey,txnId,transaction.getEffectiveCommitTimestamp(),failOnError);
                    resolved = true;
                }
            }
            status.rowResolved();
            return resolved;
        } catch (IOException e) {
            LOG.info("Unable to fetch transaction for id "+ txnId+", will not resolve",e);
            if(failOnError)
                throw new RuntimeException(e);
            return false;
        }
    }

    /******************************************************************************************************************/
    /*private helper methods */

    private void resolveCommitted(HRegion region,ByteSlice rowKey, long txnId, long commitTimestamp,boolean failOnError) {
        /*
         * Resolve the row as committed directly.
         *
         * This does a Put to the row, bypassing SI and the WAL, so it should be pretty low impact
         */
        if(region.isClosed()||region.isClosing())
            return; //do nothing if we are closing

        Put put = new Put(rowKey.getByteCopy());
        put.add(SIConstants.DEFAULT_FAMILY_BYTES,
                SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES, txnId,
                Bytes.toBytes(commitTimestamp));
        put.setAttribute(SIConstants.SI_EXEMPT,SIConstants.TRUE_BYTES);
        put.setAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME, SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
        put.setWriteToWAL(false);
        try{
            region.put(put,false);
        } catch (IOException e) {
            LOG.info("Exception encountered when attempting to resolve a row as committed",e);
            if(failOnError)
                throw new RuntimeException(e);
        }
    }

    private void resolveRolledback(HRegion region,ByteSlice rowKey, long txnId,boolean failOnError) {
        /*
         * Resolve the row as rolled back directly.
         *
         * This does a Delete to the row, bypassing SI and the WAL, so it should be pretty low impact
         */
        if(region.isClosed()||region.isClosing())
            return; //do nothing if we are closing

        Delete delete = new Delete(rowKey.getByteCopy(),txnId)
        .deleteColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES,txnId) //delete all the columns for our family only
        .deleteColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,txnId) //delete all the columns for our family only
        .deleteColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES,txnId); //delete all the columns for our family only

        delete.setWriteToWAL(false); //avoid writing to the WAL for performance
        delete.setAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
        try{
            region.delete(delete,false);
        }catch(IOException ioe){
            LOG.info("Exception encountered when attempting to resolve a row as rolled back",ioe);
            if(failOnError)
                throw new RuntimeException(ioe);
        }
    }
}
