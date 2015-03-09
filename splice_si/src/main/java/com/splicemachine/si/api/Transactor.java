package com.splicemachine.si.api;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.data.api.IHTable;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.SRowLock;
import com.splicemachine.si.impl.DataStore;

import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;
import java.util.Collection;

/**
 * The primary interface to the transaction module. This interface has the most burdensome generic signature so it is
 * only exposed in places where it is needed.
 */
public interface Transactor<Table, Mutation extends OperationWithAttributes,Put extends Mutation>{

		/**
     * Execute the put operation (with SI treatment) on the table. Send roll-forward notifications to the rollForwardQueue.
     */
    boolean processPut(Table table, RollForward rollForwardQueue, Put put) throws IOException;

    OperationStatus[] processPutBatch(Table table, RollForward rollForwardQueue, Put[] mutations)
            throws IOException;

		OperationStatus[] processKvBatch(Table table,
																		 RollForward rollForward,
																		 byte[] defaultFamilyBytes,
																		 byte[] packedColumnBytes,
																		 Collection<KVPair> toProcess,
																		 long transactionId,
																		 ConstraintChecker constraintChecker ) throws IOException;

		OperationStatus[] processKvBatch(Table table, RollForward rollForwardQueue, TxnView txnId,
																		 byte[] family, byte[] qualifier,
																		 Collection<KVPair> mutations,ConstraintChecker constraintChecker) throws IOException;
		DataStore getDataStore();
		SDataLib getDataLib();

    /**
     * Use the HBase counter API to store the specified transactionId as the value of a counter in this row.  The idea
     * is that (1) this method call itself should throw a write conflict exception if a concurrent transactions has
     * deleted the row; and (2) this method should cause future (concurrent) transaction to have a write conflict if
     * they attempt to delete the row. The same affect could be had by directly writing a KeyValue (of some new column)
     * to the specified row but that would potentially litter the target table with data having no purpose but forcing
     * these FK write-conflicts (negatively impacting read performance on that table).  By using an HBase counter we
     * take advantage of the detail that, on increment, HBase *replaces* the mem-store KV with the new KV we are writing.
     * If we quickly do one million FK checks we can end up with zero associated writes to the parent table (though
     * every counter update is still written to the WAL).
     */
    void updateCounterColumn(IHTable hbRegion, TxnView txnView, SRowLock rowLock, byte[] rowKey) throws IOException;
}
