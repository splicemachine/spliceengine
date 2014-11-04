package com.splicemachine.si.api;

import com.splicemachine.hbase.KVPair;
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

}
