package com.splicemachine.si.api;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.impl.SICompactionState;
import com.splicemachine.si.impl.TransactionId;
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
    boolean processPut(Table table, RollForwardQueue rollForwardQueue, Put put) throws IOException;
    OperationStatus[] processPutBatch(Table table, RollForwardQueue rollForwardQueue, Mutation[] mutations)
            throws IOException;

		OperationStatus[] processKvBatch(Table table, RollForwardQueue rollForwardQueue, byte[] family, byte[] qualifier, Collection<KVPair> mutations, String txnId) throws IOException;

		OperationStatus[] processKvBatch(Table table, RollForwardQueue rollForwardQueue, TransactionId txnId, byte[] family, byte[] qualifier, Collection<KVPair> mutations) throws IOException;

    /**
     * Create an object to keep track of the state of an HBase table compaction operation.
     */
    SICompactionState newCompactionState();

}
