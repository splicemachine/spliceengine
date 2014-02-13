package com.splicemachine.si.api;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.si.impl.SICompactionState;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * The primary interface to the transaction module. This interface has the most burdensome generic signature so it is
 * only exposed in places where it is needed.
 */
public interface Transactor<Table, Put, Mutation, OperationStatus, Data, Hashable extends Comparable>{

		/**
     * Execute the put operation (with SI treatment) on the table. Send roll-forward notifications to the rollForwardQueue.
     */
    boolean processPut(Table table, RollForwardQueue<Data, Hashable> rollForwardQueue, Put put) throws IOException;
    OperationStatus[] processPutBatch(Table table, RollForwardQueue<Data, Hashable> rollForwardQueue, Mutation[] mutations)
            throws IOException;

		OperationStatus[] processKvBatch(Table table, RollForwardQueue<Data,Hashable> rollForwardQueue,Collection<KVPair> mutations,String txnId) throws IOException;

    /**
     * Attempt to update all of the data rows on the table to reflect the final status of the transaction with the given
     * transactionId.
     */
    Boolean rollForward(Table table, long transactionId, List<Data> rows) throws IOException;

    /**
     * Create an object to keep track of the state of an HBase table compaction operation.
     */
    SICompactionState newCompactionState();

}
