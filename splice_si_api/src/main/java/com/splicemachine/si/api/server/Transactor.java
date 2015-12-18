package com.splicemachine.si.api.server;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.DataPut;
import com.splicemachine.storage.MutationStatus;
import com.splicemachine.storage.Partition;

import java.io.IOException;
import java.util.Collection;

/**
 * The primary interface to the transaction module. This interface has the most burdensome generic signature so it is
 * only exposed in places where it is needed.
 */
public interface Transactor{

    boolean processPut(Partition table,RollForward rollForwardQueue,DataPut put) throws IOException;

    MutationStatus[] processPutBatch(Partition table,RollForward rollForwardQueue,DataPut[] mutations) throws IOException;

    MutationStatus[] processKvBatch(Partition table,
                                    RollForward rollForward,
                                    byte[] defaultFamilyBytes,
                                    byte[] packedColumnBytes,
                                    Collection<KVPair> toProcess,
                                    long transactionId,
                                    ConstraintChecker constraintChecker) throws IOException;

    MutationStatus[] processKvBatch(Partition table,
                                    RollForward rollForward,
                                    byte[] defaultFamilyBytes,
                                    byte[] packedColumnBytes,
                                    Collection<KVPair> toProcess,
                                    TxnView txn,
                                    ConstraintChecker constraintChecker) throws IOException;

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
    void updateCounterColumn(Partition hbRegion,TxnView txnView,byte[] rowKey) throws IOException;
}
