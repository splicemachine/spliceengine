package com.splicemachine.si.api.server;

import com.splicemachine.kvpair.KVPair;
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

}
