package com.splicemachine.si.api.txn.lifecycle;

import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.utils.Source;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/14/15
 */
public interface TxnLifecycleStore{

    void beginTransaction(TxnMessage.TxnInfo txn) throws IOException;

    void elevateTransaction(long txnId, byte[] destTable) throws IOException;

    long commitTransaction(long txnId) throws IOException;

    void rollbackTransaction(long txnId) throws IOException;

    boolean keepAlive(long txnId) throws IOException;

    TxnMessage.Txn getTransaction(long txnId) throws IOException;

    long[] getActiveTransactionIds(byte[] destTable, long startId, long endId) throws IOException;

    Source<TxnMessage.Txn> getActiveTransactions(byte[] destTable, long startId, long endId) throws IOException;
}
