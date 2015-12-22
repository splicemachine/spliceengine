package com.splicemachine.si.impl;

import com.splicemachine.si.coprocessor.TxnMessage;

import java.io.IOException;
import java.util.Collection;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public interface TxnNetworkLayer extends AutoCloseable{

    void beginTransaction(byte[] rowKey,TxnMessage.TxnInfo txnInfo) throws IOException;

    TxnMessage.ActionResponse lifecycleAction(byte[] rowKey,TxnMessage.TxnLifecycleMessage lifecycleMessage) throws IOException;

    void elevate(byte[] rowKey,TxnMessage.ElevateRequest elevateRequest) throws IOException;

    long[] getActiveTxnIds(TxnMessage.ActiveTxnRequest request) throws IOException;

    Collection<TxnMessage.ActiveTxnResponse> getActiveTxns(TxnMessage.ActiveTxnRequest request) throws IOException;

    TxnMessage.Txn getTxn(byte[] rowKey,TxnMessage.TxnRequest request) throws IOException;

    void close() throws IOException;
}
