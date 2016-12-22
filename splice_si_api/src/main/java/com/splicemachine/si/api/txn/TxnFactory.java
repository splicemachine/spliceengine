package com.splicemachine.si.api.txn;

/**
 *
 *
 */
public interface TxnFactory {
    Txn getTxn();
    Txn[] getTxn(int batch);
}
