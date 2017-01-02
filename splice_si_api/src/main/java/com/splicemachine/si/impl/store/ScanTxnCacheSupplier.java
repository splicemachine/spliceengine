package com.splicemachine.si.impl.store;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import java.io.IOException;

/**
 *
 *
 *
 */
public class ScanTxnCacheSupplier implements TxnSupplier {
    TxnSupplier globalSupplier;
    TxnSupplier localSupplier;
    public ScanTxnCacheSupplier(TxnSupplier globalSupplier, TxnSupplier localSupplier) {
        this.globalSupplier = globalSupplier;
        this.localSupplier = localSupplier;
    }


    @Override
    public Txn getTransaction(long txnId) throws IOException {
        Txn txn = localSupplier.getTransaction(txnId);
        if (txn != null)
            return txn;
        return globalSupplier.getTransaction(txnId);
    }

    @Override
    public Txn[] getTransactions(long[] txnIds) throws IOException {
        Txn[] txns = new Txn[txnIds.length];
        for (int i=0; i< txnIds.length; i++) {
            txns[i] = getTransaction(txnIds[i]);
        }
        return txns;
    }

    @Override
    public void cache(Txn toCache) {
        if (!toCache.isActive())
            globalSupplier.cache(toCache);
        localSupplier.cache(toCache);
    }

    @Override
    public void cache(Txn[] toCache) {
        for (int i=0; i< toCache.length; i++) {
            cache(toCache[i]);
        }
    }
}
