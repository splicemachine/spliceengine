package com.splicemachine.si.impl.store;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import java.io.IOException;

/**
 *
 *
 *
 */
public class FixedSetTxnSupplier implements TxnSupplier {
    private LongObjectOpenHashMap<Txn> suppliedTxns;
    public FixedSetTxnSupplier(LongObjectOpenHashMap<Txn> suppliedTxns) {
        this.suppliedTxns = suppliedTxns;
    }

    @Override
    public Txn getTransaction(long txnId) throws IOException {
        return suppliedTxns.get(txnId);
    }

    @Override
    public Txn[] getTransactions(long[] txnIds) throws IOException {
        Txn[] txns = new Txn[txnIds.length];
        for (int i = 0; i< txnIds.length;i++)
            txns[i] = getTransaction(txnIds[i]);
        return txns;
    }

    @Override
    public void cache(Txn toCache) {
        throw new UnsupportedOperationException("cannot cache a FixedSet Supplier");
    }

    @Override
    public void cache(Txn[] toCache) {
        throw new UnsupportedOperationException("cannot cache a FixedSet Supplier");
    }
}
