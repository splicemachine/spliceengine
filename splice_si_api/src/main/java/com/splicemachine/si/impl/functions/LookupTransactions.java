package com.splicemachine.si.impl.functions;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.storage.Record;
import org.spark_project.guava.base.Function;

import javax.annotation.Nullable;

/**
 *
 * Utilize Global Cache to resolve transaction and place transactions seen into global cache
 * Modify record inline
 */
public class LookupTransactions implements Function<LongSet,Txn[]> {
    private TxnSupplier txnSupplier;

    public LookupTransactions(TxnSupplier txnSupplier) {
        this.txnSupplier = txnSupplier;
    }


    @Nullable
    @Override
    public Txn[] apply(LongSet txnsToLookup) {
        try {
            return txnSupplier.getTransactions(txnsToLookup.toArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
}
