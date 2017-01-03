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
public class ResolveTransaction implements Function<Record[],LongSet> {
    private TxnSupplier txnSupplier;
    private Txn activeTxn;

    public ResolveTransaction(TxnSupplier txnSupplier,Txn activeTxn) {
        this.txnSupplier = txnSupplier;
        this.activeTxn = activeTxn;
    }


    @Nullable
    @Override
    public LongSet apply(Record[] records) {
        LongSet txnSet = null;
        try {
            for (Record record : records) {
                // Empty Array Element, Txn Resolved (Committed or Rolledback) (1)
                if (record == null || record.getEffectiveTimestamp() != 0)
                    break;

                // Collapsable Txn (2)
                if (record.getTxnId2() < 0) {
                    Txn txn = txnSupplier.getTransaction(record.getTxnId1());
                    if (txn == null) { // Txn cannot be looked up in cache
                        if (txnSet==null)
                            txnSet = new LongOpenHashSet();
                        txnSet.add(record.getTxnId1());
                        continue;
                    }
                    txn.resolveCollapsibleTxn(record,activeTxn,txn);
                }
                // Hierarchical Txn (3)
                if (record.getTxnId1() > record.getTxnId2()) {
                    Txn childTxn = txnSupplier.getTransaction(record.getTxnId1());
                    Txn parentTxn = txnSupplier.getTransaction(record.getTxnId2());
                    if (childTxn == null) {
                        if (txnSet == null)
                            txnSet = new LongOpenHashSet();
                        txnSet.add(record.getTxnId1());
                    }
                    if (parentTxn == null) {
                        if (txnSet == null)
                            txnSet = new LongOpenHashSet();
                        txnSet.add(record.getTxnId2());
                    } else {


                    }


                }
            }
            return txnSet;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
