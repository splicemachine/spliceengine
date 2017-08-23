package com.splicemachine.si.impl.functions;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.txn.CommittedTxn;
import com.splicemachine.si.impl.txn.RolledBackTxn;
import com.splicemachine.storage.Record;
import com.splicemachine.utils.Pair;
import org.spark_project.guava.base.Function;
import javax.annotation.Nullable;

/**
 *
 * Utilize Global Cache to resolve transaction and place transactions seen into global cache
 * Modify record inline
 */
public class ResolveTransaction implements Function<Record[],Pair<BitSet,BitSet>> {
    private TxnSupplier txnSupplier;
    private Txn activeTxn;
    public static Pair<BitSet,BitSet> DEFAULT_INCLUDE_ALL = new Pair<>(null,null);

    public ResolveTransaction(TxnSupplier txnSupplier,Txn activeTxn) {
        this.txnSupplier = txnSupplier;
        this.activeTxn = activeTxn;
    }


    @Nullable
    @Override
    public Pair<BitSet,BitSet> apply(Record[] records) {
        for (int i = 0; i< records.length; i++) {
            if (records[i].getEffectiveTimestamp() == -1) { // Resolved Rolled Back
                if (!txnSupplier.transactionCached(records[i].getTxnId1()))
                    txnSupplier.cache(new RolledBackTxn(records[i].getTxnId1()));
            } else if (records[i].getEffectiveTimestamp() > 0) { // Resolved Committed
                if (!txnSupplier.transactionCached(records[i].getTxnId1()))
                    txnSupplier.cache(new CommittedTxn(records[i].getTxnId1(), records[i].getEffectiveTimestamp()));
            } else { // Needs Resolution
             //   txnSupplier.
            }
        }
        return DEFAULT_INCLUDE_ALL;
    }

}
