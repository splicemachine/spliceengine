package com.splicemachine.si.impl;

import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.rollforward.SegmentedRollForward;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;

public class UpdatingTxnFilter<Data> extends SimpleTxnFilter<Data> {
    private final SegmentedRollForward.Context context;

    public UpdatingTxnFilter(String tableName,
                             TxnSupplier transactionStore,
                             IgnoreTxnCacheSupplier ignoreTxnCacheSupplier,
                             TxnView myTxn,
                             ReadResolver readResolver,
                             DataStore dataStore,
                             SegmentedRollForward.Context context) {
        super(tableName, transactionStore, ignoreTxnCacheSupplier, myTxn, readResolver, dataStore);
        this.context = context;
    }

    @Override
    protected void doResolve(Data keyValue, long ts) {
        super.doResolve(keyValue, ts);
        context.rowResolved();
    }
}