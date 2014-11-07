package com.splicemachine.si.impl;

import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.rollforward.SegmentedRollForward;

public class UpdatingTxnFilter<RowLock,Data> extends SimpleTxnFilter<RowLock,Data> {
    private final SegmentedRollForward.Context context;

    public UpdatingTxnFilter(TxnSupplier transactionStore,
                             TxnView myTxn,
                             ReadResolver readResolver,
                             DataStore dataStore,
                             SegmentedRollForward.Context context) {
        super(transactionStore, myTxn, readResolver, dataStore);
        this.context = context;
    }

    @Override
    protected void doResolve(Data keyValue, long ts) {
        super.doResolve(keyValue, ts);
        context.rowResolved();
    }
}