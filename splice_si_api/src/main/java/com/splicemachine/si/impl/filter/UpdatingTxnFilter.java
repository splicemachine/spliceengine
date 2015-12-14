package com.splicemachine.si.impl.filter;

import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.SimpleTxnFilter;
import com.splicemachine.si.impl.readresolve.RegionSegmentContext;

public class UpdatingTxnFilter<OperationWithAttributes,Data,Delete extends OperationWithAttributes,Filter,
        Get extends OperationWithAttributes,Mutation,OperationStatus,Pair,
        Put extends OperationWithAttributes,RegionScanner,Result,ReturnCode,RowLock,Scan extends OperationWithAttributes,Table>
        extends SimpleTxnFilter<OperationWithAttributes,Data,Delete,Filter,
                Get,Mutation,OperationStatus,Pair,
                Put,RegionScanner,Result,ReturnCode,RowLock,Scan,Table> {
    private final RegionSegmentContext context;

    public UpdatingTxnFilter(String tableName,
                             TxnView myTxn,
                             ReadResolver readResolver,
                             RegionSegmentContext context) {
        super(tableName, myTxn, readResolver);
        this.context = context;
    }

    @Override
    protected void doResolve(Data keyValue, long ts) {
        super.doResolve(keyValue, ts);
        context.rowResolved();
    }
}