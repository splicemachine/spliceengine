package com.splicemachine.si.impl.filter;

import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.SimpleTxnFilter;
import com.splicemachine.si.impl.readresolve.RegionSegmentContext;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.DataCell;

public class UpdatingTxnFilter<OperationWithAttributes,Data,Delete extends OperationWithAttributes,Filter,
        Get extends OperationWithAttributes,OperationStatus,
        Put extends OperationWithAttributes,RegionScanner,Result,ReturnCode,Scan extends OperationWithAttributes>
        extends SimpleTxnFilter<OperationWithAttributes,Data,Delete,Filter,
                Get,
        Put,RegionScanner,Result, Scan> {
    private final RegionSegmentContext context;

    public UpdatingTxnFilter(String tableName,TxnView myTxn,ReadResolver readResolver,TxnSupplier baseSupplier,IgnoreTxnCacheSupplier ignoreTxnSupplier,DataStore dataStore,RegionSegmentContext context){
        super(tableName,myTxn,readResolver,baseSupplier,ignoreTxnSupplier,dataStore);
        this.context=context;
    }

    @Override
    protected void doResolve(DataCell data,long ts){
        super.doResolve(data, ts);
        context.rowResolved();
    }

}