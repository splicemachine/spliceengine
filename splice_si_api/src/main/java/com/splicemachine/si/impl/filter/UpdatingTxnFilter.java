package com.splicemachine.si.impl.filter;

import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.SimpleTxnFilter;
import com.splicemachine.si.impl.readresolve.RegionSegmentContext;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.DataCell;

public class UpdatingTxnFilter extends SimpleTxnFilter{
    private final RegionSegmentContext context;

    public UpdatingTxnFilter(String tableName,TxnView myTxn,ReadResolver readResolver,TxnSupplier baseSupplier,IgnoreTxnCacheSupplier ignoreTxnSupplier,RegionSegmentContext context){
        super(tableName,myTxn,readResolver,baseSupplier,ignoreTxnSupplier);
        this.context=context;
    }

    @Override
    protected void doResolve(DataCell data,long ts){
        super.doResolve(data, ts);
        context.rowResolved();
    }

}