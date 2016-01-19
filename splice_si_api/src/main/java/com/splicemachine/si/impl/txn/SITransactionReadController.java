package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.SimpleTxnFilter;
import com.splicemachine.si.impl.filter.HRowAccumulator;
import com.splicemachine.si.impl.filter.PackedTxnFilter;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.*;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/13/14
 */
public class SITransactionReadController<OperationWithAttributes,Data,
        Get extends OperationWithAttributes,
        Scan extends OperationWithAttributes>
        implements TransactionReadController{
    private final DataStore dataStore;
    private final SDataLib dataLib;
    private final TxnSupplier txnSupplier;
    private final IgnoreTxnCacheSupplier ignoreTxnSuppler;

    public SITransactionReadController(DataStore dataStore,
                                       TxnSupplier txnSupplier,
                                       IgnoreTxnCacheSupplier ignoreTxnSuppler){
        this.dataStore = dataStore;
        this.dataLib = dataStore.getDataLib();
        this.txnSupplier = txnSupplier;
        this.ignoreTxnSuppler = ignoreTxnSuppler;
    }

    @Override
    public void preProcessGet(DataGet get) throws IOException{
        get.returnAllVersions();
        get.setTimeRange(0,Long.MAX_VALUE);
    }

    @Override
    public void preProcessScan(DataScan scan) throws IOException{
        scan.setTimeRange(0l,Long.MAX_VALUE);
        scan.returnAllVersions();
    }

    @Override
    public TxnFilter newFilterState(ReadResolver readResolver,TxnView txn) throws IOException{
        return new SimpleTxnFilter(null,txn,readResolver,txnSupplier,ignoreTxnSuppler);
    }

    @Override
    public TxnFilter newFilterStatePacked(ReadResolver readResolver,
                                          EntryPredicateFilter predicateFilter,TxnView txn,boolean countStar) throws IOException{
        return new PackedTxnFilter(newFilterState(readResolver,txn),
                new HRowAccumulator(predicateFilter,new EntryDecoder(),countStar));
    }

    @Override
    public DDLFilter newDDLFilter(TxnView txn) throws IOException{
        return new DDLFilter(txn);
    }


}
