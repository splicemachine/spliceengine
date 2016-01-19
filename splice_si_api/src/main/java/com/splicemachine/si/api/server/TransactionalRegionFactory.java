package com.splicemachine.si.api.server;

import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.server.SITransactor;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.Partition;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class TransactionalRegionFactory{
    private final TxnSupplier txnSupplier;
    private final IgnoreTxnCacheSupplier ignoreSupplier;
    private final SITransactor transactor;
    private final TxnOperationFactory txnOpFactory;
    private final RollForward rollForward;
    private final ReadResolver readResolver;

    public TransactionalRegionFactory(TxnSupplier txnSupplier,
                                      IgnoreTxnCacheSupplier ignoreSupplier,
                                      SITransactor transactor,
                                      TxnOperationFactory txnOpFactory,
                                      RollForward rollForward,
                                      ReadResolver readResolver){
        this.txnSupplier=txnSupplier;
        this.ignoreSupplier=ignoreSupplier;
        this.transactor=transactor;
        this.txnOpFactory=txnOpFactory;
        this.rollForward=rollForward;
        this.readResolver=readResolver;
    }

    public TransactionalRegion newRegion(Partition p){
        return new TxnRegion(p,rollForward, readResolver,txnSupplier,ignoreSupplier,transactor,txnOpFactory);
    }
}
