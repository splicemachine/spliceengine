package com.splicemachine.si.impl.driver;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.STableFactory;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.timestamp.api.TimestampSource;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public interface SIEnvironment{
    STableFactory tableFactory();

    ExceptionFactory exceptionFactory();

    SConfiguration configuration();

    SDataLib dataLib();

    TxnStore txnStore();

    OperationStatusFactory statusFactory();

    TimestampSource timestampSource();

    TxnSupplier txnSupplier();

    IgnoreTxnCacheSupplier ignoreTxnSupplier();

    RollForward rollForward();

    TxnOperationFactory operationFactory();
}
