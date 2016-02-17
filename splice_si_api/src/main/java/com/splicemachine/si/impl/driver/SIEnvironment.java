package com.splicemachine.si.impl.driver;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.api.data.*;
import com.splicemachine.si.api.readresolve.KeyedReadResolver;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.txn.KeepAliveScheduler;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.PartitionInfoCache;
import com.splicemachine.timestamp.api.TimestampSource;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public interface SIEnvironment{
    PartitionFactory tableFactory();

    ExceptionFactory exceptionFactory();

    SConfiguration configuration();

    TxnStore txnStore();

    OperationStatusFactory statusFactory();

    TimestampSource timestampSource();

    TxnSupplier txnSupplier();

    IgnoreTxnCacheSupplier ignoreTxnSupplier();

    RollForward rollForward();

    TxnOperationFactory operationFactory();

    SIDriver getSIDriver();

    PartitionInfoCache partitionInfoCache();

    KeepAliveScheduler keepAliveScheduler();

    DataFilterFactory filterFactory();

    Clock systemClock();

    KeyedReadResolver keyedReadResolver();

    DistributedFileSystem fileSystem();

    OperationFactory baseOperationFactory();

}
