package com.splicemachine.si.testenv;

import com.splicemachine.access.api.STableFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.Partition;
import com.splicemachine.timestamp.api.TimestampSource;

public interface SITestEnv{

    SDataLib getDataLib();

    Object getStore();

    String getPersonTableName();

    Clock getClock();

    TxnStore getTxnStore();

    IgnoreTxnCacheSupplier getIgnoreTxnStore();

    TimestampSource getTimestampSource();

    Partition getPersonTable(TestTransactionSetup tts);

    DataFilterFactory getFilterFactory();

    ExceptionFactory getExceptionFactory();

    OperationStatusFactory getOperationStatusFactory();

    TxnOperationFactory getOperationFactory();

    STableFactory getTableFactory();
}
