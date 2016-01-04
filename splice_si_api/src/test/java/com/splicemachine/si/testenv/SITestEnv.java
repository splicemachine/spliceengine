package com.splicemachine.si.testenv;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.Partition;
import com.splicemachine.timestamp.api.TimestampSource;

import java.io.IOException;

public interface SITestEnv extends SITestDataEnv{

    void initialize() throws IOException;

    String getPersonTableName();

    Clock getClock();

    TxnStore getTxnStore();

    IgnoreTxnCacheSupplier getIgnoreTxnStore();

    TimestampSource getTimestampSource();

    Partition getPersonTable(TestTransactionSetup tts) throws IOException;

    Partition getPartition(String name, TestTransactionSetup tts) throws IOException;

    PartitionFactory getTableFactory();

    void createTransactionalTable(byte[] tableNameBytes) throws IOException;
}
