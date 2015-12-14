package com.splicemachine.si.testsetup;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.data.STableReader;
import com.splicemachine.si.api.data.STableWriter;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import org.apache.hadoop.hbase.HBaseTestingUtility;

public interface StoreSetup {

    SDataLib getDataLib();

    STableReader getReader();

    STableWriter getWriter();

    HBaseTestingUtility getTestCluster();

    Object getStore();

    String getPersonTableName();

    Clock getClock();

    TxnStore getTxnStore();

    IgnoreTxnCacheSupplier getIgnoreTxnStore();

    TimestampSource getTimestampSource();

}
