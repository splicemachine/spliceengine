package com.splicemachine.si.testsetup;

import com.splicemachine.si.api.Clock;
import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.api.TxnStore;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
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
