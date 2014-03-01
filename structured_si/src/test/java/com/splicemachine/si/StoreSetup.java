package com.splicemachine.si;

import com.splicemachine.si.api.Clock;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import org.apache.hadoop.hbase.HBaseTestingUtility;

public interface StoreSetup {
    SDataLib getDataLib();
    STableReader getReader();
    STableWriter getWriter();

		HBaseTestingUtility getTestCluster();
    Object getStore();
    String getPersonTableName();
    Clock getClock();
}
