package com.splicemachine.si2;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.data.api.STableWriter;
import org.apache.hadoop.hbase.HBaseTestingUtility;

public interface StoreSetup {
    SDataLib getDataLib();
    STableReader getReader();
    STableWriter getWriter();
    HBaseTestingUtility getTestCluster();
    Object getStore();
    String getPersonTableName();
}
