package com.splicemachine.si;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.light.Clock;
import com.splicemachine.si.data.light.IncrementingClock;
import com.splicemachine.si.data.light.LDataLib;
import com.splicemachine.si.data.light.LStore;
import org.apache.hadoop.hbase.HBaseTestingUtility;

public class LStoreSetup implements StoreSetup {
    LStore store;
    SDataLib dataLib;
    STableReader reader;
    STableWriter writer;

    public LStoreSetup() {
        dataLib = new LDataLib();
        Clock clock = new IncrementingClock(1000);
        store = new LStore(clock);
        reader = store;
        writer = store;
    }

    @Override
    public SDataLib getDataLib() {
        return dataLib;
    }

    @Override
    public STableReader getReader() {
        return reader;
    }

    @Override
    public STableWriter getWriter() {
        return writer;
    }

    @Override
    public HBaseTestingUtility getTestCluster() {
        return null;
    }

    @Override
    public Object getStore() {
        return store;
    }

    @Override
    public String getPersonTableName() {
        return "person";
    }
}
