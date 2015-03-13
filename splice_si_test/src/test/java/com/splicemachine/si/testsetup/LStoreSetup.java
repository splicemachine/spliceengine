package com.splicemachine.si.testsetup;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.Clock;
import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.api.TxnStore;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.light.IncrementingClock;
import com.splicemachine.si.data.light.LDataLib;
import com.splicemachine.si.data.light.LStore;
import com.splicemachine.si.impl.InMemoryTxnStore;
import org.apache.hadoop.hbase.HBaseTestingUtility;

public class LStoreSetup implements StoreSetup {

    private LStore store;
    private SDataLib dataLib;
    private STableReader reader;
    private STableWriter writer;
    private Clock clock;
    private TimestampSource source;
    private TxnStore txnStore;

    public LStoreSetup() {
        this.dataLib = new LDataLib();
        this.clock = new IncrementingClock(1_000);
        this.store = new LStore(clock);
        this.reader = store;
        this.writer = store;
        this.source = new SimpleTimestampSource();
        this.txnStore = new InMemoryTxnStore(source, SIConstants.transactionTimeout);
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

    @Override
    public Clock getClock() {
        return clock;
    }

    @Override
    public TxnStore getTxnStore() {
        return txnStore;
    }

    @Override
    public TimestampSource getTimestampSource() {
        return source;
    }
}
