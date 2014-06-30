package com.splicemachine.si;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.Clock;
import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.api.TxnLifecycleManager;
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
    LStore store;
    SDataLib dataLib;
    STableReader reader;
    STableWriter writer;
		Clock clock;
		TimestampSource source;

    public LStoreSetup() {
        dataLib = new LDataLib();
        clock = new IncrementingClock(1000);
        store = new LStore(clock);
        reader = store;
        writer = store;
				this.source = new SimpleTimestampSource();
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

    @Override public String getPersonTableName() { return "person"; }

		@Override
    public Clock getClock() {
        return clock;
    }

		@Override
		public TxnStore getTxnStore(TxnLifecycleManager txnLifecycleManager) {
				InMemoryTxnStore store = new InMemoryTxnStore(source, SIConstants.transactionTimeout);
				store.setLifecycleManager(txnLifecycleManager);
				return store;
		}

		@Override
		public TimestampSource getTimestampSource() {
				return source;
		}
}
