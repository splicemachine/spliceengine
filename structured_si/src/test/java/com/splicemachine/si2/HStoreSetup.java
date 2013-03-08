package com.splicemachine.si2;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.SScan;
import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.data.api.STableWriter;
import com.splicemachine.si2.data.hbase.HDataLib;
import com.splicemachine.si2.data.hbase.HDataLibAdapter;
import com.splicemachine.si2.data.hbase.HStore;
import com.splicemachine.si2.data.hbase.HTableReaderAdapter;
import com.splicemachine.si2.data.hbase.HTableWriterAdapter;
import org.apache.hadoop.hbase.HBaseTestingUtility;

import java.util.Iterator;

public class HStoreSetup implements StoreSetup {
    SDataLib dataLib;
    STableReader reader;
    STableWriter writer;

    HBaseTestingUtility testCluster;

    public HStoreSetup() {
        setupHBaseHarness();
    }

    private HStore setupHBaseStore() {
        try {
            testCluster = new HBaseTestingUtility();
            testCluster.startMiniCluster(1);
            final TestHTableSource tableSource = new TestHTableSource(testCluster, "people", new String[]{"attributes", "_si"});
            tableSource.addTable(testCluster, "transaction", new String[]{"siFamily"});
            return new HStore(tableSource);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void setupHBaseHarness() {
        dataLib = new HDataLibAdapter(new HDataLib());
        final HStore store = setupHBaseStore();
        final STableReader rawReader = new HTableReaderAdapter(store);
        reader = new STableReader() {
            @Override
            public STable open(String tableName) {
                return rawReader.open(tableName);
            }

            @Override
            public void close(STable table) {
            }

            @Override
            public Object get(STable table, SGet get) {
                return rawReader.get(table, get);
            }

            @Override
            public Iterator scan(STable table, SScan scan) {
                return rawReader.scan(table, scan);
            }
        };
        writer = new HTableWriterAdapter(store);
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
        return testCluster;
    }

    @Override
    public Object getStore() {
        return null;
    }
}
