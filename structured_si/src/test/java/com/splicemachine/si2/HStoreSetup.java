package com.splicemachine.si2;

import com.splicemachine.si.utils.SIConstants;
import com.splicemachine.si2.coprocessors.SIObserver;
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
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;

import java.io.IOException;
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
            testCluster.getConfiguration().setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY, SIObserver.class.getName());

            testCluster.startMiniCluster(1);
            final TestHTableSource tableSource = new TestHTableSource(testCluster, getPersonTableName(),
                    new String[]{SIConstants.DEFAULT_FAMILY, SIConstants.SNAPSHOT_ISOLATION_FAMILY});
            tableSource.addTable(testCluster, SIConstants.TRANSACTION_TABLE, new String[]{"siFamily", "siChildrenFamily"});
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
                try {
                    return rawReader.open(tableName);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
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

    @Override
    public String getPersonTableName() {
        return "999";
    }
}
