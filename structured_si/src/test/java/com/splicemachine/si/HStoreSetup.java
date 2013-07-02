package com.splicemachine.si;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.Clock;
import com.splicemachine.si.coprocessors.SIObserver;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HTableReader;
import com.splicemachine.si.data.hbase.HTableWriter;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.SystemClock;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

public class HStoreSetup implements StoreSetup {
    static int nextBasePort = 12000;

    static int getNextBasePort() {
        synchronized (HStoreSetup.class) {
            nextBasePort = nextBasePort + 4 + new Random().nextInt(10) * 100;
            return nextBasePort;
        }
    }

    SDataLib dataLib;
    STableReader reader;
    STableWriter writer;
    Clock clock = new SystemClock();

    HBaseTestingUtility testCluster;

    public HStoreSetup() {
        setupHBaseHarness();
    }

    private TestHTableSource setupHTableSource(int basePort) {
        try {
            testCluster = new HBaseTestingUtility();
            testCluster.getConfiguration().setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY, SIObserver.class.getName());
            setTestingUtilityPorts(testCluster, basePort);

            testCluster.startMiniCluster(1);
            final TestHTableSource tableSource = new TestHTableSource(testCluster, getPersonTableName(),
                    new String[]{SpliceConstants.DEFAULT_FAMILY, SIConstants.SNAPSHOT_ISOLATION_FAMILY});
            tableSource.addTable(testCluster, SpliceConstants.TRANSACTION_TABLE, new String[]{"siFamily", "siChildrenFamily"});
            return tableSource;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void setTestingUtilityPorts(HBaseTestingUtility testCluster, int basePort) {
        testCluster.getConfiguration().setInt("hbase.master.port", basePort + 0);
        testCluster.getConfiguration().setInt("hbase.master.info.port", basePort + 1);
        testCluster.getConfiguration().setInt("hbase.regionserver.port", basePort + 2);
        testCluster.getConfiguration().setInt("hbase.regionserver.info.port", basePort + 3);
    }

    private void setupHBaseHarness() {
        dataLib = new HDataLib();
        final STableReader<IHTable, Result, Get, Scan> rawReader = new HTableReader(setupHTableSource(getNextBasePort()));
        reader = new STableReader<IHTable, Result, Get, Scan>() {
            @Override
            public IHTable open(String tableName) {
                try {
                    return rawReader.open(tableName);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void close(IHTable table) {
            }

            @Override
            public Result get(IHTable table, Get get) throws IOException {
                return rawReader.get(table, get);
            }

            @Override
            public Iterator<Result> scan(IHTable table, Scan scan) throws IOException {
                return rawReader.scan(table, scan);
            }
        };
        writer = new HTableWriter();
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

    @Override
    public Clock getClock() {
        return clock;
    }
}
