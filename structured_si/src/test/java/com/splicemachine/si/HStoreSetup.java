package com.splicemachine.si;

import com.google.common.base.Function;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.Clock;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HTableReader;
import com.splicemachine.si.data.hbase.HTableWriter;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.STableReaderDelegate;
import com.splicemachine.si.impl.SystemClock;
import com.splicemachine.si.impl.Tracer;
import com.splicemachine.utils.ZkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
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

		private TestHTableSource tableSource;

		public HStoreSetup(boolean usePacked) {
        setupHBaseHarness(usePacked);
    }

    public static Map<String, HRegion> regionMap = new HashMap<String, HRegion>();

    private TestHTableSource setupHTableSource(int basePort, boolean usePacked) {
        try {
            testCluster = new HBaseTestingUtility();
            Tracer.registerRegion(new Function<Object[], Object>() {
                @Override
                public Object apply(@Nullable Object[] input) {
										assert input != null;
										regionMap.put((String) input[0], (HRegion) input[1]);
                    return null;
                }
            });
						Configuration configuration = testCluster.getConfiguration();
						SpliceConstants.config = configuration;
            setTestingUtilityPorts(testCluster, basePort);

            testCluster.startMiniCluster(1);
						ZkUtils.getZkManager().initialize(configuration);
						ZkUtils.initializeZookeeper();

						tableSource = new TestHTableSource(testCluster,new String[]{SpliceConstants.DEFAULT_FAMILY,SIConstants.DEFAULT_FAMILY});
            tableSource.addTable(testCluster, SpliceConstants.TRANSACTION_TABLE, new String[]{
										SIConstants.DEFAULT_FAMILY, SIConstants.SI_PERMISSION_FAMILY});
						tableSource.addPackedTable(getPersonTableName());
            return tableSource;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void setTestingUtilityPorts(HBaseTestingUtility testCluster, int basePort) {
        testCluster.getConfiguration().setInt("hbase.master.port", basePort);
        testCluster.getConfiguration().setInt("hbase.master.info.port", basePort + 1);
        testCluster.getConfiguration().setInt("hbase.regionserver.port", basePort + 2);
        testCluster.getConfiguration().setInt("hbase.regionserver.info.port", basePort + 3);
    }

    private void setupHBaseHarness(boolean usePacked) {
        dataLib = new HDataLib();
        final STableReader<IHTable, Get, Scan> rawReader = new HTableReader(setupHTableSource(getNextBasePort(), usePacked));
        reader = new STableReaderDelegate<IHTable, Get, Scan>(rawReader) {
            @Override
            public void close(IHTable table) {
                // Ignore close calls
            }
        };
        writer = new HTableWriter();
    }

    @Override
    public SDataLib getDataLib() {
        return dataLib;
    }

    @Override public STableReader getReader() { return reader; }

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

		public void shutdown() throws Exception {
				ZkUtils.getZkManager().close();
				testCluster.shutdownMiniCluster();
		}
}
