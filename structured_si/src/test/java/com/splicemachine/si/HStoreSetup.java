package com.splicemachine.si;

import com.google.common.base.Function;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.table.SpliceHTableFactory;
import com.splicemachine.si.api.*;
import com.splicemachine.si.coprocessors.TxnLifecycleEndpoint;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HTableReader;
import com.splicemachine.si.data.hbase.HTableWriter;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.txnclient.CoprocessorTxnStore;
import com.splicemachine.utils.ZkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;

import javax.annotation.Nullable;
import java.io.IOException;
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

		private TxnStore baseStore;
		private TimestampSource timestampSource;

		public HStoreSetup(boolean usePacked) {
        try {
            setupHBaseHarness(usePacked);
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
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
						this.timestampSource = new SimpleTimestampSource();
						HTransactorFactory.setTimestampSource(timestampSource);
						Configuration configuration = testCluster.getConfiguration();
						configuration.set("hbase.coprocessor.region.classes", TxnLifecycleEndpoint.class.getName());
						SpliceConstants.config = configuration;
            setTestingUtilityPorts(testCluster, basePort);

            testCluster.startMiniCluster(1);
						ZkUtils.getZkManager().initialize(configuration);
						ZkUtils.initializeZookeeper();

						tableSource = new TestHTableSource(testCluster,new String[]{SpliceConstants.DEFAULT_FAMILY,SIConstants.DEFAULT_FAMILY});
            tableSource.addTable(testCluster, SpliceConstants.TRANSACTION_TABLE, new String[]{
										SIConstants.DEFAULT_FAMILY, SIConstants.SI_PERMISSION_FAMILY});
						tableSource.addPackedTable(getPersonTableName());

						CoprocessorTxnStore txnS = new CoprocessorTxnStore(new SpliceHTableFactory(true),timestampSource,null);
						txnS.setCache(new CompletedTxnCacheSupplier(new LazyTxnSupplier(txnS),SIConstants.activeTransactionCacheSize,16));
						baseStore = txnS;
						HTransactorFactory.setTxnStore(baseStore);
						//TODO -sf- add CompletedTxnCache to it
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

    private void setupHBaseHarness(boolean usePacked) throws IOException {
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

		@Override
		public TxnStore getTxnStore(TxnLifecycleManager txnLifecycleManager) {
				return baseStore;
		}

		@Override
		public TimestampSource getTimestampSource() {
				return timestampSource;
		}

		public void shutdown() throws Exception {
				ZkUtils.getZkManager().close();
				testCluster.shutdownMiniCluster();
		}
}
