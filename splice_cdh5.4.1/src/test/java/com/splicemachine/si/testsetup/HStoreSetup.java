package com.splicemachine.si.testsetup;

import com.google.common.base.Function;
import com.splicemachine.access.hbase.HBaseSource;
import com.splicemachine.concurrent.*;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.coprocessors.TxnLifecycleEndpoint;
import com.splicemachine.si.api.data.IHTable;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.data.STableReader;
import com.splicemachine.si.api.data.STableWriter;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HTableReader;
import com.splicemachine.si.data.hbase.HTableWriter;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.store.CompletedTxnCacheSupplier;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.utils.SpliceUtilities;
import com.splicemachine.utils.ZkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class HStoreSetup implements StoreSetup {

    private static int nextBasePort = 12_000;
    public static final Map<String, HRegion> REGION_MAP = new HashMap<>();

    private SDataLib dataLib;
    private STableReader reader;
    private STableWriter writer;
    private Clock clock = new SystemClock();
    private HBaseTestingUtility testCluster;
    private TxnStore baseStore;
    private IgnoreTxnCacheSupplier ignoreTxnStore;
    private TimestampSource timestampSource;

    private static int getNextBasePort() {
        synchronized (HStoreSetup.class) {
            nextBasePort = nextBasePort + 4 + new Random().nextInt(10) * 100;
            return nextBasePort;
        }
    }

    public HStoreSetup() throws Exception {
        int basePort = getNextBasePort();

        dataLib = new HDataLib();
        testCluster = new HBaseTestingUtility();
        Tracer.registerRegion(new Function<Object[], Object>() {
            @Override
            public Object apply(Object[] input) {
                assert input != null;
                REGION_MAP.put((String) input[0], (HRegion) input[1]);
                return null;
            }
        });
        timestampSource = new SimpleTimestampSource();
        TransactionTimestamps.setTimestampSource(timestampSource);

        Configuration configuration = testCluster.getConfiguration();
        configuration.set("hbase.coprocessor.region.classes", TxnLifecycleEndpoint.class.getName());
        // -> MapR work-around
        configuration.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
        configuration.set("fs.default.name", "file:///");
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        System.setProperty("zookeeper.sasl.client", "false");
        System.setProperty("zookeeper.sasl.serverconfig", "fake");
        // <- MapR work-around
        configuration.setInt("hbase.master.port", basePort);
        configuration.setInt("hbase.master.info.port", basePort + 1);
        configuration.setInt("hbase.regionserver.port", basePort + 2);
        configuration.setInt("hbase.regionserver.info.port", basePort + 3);

        SpliceConstants.config = configuration;

        testCluster.startMiniCluster(1);
        ZkUtils.getZkManager().initialize(configuration);
        ZkUtils.initializeZookeeper();

        String familyString = Bytes.toString(SpliceConstants.DEFAULT_FAMILY_BYTES);
        TestHBaseTableFactory tableSource1 = new TestHBaseTableFactory(testCluster, new String[]{familyString});
        HBaseAdmin admin = testCluster.getHBaseAdmin();
        HTableDescriptor td = SpliceUtilities.generateTransactionTable();
        admin.createTable(td, SpliceUtilities.generateTransactionSplits());

        tableSource1.addPackedTable(getPersonTableName());

        TxnStore txnS = HBaseSource.getInstance().getTxnStore(timestampSource);
        txnS.setCache(new CompletedTxnCacheSupplier(txnS, SIConstants.activeTransactionCacheSize, 16));
        baseStore = txnS;
        TransactionStorage.setTxnStore(baseStore);
        ignoreTxnStore = TransactionStorage.getIgnoreTxnSupplier();
        //TODO -sf- add CompletedTxnCache to it

        STableReader<IHTable, Get, Scan> rawReader = new HTableReader(tableSource1);
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

    @Override
    public TxnStore getTxnStore() {
        return baseStore;
    }

    @Override
    public IgnoreTxnCacheSupplier getIgnoreTxnStore() {
        return ignoreTxnStore;
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
