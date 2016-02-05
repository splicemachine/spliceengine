package com.splicemachine.si.testsetup;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.IncrementingClock;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.data.HExceptionFactory;
import com.splicemachine.si.data.hbase.HOperationStatusFactory;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.si.data.hbase.coprocessor.SIObserver;
import com.splicemachine.si.data.hbase.coprocessor.TableFactoryService;
import com.splicemachine.si.data.hbase.coprocessor.TxnLifecycleEndpoint;
import com.splicemachine.si.impl.HOperationFactory;
import com.splicemachine.si.impl.SimpleTxnOperationFactory;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.driver.SIEnvironment;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.si.testenv.SITestEnv;
import com.splicemachine.si.testenv.TestTransactionSetup;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.HFilterFactory;
import com.splicemachine.storage.Partition;
import com.splicemachine.timestamp.api.TimestampSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import java.io.IOException;
import java.util.Random;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class HBaseSITestEnv implements SITestEnv{
    private static int nextBasePort = 12_000;
    private final PartitionFactory<TableName> tableFactory;
    private MiniHBaseCluster testCluster;
    private Clock clock;
    private TxnStore txnStore;
    private IgnoreTxnCacheSupplier ignoreTxnSupplier;
    private TimestampSource timestampSource;
    private HBaseTestingUtility testUtility;

    public HBaseSITestEnv(){
        this(Level.ERROR);
    }

    public HBaseSITestEnv(Level baseLoggingLevel){
        configureLogging(baseLoggingLevel);
        Configuration conf = HConfiguration.INSTANCE.unwrapDelegate();
        try{
            startCluster(conf);
            SIEnvironment hEnv=loadSIEnvironment();
            try(HBaseAdmin hBaseAdmin=testUtility.getHBaseAdmin()){
                // TODO (wjk): use default namespace constant instead of 'splice'?
                hBaseAdmin.createNamespace(NamespaceDescriptor.create("splice").build());
                addTxnTable(hBaseAdmin);
            }

            this.clock = new IncrementingClock();
            this.txnStore = hEnv.txnStore();
            this.ignoreTxnSupplier = hEnv.ignoreTxnSupplier();
            this.timestampSource = hEnv.timestampSource();
            this.tableFactory = TableFactoryService.loadTableFactory(clock,hEnv.configuration());
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void initialize() throws IOException{
        try(HBaseAdmin hBaseAdmin=testUtility.getHBaseAdmin()){
            hBaseAdmin.createTable(generateDefaultSIGovernedTable("person"));
        }
    }

    @Override public String getPersonTableName(){ return "person"; }
    @Override public ExceptionFactory getExceptionFactory(){ return HExceptionFactory.INSTANCE; }
    @Override public OperationStatusFactory getOperationStatusFactory(){ return HOperationStatusFactory.INSTANCE; }

    @Override public Clock getClock(){ return clock; }
    @Override public TxnStore getTxnStore(){ return txnStore; }
    @Override public IgnoreTxnCacheSupplier getIgnoreTxnStore(){ return ignoreTxnSupplier; }
    @Override public TimestampSource getTimestampSource(){ return timestampSource; }
    @Override public DataFilterFactory getFilterFactory(){ return HFilterFactory.INSTANCE; }
    @Override public PartitionFactory getTableFactory(){ return tableFactory; }

    @Override
    public Partition getPersonTable(TestTransactionSetup tts) throws IOException{
        return getTableFactory().getTable(getPersonTableName());
    }

    @Override
    public TxnOperationFactory getOperationFactory(){
        return new SimpleTxnOperationFactory(getExceptionFactory(),HOperationFactory.INSTANCE);
    }

    @Override
    public OperationFactory getBaseOperationFactory(){
        return HOperationFactory.INSTANCE;
    }

    @Override
    public Partition getPartition(String name,TestTransactionSetup tts) throws IOException{
        return getTableFactory().getTable(name);
    }

    @Override
    public void createTransactionalTable(byte[] tableNameBytes) throws IOException{
        try(PartitionAdmin pa = getTableFactory().getAdmin()){
            PartitionCreator partitionCreator=pa.newPartition().withName(Bytes.toString(tableNameBytes))
                    .withCoprocessor(SIObserver.class.getName());
            addCoprocessors(partitionCreator);
            partitionCreator.create();
        }
    }

    protected void addCoprocessors(PartitionCreator userTable) throws IOException{
        //default no-op
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    private static HTableDescriptor generateTransactionTable() throws IOException{
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("splice",HConfiguration.TRANSACTION_TABLE));
        desc.addCoprocessor(TxnLifecycleEndpoint.class.getName());

        HColumnDescriptor columnDescriptor = new HColumnDescriptor(SIConstants.DEFAULT_FAMILY_BYTES);
        columnDescriptor.setMaxVersions(5);
        columnDescriptor.setCompressionType(Compression.Algorithm.NONE);
        columnDescriptor.setInMemory(true);
        columnDescriptor.setBlockCacheEnabled(true);
        columnDescriptor.setBloomFilterType(BloomType.ROWCOL);
        desc.addFamily(columnDescriptor);
        desc.addFamily(new HColumnDescriptor(Bytes.toBytes(SIConstants.SI_PERMISSION_FAMILY)));
        return desc;
    }


    public static HColumnDescriptor createDataFamily() {
        HColumnDescriptor snapshot = new HColumnDescriptor(SIConstants.DEFAULT_FAMILY_BYTES);
        snapshot.setMaxVersions(Integer.MAX_VALUE);
        snapshot.setCompressionType(Compression.Algorithm.NONE);
        snapshot.setInMemory(true);
        snapshot.setBlockCacheEnabled(true);
        snapshot.setBloomFilterType(BloomType.ROW);
        return snapshot;
    }

    private HTableDescriptor generateDefaultSIGovernedTable(String tableName) throws IOException{
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(HConfiguration.INSTANCE.getString(HConfiguration.NAMESPACE),tableName));
        desc.addFamily(createDataFamily());
        desc.addCoprocessor(SIObserver.class.getName());
        return desc;
    }


    private void addTxnTable(HBaseAdmin admin) throws IOException{
        byte[][] splits = new byte[15][];

        for(int i = 0; i < splits.length; ++i) {
            splits[i] = new byte[]{(byte)(i + 1)};
        }

        admin.createTable(generateTransactionTable(),splits);
    }

    private void startCluster(Configuration conf) throws Exception{
        int basePort = getNextBasePort();
        testUtility = new HBaseTestingUtility(conf);

        Configuration configuration = testUtility.getConfiguration();
        // -> MapR work-around
        configuration.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
        configuration.set("fs.default.name", "file:///");
        configuration.set("fs.hdfs.client", "org.apache.hadoop.hdfs.DistributedFileSystem");
        System.setProperty("zookeeper.sasl.client", "false");
        System.setProperty("zookeeper.sasl.serverconfig", "fake");
        // <- MapR work-around
        configuration.setInt("hbase.master.port", basePort);
        configuration.setInt("hbase.master.info.port", basePort + 1);
        configuration.setInt("hbase.regionserver.port", basePort + 2);
        configuration.setInt("hbase.regionserver.info.port", basePort + 3);

        testUtility.startMiniCluster(1);
        ZkUtils.getZkManager().initialize(HConfiguration.INSTANCE);
        ZkUtils.initializeZookeeper();
    }

    private static int getNextBasePort() {
        synchronized (HBaseSITestEnv.class) {
            nextBasePort = nextBasePort + 4 + new Random().nextInt(10) * 100;
            return nextBasePort;
        }
    }

    private SIEnvironment loadSIEnvironment() throws IOException{
        HBaseSIEnvironment siEnv=new HBaseSIEnvironment(new ConcurrentTimestampSource(),clock);
        HBaseSIEnvironment.setEnvironment(siEnv);
        SIDriver.loadDriver(siEnv);
        return siEnv;
    }

    private void configureLogging(Level baseLevel){
        Logger.getRootLogger().setLevel(baseLevel);
        Logger.getRootLogger().addAppender(new ConsoleAppender(new SimpleLayout()));
        Logger.getLogger("org.apache.hadoop.conf").setLevel(Level.WARN);
        Logger.getLogger("org.apache.hadoop.hdfs").setLevel(baseLevel);
        Logger.getLogger("org.apache.hadoop.http").setLevel(baseLevel);
        Logger.getLogger("org.apache.hadoop.metrics2").setLevel(baseLevel);
        Logger.getLogger("org.apache.hadoop.net").setLevel(baseLevel);
        Logger.getLogger("org.apache.hadoop.ipc").setLevel(baseLevel);
        Logger.getLogger("org.apache.hadoop.util").setLevel(baseLevel);
        Logger.getLogger("org.apache.hadoop.hbase").setLevel(baseLevel);
        Logger.getLogger("BlockStateChange").setLevel(baseLevel);
        Logger.getLogger("org.mortbay.log").setLevel(baseLevel);
        Logger.getLogger("org.apache.zookeeper").setLevel(baseLevel);
    }
}
