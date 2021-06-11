/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

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
import com.splicemachine.si.testenv.SITestEnv;
import com.splicemachine.si.testenv.TestTransactionSetup;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.HFilterFactory;
import com.splicemachine.storage.Partition;
import com.splicemachine.timestamp.api.TimestampSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.async.AsyncLogger;
import org.apache.logging.log4j.core.async.AsyncLoggerConfig;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.apache.logging.log4j.core.async.AsyncLoggerConfig.createLogger;

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
    private TimestampSource timestampSource;
    private HBaseTestingUtility testUtility;

    public HBaseSITestEnv(){
        this(Level.ERROR);
    }

    public HBaseSITestEnv(Level baseLoggingLevel){
        configureLogging(baseLoggingLevel);
        Configuration conf = HConfiguration.unwrapDelegate();
        conf.set("fs.defaultFS", "file:///");
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
            this.timestampSource = hEnv.timestampSource();
            this.tableFactory = TableFactoryService.loadTableFactory(clock,hEnv.configuration(),hEnv.partitionInfoCache());
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void initialize() throws IOException{
        try(HBaseAdmin hBaseAdmin=testUtility.getHBaseAdmin()){
            TableDescriptor table = generateDefaultSIGovernedTable("1440");
            if (hBaseAdmin.tableExists(table.getTableName())) {
                hBaseAdmin.disableTable(table.getTableName());
                hBaseAdmin.deleteTable(table.getTableName());
            }
            hBaseAdmin.createTable(table);
        }
    }

    @Override public String getPersonTableName(){ return "1440"; }
    @Override public ExceptionFactory getExceptionFactory(){ return HExceptionFactory.INSTANCE; }
    @Override public OperationStatusFactory getOperationStatusFactory(){ return HOperationStatusFactory.INSTANCE; }

    @Override public Clock getClock(){ return clock; }
    @Override public TxnStore getTxnStore(){ return txnStore; }
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

    private static TableDescriptor generateTransactionTable() throws IOException{
        TableName tableName = TableName.valueOf("splice",HConfiguration.TRANSACTION_TABLE);
        TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
        tableBuilder.setCoprocessor(TxnLifecycleEndpoint.class.getName());

        ColumnFamilyDescriptor[] columnBuilders = {
                ColumnFamilyDescriptorBuilder
                        .newBuilder(SIConstants.DEFAULT_FAMILY_BYTES)
                        .setMaxVersions(5)
                        .setCompressionType(Compression.Algorithm.NONE)
                        .setInMemory(true)
                        .setBlockCacheEnabled(true)
                        .setBloomFilterType(BloomType.ROWCOL)
                        .build(),
                ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes(SIConstants.SI_PERMISSION_FAMILY))
                        .build()
        };

        tableBuilder.setColumnFamilies(Arrays.asList(columnBuilders));
        return tableBuilder.build();
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

    private TableDescriptor generateDefaultSIGovernedTable(String tableName) throws IOException{
        TableName name = TableName.valueOf(HConfiguration.getConfiguration().getNamespace(),tableName);
        return TableDescriptorBuilder
                .newBuilder(name)
                .setColumnFamily(createDataFamily())
                .setCoprocessor(SIObserver.class.getName())
                .build();
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
        // -> MapR work-around
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
        conf.set("fs.default.name", "file:///");
        conf.set("fs.hdfs.client", "org.apache.hadoop.hdfs.DistributedFileSystem");
        System.setProperty("zookeeper.sasl.client", "false");
        System.setProperty("zookeeper.sasl.serverconfig", "fake");
        // <- MapR work-around
        conf.setInt("hbase.master.port", basePort);
        conf.setInt("hbase.master.info.port", basePort + 1);
        conf.setInt("hbase.regionserver.port", basePort + 2);
        conf.setInt("hbase.regionserver.info.port", basePort + 3);

        testUtility = new HBaseTestingUtility(conf);

        Configuration configuration = testUtility.getConfiguration();
        // -> MapR work-around
        configuration.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
        configuration.set("fs.default.name", "file:///");
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.set("fs.hdfs.client", "org.apache.hadoop.hdfs.DistributedFileSystem");
        System.setProperty("zookeeper.sasl.client", "false");
        System.setProperty("zookeeper.sasl.serverconfig", "fake");
        // <- MapR work-around
        configuration.setInt("hbase.master.port", basePort);
        configuration.setInt("hbase.master.info.port", basePort + 1);
        configuration.setInt("hbase.regionserver.port", basePort + 2);
        configuration.setInt("hbase.regionserver.info.port", basePort + 3);
        if (FileSystem.class.getProtectionDomain().getCodeSource().getLocation().getPath().contains("mapr")) {
            testUtility.startMiniCluster(1);
        } else {
            testUtility.startMiniZKCluster();
            testUtility.startMiniHBaseCluster(1, 1);
        }
        ZkUtils.getZkManager().initialize(HConfiguration.getConfiguration());
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
        SIDriver driver = SIDriver.loadDriver(siEnv);
        siEnv.setSIDriver(driver);
        return siEnv;
    }

    private void createLogger(String loggerName, Level level, AppenderRef[] refs,
                              org.apache.logging.log4j.core.config.Configuration config, Appender appender) {
        LoggerConfig loggerConfig = AsyncLoggerConfig.createLogger(false, level, loggerName, "true", refs, null, config, null);
        loggerConfig.addAppender(appender, null, null);
        config.addLogger(loggerName, loggerConfig);
    }

    private void configureLogging(Level baseLevel){
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
        final Layout layout = PatternLayout.createDefaultLayout(config);
        ConsoleAppender consoleAppender = ConsoleAppender.createDefaultAppenderForLayout(layout);
        consoleAppender.start();
        config.addAppender(consoleAppender);
        Configurator.setLevel(LogManager.getRootLogger().getName(), baseLevel);
        LoggerConfig rootLoggerCfg = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        rootLoggerCfg.addAppender(consoleAppender, null, null);

//        AppenderRef ref = AppenderRef.createAppenderRef("Console", null, null);
//        AppenderRef[] refs = new AppenderRef[] {ref};
//        createLogger("org.apache.hadoop.conf", Level.WARN, refs, config, consoleAppender);
//        createLogger("org.apache.hadoop.hdfs", baseLevel, refs, config, consoleAppender);
//        createLogger("org.apache.hadoop.http", baseLevel, refs, config, consoleAppender);
//        createLogger("org.apache.hadoop.metrics2", baseLevel, refs, config, consoleAppender);
//        createLogger("org.apache.hadoop.net", baseLevel, refs, config, consoleAppender);
//        createLogger("org.apache.hadoop.ipc", baseLevel, refs, config, consoleAppender);
//        createLogger("org.apache.hadoop.conf", baseLevel, refs, config, consoleAppender);
//        createLogger("org.apache.hadoop.util", baseLevel, refs, config, consoleAppender);
//        createLogger("org.apache.hadoop.hbase", baseLevel, refs, config, consoleAppender);
//        createLogger("BlockStateChange", baseLevel, refs, config, consoleAppender);
//        createLogger("org.apache.hadoop.hbase", baseLevel, refs, config, consoleAppender);
//        createLogger("BlockStateChange", baseLevel, refs, config, consoleAppender);

        Configurator.setLevel("org.apache.hadoop.conf", Level.WARN);
        Configurator.setLevel("org.apache.hadoop.hdfs", baseLevel);
        Configurator.setLevel("org.apache.hadoop.http", baseLevel);
        Configurator.setLevel("org.apache.hadoop.metrics2", baseLevel);
        Configurator.setLevel("org.apache.hadoop.net", baseLevel);
        Configurator.setLevel("org.apache.hadoop.ipc", baseLevel);
        Configurator.setLevel("org.apache.hadoop.util", baseLevel);
        Configurator.setLevel("org.apache.hadoop.hbase", baseLevel);
        Configurator.setLevel("BlockStateChange", baseLevel);
        Configurator.setLevel("org.mortbay.log", baseLevel);
        Configurator.setLevel("org.apache.zookeeper", baseLevel);
        ctx.updateLoggers();
    }
}
