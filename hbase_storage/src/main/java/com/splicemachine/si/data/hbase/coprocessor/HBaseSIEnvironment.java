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

package com.splicemachine.si.data.hbase.coprocessor;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import com.splicemachine.access.api.*;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.access.hbase.HFilesystemAdmin;
import com.splicemachine.access.hbase.HSnowflakeFactory;
import com.splicemachine.access.util.ByteComparisons;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.si.api.server.ClusterHealth;
import com.splicemachine.si.data.hbase.rollforward.HBaseRollForward;
import com.splicemachine.si.impl.store.IgnoreTxnSupplier;
import com.splicemachine.storage.HClusterHealthFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.readresolve.KeyedReadResolver;
import com.splicemachine.si.api.rollforward.RollForward;
import com.splicemachine.si.api.txn.KeepAliveScheduler;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.data.HExceptionFactory;
import com.splicemachine.si.data.hbase.HOperationStatusFactory;
import com.splicemachine.si.impl.CoprocessorTxnStore;
import com.splicemachine.si.impl.HOperationFactory;
import com.splicemachine.si.impl.QueuedKeepAliveScheduler;
import com.splicemachine.si.impl.SimpleTxnOperationFactory;
import com.splicemachine.si.impl.TxnNetworkLayerFactory;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.driver.SIEnvironment;
import com.splicemachine.si.impl.readresolve.SynchronousReadResolver;
import com.splicemachine.si.impl.store.CompletedTxnCacheSupplier;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.HFilterFactory;
import com.splicemachine.storage.HNIOFileSystem;
import com.splicemachine.storage.PartitionInfoCache;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.timestamp.hbase.ZkTimestampSource;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class HBaseSIEnvironment implements SIEnvironment{
    private static volatile HBaseSIEnvironment INSTANCE;

    private final TimestampSource timestampSource;
    private final PartitionFactory<TableName> partitionFactory;
    private final OldestActiveTransactionTaskFactory oldestActiveTransactionTaskFactory;
    private final TxnStore txnStore;
    private final TxnSupplier txnSupplier;
    private final IgnoreTxnSupplier ignoreTxnSupplier;
    private final TxnOperationFactory txnOpFactory;
    private final PartitionInfoCache partitionCache;
    private final KeepAliveScheduler keepAlive;
    private final SConfiguration config;
    private final HOperationFactory opFactory;
    private final Clock clock;
    private final DistributedFileSystem fileSystem;
    private final SnowflakeFactory snowflakeFactory;
    private final HClusterHealthFactory clusterHealthFactory;
    private final HFilesystemAdmin filesystemAdmin;
    private final HBaseRollForward rollForward;
    private SIDriver siDriver;


    public static HBaseSIEnvironment loadEnvironment(Clock clock,RecoverableZooKeeper rzk) throws IOException{
        HBaseSIEnvironment env = INSTANCE;
        if(env==null){
            synchronized(HBaseSIEnvironment.class){
                env = INSTANCE;
                if(env==null){
                    env = INSTANCE = new HBaseSIEnvironment(rzk,clock);
                }
            }
        }
        return env;
    }

    public static void setEnvironment(HBaseSIEnvironment siEnv){
        INSTANCE = siEnv;
    }

    public HBaseSIEnvironment(TimestampSource timeSource,Clock clock) throws IOException{
        ByteComparisons.setComparator(HBaseComparator.INSTANCE);
        this.config=HConfiguration.getConfiguration();
        this.timestampSource = timeSource;
        this.partitionCache = PartitionCacheService.loadPartitionCache(config);
        this.partitionFactory = TableFactoryService.loadTableFactory(clock,this.config,partitionCache);
        this.oldestActiveTransactionTaskFactory = new HOldestActiveTransactionTaskFactory();
        TxnNetworkLayerFactory txnNetworkLayerFactory= TableFactoryService.loadTxnNetworkLayer(this.config);
        this.txnStore = new CoprocessorTxnStore(txnNetworkLayerFactory,timestampSource,null);
        int completedTxnCacheSize = config.getCompletedTxnCacheSize();
        int completedTxnConcurrency = config.getCompletedTxnConcurrency();
        this.txnSupplier = new CompletedTxnCacheSupplier(txnStore,completedTxnCacheSize,completedTxnConcurrency);
        this.txnStore.setCache(txnSupplier);
        this.opFactory = HOperationFactory.INSTANCE;
        this.txnOpFactory = new SimpleTxnOperationFactory(exceptionFactory(),opFactory);
        this.ignoreTxnSupplier = new IgnoreTxnSupplier(partitionFactory, txnOpFactory);
        this.clock = clock;
        this.snowflakeFactory = new HSnowflakeFactory();
        this.fileSystem = new HNIOFileSystem(FileSystem.get((Configuration) config.getConfigSource().unwrapDelegate()), exceptionFactory());

        this.filesystemAdmin = new HFilesystemAdmin(HBaseConnectionFactory.getInstance(config).getConnection().getAdmin());
        this.keepAlive = new QueuedKeepAliveScheduler(config.getTransactionKeepAliveInterval(),
                config.getTransactionTimeout(),
                config.getTransactionKeepAliveThreads(),
                txnStore);
        this.clusterHealthFactory = new HClusterHealthFactory(ZkUtils.getRecoverableZooKeeper());
        this.rollForward = new HBaseRollForward(txnSupplier, config);
        this.rollForward.start();
        siDriver = SIDriver.loadDriver(this);
    }

    @SuppressWarnings("unchecked")
    public HBaseSIEnvironment(RecoverableZooKeeper rzk,Clock clock) throws IOException{
        ByteComparisons.setComparator(HBaseComparator.INSTANCE);
        this.config=HConfiguration.getConfiguration();

        this.timestampSource = new ZkTimestampSource(config,rzk);
        this.partitionCache = PartitionCacheService.loadPartitionCache(config);
        this.partitionFactory = TableFactoryService.loadTableFactory(clock, this.config,partitionCache);
        this.oldestActiveTransactionTaskFactory = new HOldestActiveTransactionTaskFactory();
        TxnNetworkLayerFactory txnNetworkLayerFactory= TableFactoryService.loadTxnNetworkLayer(this.config);
        this.txnStore = new CoprocessorTxnStore(txnNetworkLayerFactory,timestampSource,null);
        int completedTxnCacheSize = config.getCompletedTxnCacheSize();
        int completedTxnConcurrency = config.getCompletedTxnConcurrency();
        this.txnSupplier = new CompletedTxnCacheSupplier(txnStore,completedTxnCacheSize,completedTxnConcurrency);
        this.txnStore.setCache(txnSupplier);
        this.opFactory = HOperationFactory.INSTANCE;
        this.txnOpFactory = new SimpleTxnOperationFactory(exceptionFactory(),opFactory);
        this.clock = clock;
        this.fileSystem = new HNIOFileSystem(FileSystem.get((Configuration) config.getConfigSource().unwrapDelegate()), exceptionFactory());
        this.snowflakeFactory = new HSnowflakeFactory();
        this.clusterHealthFactory = new HClusterHealthFactory(rzk);
        this.ignoreTxnSupplier = new IgnoreTxnSupplier(partitionFactory, txnOpFactory);
        this.filesystemAdmin = new HFilesystemAdmin(HBaseConnectionFactory.getInstance(config).getConnection().getAdmin());

        this.keepAlive = new QueuedKeepAliveScheduler(config.getTransactionKeepAliveInterval(),
                config.getTransactionTimeout(),
                config.getTransactionKeepAliveThreads(),
                txnStore);
        this.rollForward = new HBaseRollForward(txnSupplier, config);
        this.rollForward.start();
        siDriver = SIDriver.loadDriver(this);
    }


    @Override public PartitionFactory tableFactory(){ return partitionFactory; }

    @Override
    public ExceptionFactory exceptionFactory(){
        return HExceptionFactory.INSTANCE;
    }

    @Override
    public OldestActiveTransactionTaskFactory oldestActiveTransactionTaskFactory(){
        return oldestActiveTransactionTaskFactory;
    }

    @Override
    public SConfiguration configuration(){
        return config;
    }

    @Override
    public TxnStore txnStore(){
        return txnStore;
    }

    @Override
    public TxnSupplier txnSupplier(){
        return txnSupplier;
    }

    @Override
    public IgnoreTxnSupplier ignoreTxnSupplier(){
        return ignoreTxnSupplier;
    }

    @Override
    public OperationStatusFactory statusFactory(){
        return HOperationStatusFactory.INSTANCE;
    }

    @Override
    public TimestampSource timestampSource(){
        return timestampSource;
    }

    @Override
    public RollForward rollForward(){
        return rollForward;
    }

    @Override
    public TxnOperationFactory operationFactory(){
        return txnOpFactory;
    }

    @Override
    public SIDriver getSIDriver(){
        return siDriver;
    }

    @Override
    public PartitionInfoCache partitionInfoCache(){
        return partitionCache;
    }

    @Override
    public KeepAliveScheduler keepAliveScheduler(){
        return keepAlive;
    }

    @Override
    public DataFilterFactory filterFactory(){
        return HFilterFactory.INSTANCE;
    }

    @Override
    public Clock systemClock(){
        return clock;
    }

    @Override
    public KeyedReadResolver keyedReadResolver(){
        return SynchronousReadResolver.INSTANCE;
    }

    @Override
    public DistributedFileSystem fileSystem(String path) throws IOException, URISyntaxException  {
        return new HNIOFileSystem(FileSystem.get(new URI(path), (Configuration) config.getConfigSource().unwrapDelegate()), exceptionFactory());
    }

    @Override
    public OperationFactory baseOperationFactory(){
        return opFactory;
    }

    public void setSIDriver(SIDriver siDriver) {
        this.siDriver = siDriver;
    }

    @Override
    public SnowflakeFactory snowflakeFactory() {
        return snowflakeFactory;
    }

    @Override
    public ClusterHealth clusterHealthFactory() {
        return clusterHealthFactory;
    }

    @Override
    public FilesystemAdmin filesystemAdmin() {
        return filesystemAdmin;
    }
}
