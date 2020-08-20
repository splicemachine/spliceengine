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

package com.splicemachine.si.impl.driver;

import splice.com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.FilesystemAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.OldestActiveTransactionTaskFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.SnowflakeFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.readresolve.AsyncReadResolver;
import com.splicemachine.si.api.readresolve.KeyedReadResolver;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.rollforward.RollForward;
import com.splicemachine.si.api.server.ClusterHealth;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.ClientTxnLifecycleManager;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.execution.ManagedThreadPool;
import com.splicemachine.si.impl.execution.NonRejectingExecutor;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.si.impl.rollforward.RollForwardStatus;
import com.splicemachine.si.impl.server.SITransactor;
import com.splicemachine.si.impl.store.IgnoreTxnSupplier;
import com.splicemachine.si.impl.txn.SITransactionReadController;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionInfoCache;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.utils.GreenLight;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SIDriver {
    private static final Logger LOG = Logger.getLogger("splice.uncaught");

    static {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("["+t.getName()+"] Uncaught error, killing thread: ", e);
            }
        });
    }

    private static volatile SIDriver INSTANCE;

    public static SIDriver driver(){ return INSTANCE;}

    public static SIDriver loadDriver(SIEnvironment env){
        SIDriver siDriver=new SIDriver(env);
        INSTANCE =siDriver;
        return siDriver;
    }

    private final SITransactionReadController readController;
    private final PartitionFactory tableFactory;
    private final ExceptionFactory exceptionFactory;
    private final OldestActiveTransactionTaskFactory oldestActiveTransactionTaskFactory;
    private final SConfiguration config;
    private final TxnStore txnStore;
    private final OperationStatusFactory operationStatusFactory;
    private final TimestampSource timestampSource;
    private final TxnSupplier txnSupplier;
    private final IgnoreTxnSupplier ignoreTxnSupplier;
    private final Transactor transactor;
    private final TxnOperationFactory txnOpFactory;
    private final RollForward rollForward;
    private final TxnLifecycleManager lifecycleManager;
    private final DataFilterFactory filterFactory;
    private final Clock clock;
    private final AsyncReadResolver readResolver;
    private final OperationFactory baseOpFactory;
    private final PartitionInfoCache partitionInfoCache;
    private final SnowflakeFactory snowflakeFactory;
    private final SIEnvironment env;
    private final ClusterHealth clusterHealth;
    private final ManagedThreadPool rejectingThreadPool;
    private final NonRejectingExecutor threadPool;
    private boolean engineStarted = false;

    public SIDriver(SIEnvironment env){
        this.tableFactory = env.tableFactory();
        this.exceptionFactory = env.exceptionFactory();
        this.oldestActiveTransactionTaskFactory = env.oldestActiveTransactionTaskFactory();
        this.config = env.configuration();
        this.txnStore = env.txnStore();
        this.operationStatusFactory = env.statusFactory();
        this.timestampSource = env.timestampSource();
        this.txnSupplier = env.txnSupplier();
        this.txnOpFactory = env.operationFactory();
        this.rollForward = env.rollForward();
        this.filterFactory = env.filterFactory();
        this.clock = env.systemClock();
        this.partitionInfoCache = env.partitionInfoCache();
        this.snowflakeFactory = env.snowflakeFactory();
        this.ignoreTxnSupplier = env.ignoreTxnSupplier();
        //noinspection unchecked
        this.transactor = new SITransactor(
                this.txnSupplier,
                this.txnOpFactory,
                env.baseOperationFactory(),
                this.operationStatusFactory,
                this.exceptionFactory);
        ClientTxnLifecycleManager clientTxnLifecycleManager=new ClientTxnLifecycleManager(this.timestampSource,env.exceptionFactory());
        clientTxnLifecycleManager.setTxnStore(this.txnStore);
        clientTxnLifecycleManager.setKeepAliveScheduler(env.keepAliveScheduler());
        this.lifecycleManager =clientTxnLifecycleManager;
        readController = new SITransactionReadController(txnSupplier);
        readResolver = initializedReadResolver(config,env.keyedReadResolver());
        this.baseOpFactory = env.baseOperationFactory();
        this.env = env;
        this.clusterHealth = env.clusterHealthFactory();

        /* Create a general purpose thread pool */
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(20, config.getThreadPoolMaxSize(),
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new ThreadFactoryBuilder().setNameFormat("SpliceThreadPool-%d").setDaemon(true).build(),
                new ThreadPoolExecutor.AbortPolicy());
        tpe.allowCoreThreadTimeOut(false);
        tpe.prestartAllCoreThreads();
        this.rejectingThreadPool = new ManagedThreadPool(tpe);
        this.threadPool = new NonRejectingExecutor(rejectingThreadPool);
    }


    public TransactionReadController readController(){
        return readController;
    }

    public PartitionFactory getTableFactory(){
        return tableFactory;
    }

    public ExceptionFactory getExceptionFactory(){
        return exceptionFactory;
    }

    public OldestActiveTransactionTaskFactory getOldestActiveTransactionTaskFactory() {
        return oldestActiveTransactionTaskFactory;
    }

    public SnowflakeFactory getSnowflakeFactory() {
        return snowflakeFactory;
    }

    /**
     * @return the configuration specific to this architecture.
     */
    public SConfiguration getConfiguration(){
        return config;
    }

    public TxnStore getTxnStore() {
        return txnStore;
    }

    public TxnSupplier getTxnSupplier(){
        return txnSupplier;
    }
    public IgnoreTxnSupplier getIgnoreTxnSupplier(){
        return ignoreTxnSupplier;
    }
    public OperationStatusFactory getOperationStatusLib() {
        return operationStatusFactory;
    }

    public PartitionInfoCache getPartitionInfoCache() { return partitionInfoCache; }

    public TimestampSource getTimestampSource() {
        return timestampSource;
    }

    public Transactor getTransactor(){
        return transactor;
    }

    public SIEnvironment getSIEnvironment(){
        return env;
    }

    public TxnOperationFactory getOperationFactory(){
        return txnOpFactory;
    }

    public RollForward getRollForward(){
        return rollForward;
    }

    public ReadResolver getReadResolver(Partition basePartition){
        if(readResolver==null) return NoOpReadResolver.INSTANCE;
        else
        return readResolver.getResolver(basePartition,getRollForward());
    }

    public TxnLifecycleManager lifecycleManager(){
        return lifecycleManager;
    }

    public DataFilterFactory filterFactory(){
        return filterFactory;
    }

    public TransactionalRegion transactionalPartition(long conglomId,Partition basePartition){
        if(conglomId>=0){
            return new TxnRegion(basePartition,
                    getRollForward(),
                    getReadResolver(basePartition),
                    getTxnSupplier(),
                    getTransactor(),
                    getOperationFactory());
        }else{
            return new TxnRegion(basePartition,
                    NoopRollForward.INSTANCE,
                    NoOpReadResolver.INSTANCE,
                    getTxnSupplier(),
                    getTransactor(),
                    getOperationFactory());
        }
    }

    public Clock getClock(){
        return clock;
    }

    public DistributedFileSystem getFileSystem(String path) throws URISyntaxException, IOException {
        return SIDriver.driver().getSIEnvironment().fileSystem(path);
    }

    public FilesystemAdmin getFilesystemAdmin() {
        return env.filesystemAdmin();
    }

    public ClusterHealth clusterHealth() {
        return clusterHealth;
    }


    public OperationFactory baseOperationFactory(){
        return baseOpFactory;
    }

    public ExecutorService getExecutorService() {
        return threadPool;
    }

    public ExecutorService getRejectingExecutorService() {
        return rejectingThreadPool;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private AsyncReadResolver initializedReadResolver(SConfiguration config,KeyedReadResolver keyedResolver){
        int maxThreads = config.getReadResolverThreads();
        int bufferSize = config.getReadResolverQueueSize();
        if(bufferSize<=0) return null;
        final AsyncReadResolver asyncReadResolver=new AsyncReadResolver(maxThreads,
                bufferSize,
                txnSupplier,
                new RollForwardStatus(),
                GreenLight.INSTANCE,keyedResolver);
        asyncReadResolver.start();
        return asyncReadResolver;
    }

    public void shutdownDriver() {
        threadPool.shutdownNow();
        getTimestampSource().shutdown();
    }

    public boolean isEngineStarted() {
        return engineStarted;
    }

    public void engineStarted() {
        engineStarted = true;
    }
}
