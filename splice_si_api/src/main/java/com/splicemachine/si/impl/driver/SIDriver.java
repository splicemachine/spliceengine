/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.si.impl.driver;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.PartitionFactory;
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
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.ClientTxnLifecycleManager;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.si.impl.rollforward.RollForwardStatus;
import com.splicemachine.si.impl.server.SITransactor;
import com.splicemachine.si.impl.txn.SITransactionReadController;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionInfoCache;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.utils.GreenLight;
import org.apache.log4j.Logger;

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
    private final SConfiguration config;
    private final TxnStore txnStore;
    private final OperationStatusFactory operationStatusFactory;
    private final TimestampSource timestampSource;
    private final TxnSupplier txnSupplier;
    private final Transactor transactor;
    private final TxnOperationFactory txnOpFactory;
    private final RollForward rollForward;
    private final TxnLifecycleManager lifecycleManager;
    private final DataFilterFactory filterFactory;
    private final Clock clock;
    private final AsyncReadResolver readResolver;
    private final DistributedFileSystem fileSystem;
    private final OperationFactory baseOpFactory;
    private final PartitionInfoCache partitionInfoCache;
    private final SnowflakeFactory snowflakeFactory;
    private final SIEnvironment env;

    public SIDriver(SIEnvironment env){
        this.tableFactory = env.tableFactory();
        this.exceptionFactory = env.exceptionFactory();
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
        this.fileSystem = env.fileSystem();
        this.baseOpFactory = env.baseOperationFactory();
        this.env = env;
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

    public DistributedFileSystem fileSystem(){
        return fileSystem;
    }

    public DistributedFileSystem getFileSystem(String path){
        return fileSystem;
    }


    public OperationFactory baseOperationFactory(){
        return baseOpFactory;
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
}
