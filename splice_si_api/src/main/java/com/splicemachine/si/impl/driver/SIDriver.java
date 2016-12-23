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
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.*;
import com.splicemachine.si.impl.ClientTxnLifecycleManager;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.server.SITransactor;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionInfoCache;
import com.splicemachine.timestamp.api.TimestampSource;
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

    private final PartitionFactory tableFactory;
    private final ExceptionFactory exceptionFactory;
    private final SConfiguration config;
    private final TransactionStore txnStore;
    private final OperationStatusFactory operationStatusFactory;
    private final TimestampSource logicalTimestampSource;
    private final TxnLocationFactory txnLocationFactory;
    private final TimestampSource physicalTimestampSource;
    private final TxnSupplier globalTxnCache;
    private final Transactor transactor;
    private final TxnOperationFactory txnOpFactory;
    private final TxnLifecycleManager lifecycleManager;
    private final Clock clock;
    private final DistributedFileSystem fileSystem;
    private final PartitionInfoCache partitionInfoCache;
    private final SnowflakeFactory snowflakeFactory;
    private final SIEnvironment env;
    private final TxnFactory txnFactory;

    public SIDriver(SIEnvironment env){
        this.tableFactory = env.tableFactory();
        this.exceptionFactory = env.exceptionFactory();
        this.config = env.configuration();
        this.txnStore = env.txnStore();
        this.operationStatusFactory = env.statusFactory();
        this.globalTxnCache = env.globalTxnCache();
        this.logicalTimestampSource = env.logicalTimestampSource();
        this.physicalTimestampSource = env.physicalTimestampSource();
        this.txnLocationFactory = env.txnLocationFactory();
        this.txnOpFactory = env.operationFactory();
        this.clock = env.systemClock();
        this.partitionInfoCache = env.partitionInfoCache();
        this.snowflakeFactory = env.snowflakeFactory();
        this.txnFactory = env.txnFactory();
        //noinspection unchecked
        this.transactor = new SITransactor(
                txnStore,
                this.txnOpFactory,
                this.operationStatusFactory,
                this.exceptionFactory);

        ClientTxnLifecycleManager clientTxnLifecycleManager=new
        ClientTxnLifecycleManager(logicalTimestampSource,
                physicalTimestampSource,
                exceptionFactory,
                txnFactory,
                txnLocationFactory,
                globalTxnCache,
                txnStore);
        this.lifecycleManager =clientTxnLifecycleManager;
        this.fileSystem = env.fileSystem();
        this.env = env;
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

    public TransactionStore getTxnStore() {
        return txnStore;
    }

    public OperationStatusFactory getOperationStatusLib() {
        return operationStatusFactory;
    }

    public PartitionInfoCache getPartitionInfoCache() { return partitionInfoCache; }


    public Transactor getTransactor(){
        return transactor;
    }

    public SIEnvironment getSIEnvironment(){
        return env;
    }

    public TxnOperationFactory getOperationFactory(){
        return txnOpFactory;
    }

    public TxnLifecycleManager lifecycleManager(){
        return lifecycleManager;
    }

    public TransactionalRegion transactionalPartition(long conglomId,Partition basePartition){
        if(conglomId>=0){
            return new TxnRegion(basePartition,
                    txnStore, // Need to figure out caching...
                    getTransactor(),
                    getOperationFactory());
        }else{
            return new TxnRegion(basePartition,
                    txnStore,
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


}
