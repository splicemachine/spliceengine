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

package com.splicemachine.derby.ddl;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;
import org.spark_project.guava.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.ddl.DDLMessage.DDLChange;
import com.splicemachine.si.api.txn.TxnSupplier;

/**
 * An instance of this class in each region server listens for DDL notifications.
 */
public class AsynchronousDDLWatcher implements DDLWatcher,CommunicationListener{

    private static final Logger LOG = Logger.getLogger(AsynchronousDDLWatcher.class);


    private Set<DDLListener> ddlListeners =new CopyOnWriteArraySet<>();

    private ExecutorService refreshThread = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setNameFormat("ZooKeeperDDLWatcherRefresher").setDaemon(true).build());

    private final Lock refreshNotifierLock = new ReentrantLock();
    private final Condition refreshNotifierCondition = refreshNotifierLock.newCondition();
    private final AtomicInteger requestCount = new AtomicInteger(0);
    private final DDLWatchChecker checker;
    private final DDLWatchRefresher refresher; //= new DDLWatchRefresher(checker,new SystemClock(),MAXIMUM_DDL_WAIT_MS);
    private final long refreshWaitMs;

    public AsynchronousDDLWatcher(Clock clock,
                                  SConfiguration config,
                                  DDLWatchChecker ddlWatchChecker,
                                  SqlExceptionFactory exceptionFactory,
                                  TxnSupplier txnSupplier){
        long maxDdlWait = config.getMaxDdlWait() << 1;
        this.refreshWaitMs = config.getDdlRefreshInterval();
        this.checker = ddlWatchChecker;
        this.refresher = new DDLWatchRefresher(checker,
                clock,
                exceptionFactory,
                maxDdlWait,txnSupplier );
    }

    @Override
    public void start() throws IOException {
        if(!checker.initialize(this))
            return; //we aren't a server, so do nothing further
        // run refresh() synchronously the first time
        if(!refresher.refreshDDL(ddlListeners)) return;

        refreshThread.submit(new Runnable() {
            @Override
            public void run() {
                /*
                 * We loop infinitely here, and rely on daemon threads to allow
                 * us to shut down properly. This way, we will just always be running
                 * as long as we are up.
                 */
                //noinspection InfiniteLoopStatement
                while(true){
                    int signalledWhileRefresh;
                    int currentSignalSize = requestCount.get();
                    try{
                        if(!refresher.refreshDDL(ddlListeners)) return;
                    }catch(Throwable e){
                        LOG.error("Failed to refresh ddl",e);
                    }

                    refreshNotifierLock.lock();
                    try{
                        signalledWhileRefresh = requestCount.addAndGet(-currentSignalSize);
                        //someone notified us while we were refreshing, so don't go to sleep yet
                        if(signalledWhileRefresh!=0)
                            continue;
                        /*
                         * Wait to be notified, but only up to the refresh interval. After that,
                         * we go ahead and refresh anyway.
                         */
                        refreshNotifierCondition.await(refreshWaitMs,TimeUnit.MILLISECONDS);
                    }catch (InterruptedException e) {
                        LOG.error("Interrupted while forcibly refreshing, terminating thread");
                    } finally{
                        refreshNotifierLock.unlock();
                    }
                }
            }
        });
    }

    public void signalRefresh() {
        /*
         * We use condition signalling to notify events; this is
         * a pretty decent way of ensuring that we don't waste resources
         * when nothing is going on. However, it is possible that, if you
         * receive a notification of an event while you are in the process
         * of refreshing, that that event may not be picked up. To prevent
         * this, we keep track of how many signals we emit, and the refresh
         * thread makes sure to perform refreshes if it received an event
         * while it wasn't listening.
         */
        refreshNotifierLock.lock();
        try {
            requestCount.incrementAndGet();
            refreshNotifierCondition.signal();
        }finally{
            refreshNotifierLock.unlock();
        }
    }

    @Override
    public void onCommunicationEvent(String node){
       signalRefresh();
    }

    @Override
    public Collection<DDLChange> getTentativeDDLs() {
        return refresher.tentativeDDLChanges();
    }

    @Override
    public void registerDDLListener(DDLListener listener) {
        if(refresher.numCurrentDDLChanges()>0)
            listener.startGlobalChange();
        this.ddlListeners.add(listener);
    }

    @Override
    public void unregisterDDLListener(DDLListener listener) {
        ddlListeners.remove(listener);
    }

    @Override
    public boolean canUseCache(TransactionManager xact_mgr) {
        return refresher.canUseCache(xact_mgr);
    }

    @Override
    public boolean canUseSPSCache(TransactionManager txnMgr){
        return refresher.canUseSPSCache(txnMgr);
    }
}
