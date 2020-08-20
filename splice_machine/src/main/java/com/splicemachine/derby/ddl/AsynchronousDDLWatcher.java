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
import splice.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.ddl.DDLMessage.DDLChange;
import com.splicemachine.si.api.filter.TransactionReadController;
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

    public AsynchronousDDLWatcher(TransactionReadController txnController,
                                  Clock clock,
                                  SConfiguration config,
                                  DDLWatchChecker ddlWatchChecker,
                                  SqlExceptionFactory exceptionFactory,
                                  TxnSupplier txnSupplier){
        this.refreshWaitMs = config.getDdlRefreshInterval();
        this.checker = ddlWatchChecker;
        this.refresher = new DDLWatchRefresher(checker,
                txnController,
                exceptionFactory,
                txnSupplier );
    }

    @Override
    public void start() throws IOException {
        if(!checker.initialize(this))
            return; //we aren't a server, so do nothing further

        try {
            // run refresh() synchronously the first time
            if (!refresher.refreshDDL(ddlListeners)) return;

            refreshThread.submit(new Runnable() {
                @Override
                public void run() {
                    /*
                     * We loop infinitely here, and rely on daemon threads to allow
                     * us to shut down properly. This way, we will just always be running
                     * as long as we are up.
                     */
                    //noinspection InfiniteLoopStatement
                    while (true) {
                        int signalledWhileRefresh;
                        int currentSignalSize = requestCount.get();
                        try {
                            if (!refresher.refreshDDL(ddlListeners)) return;
                        } catch (Throwable e) {
                            LOG.error("Failed to refresh ddl", e);
                        }

                        refreshNotifierLock.lock();
                        try {
                            signalledWhileRefresh = requestCount.addAndGet(-currentSignalSize);
                            //someone notified us while we were refreshing, so don't go to sleep yet
                            if (signalledWhileRefresh != 0)
                                continue;
                            /*
                             * Wait to be notified, but only up to the refresh interval. After that,
                             * we go ahead and refresh anyway.
                             */
                            refreshNotifierCondition.await(refreshWaitMs, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            LOG.error("Interrupted while forcibly refreshing, terminating thread");
                        } finally {
                            refreshNotifierLock.unlock();
                        }
                    }
                }
            });
        } catch (Exception e) {
            LOG.error("Unexpected exception while starting DDLWatcher", e);
            // Swallow exception to prevent boot process from failing, we've already registered in ZK so it should be fine
        }
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
    public boolean canWriteCache(TransactionManager xact_mgr) {
        return refresher.canWriteCache(xact_mgr);
    }

    @Override
    public boolean canReadCache(TransactionManager xact_mgr) {
        return refresher.canReadCache(xact_mgr);
    }

    @Override
    public boolean canUseSPSCache(TransactionManager txnMgr){
        return refresher.canUseSPSCache(txnMgr);
    }
}
