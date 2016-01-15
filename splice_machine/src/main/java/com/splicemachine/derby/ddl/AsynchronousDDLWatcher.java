package com.splicemachine.derby.ddl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.filter.TransactionReadController;
import org.apache.log4j.Logger;
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
                                  DDLWatchChecker ddlWatchChecker){
        long maxDdlWait = config.getLong(DDLConfiguration.MAX_DDL_WAIT)<<1;
        this.refreshWaitMs = config.getLong(DDLConfiguration.DDL_REFRESH_INTERVAL);
        this.checker = ddlWatchChecker;
        this.refresher = new DDLWatchRefresher(checker,txnController,clock,maxDdlWait);
    }

    @Override
    public void start() throws StandardException {

        try{
            if(!checker.initialize(this))
                return; //we aren't a server, so do nothing further
        }catch(IOException ioe){
            throw Exceptions.parseException(ioe);
        }
        // run refresh() synchronously the first time
        try {
            if(!refresher.refreshDDL(ddlListeners)) return;
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }

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
}
