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

package com.splicemachine.si.impl;

import com.splicemachine.si.impl.driver.SIDriver;
import splice.com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.concurrent.ThreadLocalRandom;
import com.splicemachine.si.api.txn.KeepAliveScheduler;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 6/25/14
 */
public class QueuedKeepAliveScheduler implements KeepAliveScheduler{
    private static final Logger LOG=Logger.getLogger(QueuedKeepAliveScheduler.class);
    private final long maxWaitIntervalMs;
    private final long maxKeepAliveIntervalMs;
    private final ScheduledExecutorService threadPool;
    private final com.splicemachine.concurrent.ThreadLocalRandom random;

    private final
    @ThreadSafe
    TxnStore txnStore;

    private volatile boolean shutdown=false;


    public QueuedKeepAliveScheduler(long maxWaitIntervalMs,long maxKeepAliveIntervalMs,
                                    int numKeepers,TxnStore txnStore){
        this.maxWaitIntervalMs=maxWaitIntervalMs;
        ThreadFactory factory=new ThreadFactoryBuilder().setNameFormat("keepAlive-thread-%d").setDaemon(true).build();

        this.threadPool=Executors.newScheduledThreadPool(numKeepers,factory);
        this.random=ThreadLocalRandom.current();
        this.txnStore=txnStore;
        this.maxKeepAliveIntervalMs=maxKeepAliveIntervalMs;
    }

    @Override
    public void scheduleKeepAlive(Txn txn){
        if(shutdown) return;

//				activeTxns.add(txn);
        threadPool.schedule(new KeepAlive(txn),random.nextLong(maxWaitIntervalMs),TimeUnit.MILLISECONDS);
    }

    @Override
    public void start(){
    }

    @Override
    public void stop(){
        shutdown=true;
        threadPool.shutdownNow();
    }

    private class KeepAlive implements Runnable{
        private final Txn txn;
        private long lastKeepAliveTime;

        public KeepAlive(Txn txn){
            this.txn=txn;
            this.lastKeepAliveTime=System.currentTimeMillis();
        }

        @Override
        public void run(){
            if (SIDriver.driver().lifecycleManager().isRestoreMode()){
                return;
            }
            if(txn.getEffectiveState()!=Txn.State.ACTIVE){
                return; //nothing to do, we no longer need to keep anything alive
            }
            long keepAliveTime=System.currentTimeMillis()-lastKeepAliveTime;

            if(keepAliveTime>2*maxKeepAliveIntervalMs){
                SpliceLogUtils.warn(LOG,"It has been %d ms since the last time we tried to perform"+
                        "a keep alive, which is longer than the maximum interval");
                                /*
								 * We are the only ones trying to keep this transaction alive. If we know
								 * for a fact that we had to wait longer than the transaction timeout, then
								 * we don't need to keep trying--just roll back the transaction and return.
								 *
								 * However, we want to leave some room for network slop here, so we err
								 * on the side of caution, and only use this if we exceed twice the actual
								 * keep alive window. That way, we probably never need this, but it's available
								 * if we do.
								 */
                try{
                    txn.rollback();
                }catch(IOException e){
                    LOG.info("Unable to roll back transaction "+txn.getTxnId()
                            +" but nothing to be concerned with, since it has already timed out",e);
                }
                return;
            }

            try{
                long time=System.currentTimeMillis();
                boolean reschedule=txnStore.keepAlive(txn.getTxnId());
                time=System.currentTimeMillis()-time; //measure our latency
                if(reschedule){
                    //use a random slop factor to load-balance our keep alive requests.
                    threadPool.schedule(this,random.nextLong(maxWaitIntervalMs),TimeUnit.MILLISECONDS);
                    lastKeepAliveTime=System.currentTimeMillis(); //include network latency in our wait period
                }
                if(time>0.1*maxKeepAliveIntervalMs)
                    SpliceLogUtils.warn(LOG,"It took longer than 10% of the keep-alive interval to perform"+
                            "keep alive for transaction %d. This may be a sign that load will begin interfering"+
                            "with the transaction system",txn.getTxnId());
            }catch(HTransactionTimeout tte){
                LOG.error("Transaction "+txn.getTxnId()+" has timed out");
									/*
									 * We attempted to keep alive a transaction that has already timed out for a different
									 * reason. Ensure that the transaction is rolled back
									 */
                try{
                    txn.rollback();
                }catch(IOException e){
                    LOG.info("Unable to roll back transaction "+
                            txn.getTxnId()+" but nothing to be concerned with, since it has already timed out",e);
                }
            }catch(IOException e){
								/*
								 * This could be a real problem, but we don't have anything that we can really do about this,
								 * so we just log the error and hope it resolves itself.
								 */
                LOG.error("Unable to keep transaction "+txn.getTxnId()+" alive. Will try again in a bit",e);
                threadPool.schedule(this,random.nextLong(maxWaitIntervalMs),TimeUnit.MILLISECONDS);
            }
        }
    }
}
