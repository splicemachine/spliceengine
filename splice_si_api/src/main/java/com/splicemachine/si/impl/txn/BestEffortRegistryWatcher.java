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
 *
 */

package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.txn.TxnRegistryWatcher;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Uses concurrent scheduling to perform a "Best effort" registry watch.
 *
 * @author Scott Fines
 *         Date: 12/14/16
 */
public class BestEffortRegistryWatcher {
    private static final Logger LOG=Logger.getLogger(BestEffortRegistryWatcher.class);
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection") //it actually is updated, using the removeIf() call
    private final Set<Action> actions =new ConcurrentSkipListSet<>();
    private ScheduledExecutorService actionPerformer;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private final TxnRegistryWatcher registryWatcher;

    public BestEffortRegistryWatcher(TxnRegistryWatcher registry){
        this.registryWatcher=registry;
    }

    public void start(){
        if(!started.compareAndSet(false,true)) return;

        actionPerformer=Executors.newSingleThreadScheduledExecutor(r->{
            Thread t=new Thread(r);
            t.setName("ConcurrentTxnRegistry-actionWorker");
            t.setDaemon(true);
            return t;
        });
        actionPerformer.scheduleWithFixedDelay(()->{
            final long mat=getMAT();
            actions.removeIf(act->{
                if(mat>0 && act.txnId>=mat) return false;
                performAction(act.txnId,act.requiresCommit,act.action);
                return true;
            });
        },10L,10L,TimeUnit.SECONDS);

    }

    private long getMAT(){
        long currMin=0L;
        try{
            currMin=registryWatcher.currentView(false).getMinimumActiveTransactionId();
        }catch(IOException e){
            LOG.error("Unable to obtain MAT",e);
        }
        return currMin;
    }

    public void shutdown(){
        actionPerformer.shutdownNow();
    }

    public void registerAction(long minTxnId,boolean requiresCommit,Runnable action){
        if(getMAT()>minTxnId){
            performAction(minTxnId,requiresCommit,action);
        }else
            register(minTxnId, requiresCommit, action);
    }

    private static class Action implements Comparable<Action>{
        private static final AtomicLong seqGen = new AtomicLong(0L);
        private final long txnId;
        private final boolean requiresCommit;
        private final Runnable action;
        private final long seqId;

        public Action(long txnId,boolean requiresCommit,Runnable action){
            this.txnId=txnId;
            this.requiresCommit=requiresCommit;
            this.action=action;
            this.seqId = seqGen.incrementAndGet();
        }

        @Override
        public int compareTo(Action o){
            if(o==null) return 1;
            int c = Long.compare(txnId,o.txnId);
            if(c!=0) return c;
            else return Long.compare(seqId,o.seqId);
        }
    }

    private void register(long minTxnId, boolean requiresCommit,Runnable action){
        actions.add(new Action(minTxnId,requiresCommit,action));
    }

    private void performAction(long minTxnId, boolean requiredCommit,Runnable action){
        if(requiredCommit){
            SIDriver driver = SIDriver.driver();
            if(driver==null) return;

            try{
                TxnView txn=driver.getTxnSupplier().getTransaction(minTxnId);
                if(txn.getEffectiveCommitTimestamp()<0L){
                    //the transaction hasn't been fully committed, so discard the action
                    return;
                }
                action.run();
            }catch(IOException e){
                Logger.getLogger(ConcurrentTxnRegistry.class).warn("Unable to perform action ("+minTxnId+","+true+","+action+")",e);
            }
        }else{
            /*
             * we can perform the action right away, because we know the txn is
             * either rolled back or committed(since it's below the MAT).
             */
            action.run();
        }
    }
}
