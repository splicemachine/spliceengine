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

import com.splicemachine.si.api.txn.*;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.log4j.Logger;

import javax.management.*;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 *         Date: 11/21/16
 */
public class ConcurrentTxnRegistry implements TxnRegistry,TxnRegistry.TxnRegistryView{
    private final NavigableSet<Txn> activeTxns =new ConcurrentSkipListSet<>((Txn o1,Txn o2)->{
        if(o1==null){
            if(o2==null) return 0;
            return -1;
        }else if(o2==null) return 1;
        return Long.compare(o1.getBeginTimestamp(),o2.getBeginTimestamp());
    });
    private final TxnRegistryWatcher watcher = new DirectWatcher(this);

    @Override
    public int activeTxnCount(){
        return activeTxns.size();
    }

    @Override
    public long minimumActiveTransactionId(){
        try{
            Txn first=activeTxns.first();
            if(first==null) return 0L;
            return first.getBeginTimestamp();
        }catch(NoSuchElementException nsee){
            return 0L;
        }
    }

    @Override public TxnRegistryView asView(){ return this; }
    @Override public TxnRegistryWatcher watcher(){ return watcher; }

    @Override public int getActiveTxnCount(){ return activeTxnCount(); }
    @Override public long getMinimumActiveTransactionId(){ return minimumActiveTransactionId(); }

    @Override
    public void registerTxn(Txn txn){
        activeTxns.add(txn);
    }

    @Override
    public void deregisterTxn(Txn txn){
        activeTxns.remove(txn);
    }

    public void registerJmx(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException{
        ObjectName name = new ObjectName("com.splicemachine.si.txn:type=TxnRegistry.TxnRegistryView");
        mbs.registerMBean(this,name);
    }

    /* ********************************************************************************************/
    /*private helper methods*/
    private static class DirectWatcher implements TxnRegistryWatcher{
        private final ConcurrentTxnRegistry registry;

        DirectWatcher(ConcurrentTxnRegistry registry){
            this.registry=registry;
        }

        @Override public void start(){ }
        @Override public void shutdown(){ }

        @Override
        public TxnRegistryView currentView(boolean forceUpdate){
            return registry.asView(); //we are always up-to-date, so no need to force anything
        }

        @Override
        public void registerAction(long minTxnId,boolean requiresCommit,Runnable action){
            if(registry.minimumActiveTransactionId()>minTxnId){
                registry.performAction(minTxnId,requiresCommit,action);
            }else
                registry.register(minTxnId,requiresCommit,action);
        }
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

    private final Set<Action> actions =new ConcurrentSkipListSet<>();
    private ScheduledExecutorService actionPerformer;
    private void register(long minTxnId, boolean requiresCommit,Runnable action){
        actions.add(new Action(minTxnId,requiresCommit,action));
        if(actionPerformer==null){
            actionPerformer =Executors.newSingleThreadScheduledExecutor(r->{
                Thread t = new Thread(r);
                t.setName("ConcurrentTxnRegistry-actionWorker");
                t.setDaemon(true);
                return t;
            });
            actionPerformer.scheduleWithFixedDelay(()->{
                long currMin = minimumActiveTransactionId();
                actions.stream()
                        .filter(action1-> action1.txnId < currMin)
                        .forEach(action1->performAction(action1.txnId,action1.requiresCommit,action1.action));
            },1L,1L,TimeUnit.MINUTES);
        }
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
