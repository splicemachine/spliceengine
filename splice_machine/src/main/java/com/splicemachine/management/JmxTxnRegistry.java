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

package com.splicemachine.management;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.hbase.jmx.JMXUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.TxnRegistry;
import com.splicemachine.si.api.txn.TxnRegistryWatcher;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.txn.BestEffortRegistryWatcher;
import com.splicemachine.storage.PartitionServer;
import org.apache.log4j.Logger;
import org.spark_project.guava.util.concurrent.ThreadFactoryBuilder;

import javax.management.MalformedObjectNameException;
import javax.management.remote.JMXConnector;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Uses a background thread to periodically contact JMX to get the latest
 * TxnRegistry information.
 *
 * @author Scott Fines
 *         Date: 11/29/16
 */
class JmxTxnRegistry implements TxnRegistryWatcher{
    private static final Logger LOGGER=Logger.getLogger(JmxTxnRegistry.class);
    private final AtomicReference<TxnRegistry.TxnRegistryView> currentView = new AtomicReference<>(null);

    private final Callable<TxnRegistry.TxnRegistryView> updateCallable = new JmxUpdateCallable();
    private final AtomicReference<FutureTask<TxnRegistry.TxnRegistryView>> updateFutureRef = new AtomicReference<>(null);

    private final ScheduledExecutorService backgroundUpdater;
    private final long updateTimeMillis;
    private final BestEffortRegistryWatcher actionHolder;

    JmxTxnRegistry(long updateTimeMillis){
        this.updateTimeMillis=updateTimeMillis;
        ThreadFactory tf=new ThreadFactoryBuilder().setDaemon(true).setNameFormat("txnRegistryWatcher").build();

        this.backgroundUpdater =Executors.newSingleThreadScheduledExecutor(tf);
        this.actionHolder = new BestEffortRegistryWatcher(this);
    }

    @Override
    public void start(){
        backgroundUpdater.scheduleWithFixedDelay(new Updater(),updateTimeMillis,updateTimeMillis,TimeUnit.MILLISECONDS);
        actionHolder.start();
    }

    @Override
    public void shutdown(){
        backgroundUpdater.shutdownNow();
        actionHolder.shutdown();
    }

    @Override
    public TxnRegistry.TxnRegistryView currentView(boolean forceUpdate) throws IOException{
        TxnRegistry.TxnRegistryView view = forceUpdate? refreshView():currentView.get();
        if(view==null){
            view = refreshView();
        }
        return view;
    }

    @Override
    public void registerAction(long minTxnId,boolean requiresCommit,Runnable action){
        actionHolder.registerAction(minTxnId, requiresCommit, action);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    private TxnRegistry.TxnRegistryView refreshView() throws IOException{
        FutureTask<TxnRegistry.TxnRegistryView> uFuture;
        do{
            uFuture=updateFutureRef.get();
            if(uFuture!=null){
                try{
                   return uFuture.get();
                }catch(InterruptedException e){
                    Thread.currentThread().interrupt();
                    return null;
                }catch(ExecutionException e){
                    Throwable t=e.getCause();
                    if(t instanceof IOException) throw (IOException)t;
                    else throw new IOException(t);
                }
            }
            else{
                FutureTask<TxnRegistry.TxnRegistryView> fut = new FutureTask<>(updateCallable);
                if(updateFutureRef.compareAndSet(null,fut)){
                    uFuture = fut;
                    break;
                }
            }
        }while(true);

        uFuture.run();
        try{
            TxnRegistry.TxnRegistryView txnRegistryView=uFuture.get();
            currentView.set(txnRegistryView);
            updateFutureRef.set(null);
            return txnRegistryView;
        }catch(InterruptedException e){
            Thread.currentThread().interrupt();
            return null;
        }catch(ExecutionException e){
            Throwable t=e.getCause();
            if(t instanceof IOException) throw (IOException)t;
            else throw new IOException(t);
        }
    }

    private final class Updater implements Runnable{

        @Override
        public void run(){
            try{
                refreshView();
            }catch(IOException e){
                LOGGER.warn("Unable to refresh registry view",e);
            }
        }
    }

    private final class JmxUpdateCallable implements Callable<TxnRegistry.TxnRegistryView>{

        @Override
        public TxnRegistry.TxnRegistryView call() throws Exception{
            if(Thread.currentThread().isInterrupted()) return currentView.get();

            try(PartitionAdmin admin = SIDriver.driver().getTableFactory().getAdmin()){
                Collection<PartitionServer> partitionServers=admin.allServers();
                /*
                 * We have a list of servers. Ideally, we would be able to connect to all the servers. However,
                 * if one server is unavailable, we have to deal with that (otherwise, we may not be able to use this
                 * for critical "must-occur" situations like Compaction logic, etc). So we attempt to connect
                 * to each. If it connects, then we perform our logic; if it fails (after retries), then we assume
                 * that the minimum active transaction is 0 (effectively disabling any logic requiring an accurate MAT),
                 * and bail.
                 */
                long mat = 0L;
                int numActiveTxns = 0;
                for(PartitionServer pServer:partitionServers){
                    if(Thread.currentThread().isInterrupted()) return currentView.get();
                    int tryCount = 0;
                    IOException error = null;
                    int maxTries = SIDriver.driver().getConfiguration().getMaxRetries();
                    while(tryCount<maxTries){
                        try(JMXConnector jmxc=JMXUtils.getMBeanServerConnection(pServer.getHostname())){
                            TxnRegistry.TxnRegistryView txnRegistry=JMXUtils.getTxnRegistry(jmxc);
                            int activeTxnCount=txnRegistry.getActiveTxnCount();
                            long serverMat=txnRegistry.getMinimumActiveTransactionId();

                            /*
                             * if it gets to this point, then we are operating wholly on in-jvm data,
                             * and can safely modify our counters
                             */
                            numActiveTxns+=activeTxnCount;
                            if(mat>serverMat|| mat==0){
                                mat = serverMat;
                            }
                            break;
                        }catch(IOException ioe){
                            error=ioe;
                        }
                        tryCount++;
                    }
                    if(error!=null){
                        LOGGER.error("Unable to get TxnRegistry information for node "+pServer.getHostname(),error);
                    }
                    if(tryCount>=maxTries){
                    /*
                     * We couldn't contact a server within the retries allowed, so we have to bail out. In
                     * this case, we set the mat to 0, then abort early.
                     */
                        mat =0L;
                        break;
                    }
                }
                final int totalActive = numActiveTxns;
                final long finalMat = mat;
                return new TxnRegistry.TxnRegistryView(){
                    @Override public int getActiveTxnCount(){ return totalActive; }
                    @Override public long getMinimumActiveTransactionId(){ return finalMat; }
                };
            }catch(IOException |MalformedObjectNameException e){
                //malformed objects should never happen, but just in case
                throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
            }
        }
    }

}
