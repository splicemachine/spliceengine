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

package com.splicemachine.si.data.hbase.coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.CountedReference;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.constants.EnvUtils;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.si.api.txn.lifecycle.TxnLifecycleStore;
import com.splicemachine.si.api.txn.lifecycle.TxnPartition;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.data.StripedTxnLifecycleStore;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.region.RegionServerControl;
import com.splicemachine.si.impl.region.RegionTxnStore;
import com.splicemachine.si.impl.region.TransactionResolver;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.Source;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.RpcUtils;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import splice.com.google.common.base.Supplier;
import splice.com.google.common.collect.Lists;
import splice.com.google.common.primitives.Longs;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 6/19/14
 */
public class TxnLifecycleEndpoint extends TxnMessage.TxnLifecycleService implements RegionCoprocessor {
    private static final Logger LOG=Logger.getLogger(TxnLifecycleEndpoint.class);
    private TxnLifecycleStore lifecycleStore;
    private volatile boolean isTxnTable=false;

    public static final CountedReference<TransactionResolver> resolverRef=new CountedReference<>(new Supplier<TransactionResolver>(){
        @Override
        public TransactionResolver get(){
            return new TransactionResolver(SIDriver.driver().getTxnSupplier(),2,128);
        }
    }, new CountedReference.ShutdownAction<TransactionResolver>(){
        @Override
        public void shutdown(TransactionResolver instance){
            instance.shutdown();
        }
    });

    @Override
    public void start(CoprocessorEnvironment env) throws IOException{
        try {
            RegionCoprocessorEnvironment rce=(RegionCoprocessorEnvironment)env;
            HRegion region=(HRegion)rce.getRegion();
            HBaseSIEnvironment siEnv = HBaseSIEnvironment.loadEnvironment(new SystemClock(), ZkUtils.getRecoverableZooKeeper());
            SConfiguration configuration=siEnv.configuration();
            TableType table= EnvUtils.getTableType(configuration,rce);
            if(table.equals(TableType.TRANSACTION_TABLE)){
                TransactionResolver resolver=resolverRef.get();
                SIDriver driver=siEnv.getSIDriver();
                assert driver!=null:"SIDriver Cannot be null";
                long txnKeepAliveTimeout = configuration.getTransactionTimeout();
                @SuppressWarnings("unchecked") TxnPartition regionStore=new RegionTxnStore(region,
                        driver.getTxnSupplier(),
                        resolver,
                        txnKeepAliveTimeout,
                        new SystemClock());
                TimestampSource timestampSource=driver.getTimestampSource();
                int txnLockStrips = configuration.getTransactionLockStripes();
                lifecycleStore = new StripedTxnLifecycleStore(txnLockStrips,regionStore,
                        new RegionServerControl(region, (RegionServerServices)rce.getOnlineRegions()),timestampSource);
                isTxnTable=true;
            }
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        try {
            SpliceLogUtils.info(LOG, "Shutting down TxnLifecycleEndpoint");
            if(isTxnTable) {
                resolverRef.release(true);
            }
        } catch (Throwable t) {
            throw CoprocessorUtils.getIOException(t);
        }
    }


    @Override
    public Iterable<Service> getServices() {
        List<Service> services = Lists.newArrayList();
        services.add(this);
        return services;
    }

    @Override
    public void beginTransaction(RpcController controller, TxnMessage.TxnInfo request, RpcCallback<TxnMessage.VoidResponse> done){
        try (RpcUtils.RootEnv env = RpcUtils.getRootEnv()) {
            lifecycleStore.beginTransaction(request);
            done.run(TxnMessage.VoidResponse.getDefaultInstance());
        }catch(IOException ioe){
            setControllerException(controller,ioe);
        }
    }

    @Override
    public void elevateTransaction(RpcController controller,TxnMessage.ElevateRequest request,RpcCallback<TxnMessage.VoidResponse> done){
        if(request.getNewDestinationTable()==null){
            LOG.warn("Attempting to elevate a transaction with no destination table. This is probably a waste of a network call");
            return;
        }
        try (RpcUtils.RootEnv env = RpcUtils.getRootEnv()) {
            lifecycleStore.elevateTransaction(request.getTxnId(),request.getNewDestinationTable().toByteArray());
            done.run(TxnMessage.VoidResponse.getDefaultInstance());
        }catch(IOException ioe){
            setControllerException(controller,ioe);
        }
    }

    @Override
    public void beginChildTransaction(RpcController controller,TxnMessage.CreateChildRequest request,RpcCallback<TxnMessage.Txn> done){

    }

    @Override
    @SuppressFBWarnings(value = "SF_SWITCH_NO_DEFAULT",justification = "Intentional")
    public void lifecycleAction(RpcController controller,TxnMessage.TxnLifecycleMessage request,RpcCallback<TxnMessage.ActionResponse> done){
        try (RpcUtils.RootEnv env = RpcUtils.getRootEnv()) {
            TxnMessage.ActionResponse response=null;
            switch(request.getAction()){
                case COMMIT:
                    response=TxnMessage.ActionResponse.newBuilder().setCommitTs(commit(request.getTxnId())).build();
                    break;
                case TIMEOUT:
                case ROLLBACk:
                    rollback(request.getTxnId());
                    response=TxnMessage.ActionResponse.getDefaultInstance();
                    break;
                case KEEPALIVE:
                    boolean b=keepAlive(request.getTxnId());
                    response=TxnMessage.ActionResponse.newBuilder().setContinue(b).build();
                    break;
                case ROLLBACK_SUBTRANSACTIONS:
                    long[] ids = Longs.toArray(request.getRolledbackSubTxnsList());
                    rollbackSubtransactions(request.getTxnId(), ids);
                    response=TxnMessage.ActionResponse.getDefaultInstance();
                    break;
            }
            done.run(response);
        }catch(IOException ioe){
            setControllerException(controller,ioe);
        }
    }

    @Override
    public void getTransaction(RpcController controller,TxnMessage.TxnRequest request,RpcCallback<TxnMessage.Txn> done){
        try (RpcUtils.RootEnv env = RpcUtils.getRootEnv()) {
            long txnId=request.getTxnId();
            boolean isOld = request.hasIsOld() && request.getIsOld();
            TxnMessage.Txn transaction;
            if (isOld) {
                transaction = lifecycleStore.getOldTransaction(txnId);
            } else {
                transaction = lifecycleStore.getTransaction(txnId);
            }
            done.run(transaction);
        }catch(IOException ioe){
            setControllerException(controller,ioe);
        }
    }

    @Override
    public void getTaskId(RpcController controller,TxnMessage.TxnRequest request,RpcCallback<TxnMessage.TaskId> done){
        try (RpcUtils.RootEnv env = RpcUtils.getRootEnv()) {
            long txnId=request.getTxnId();
            TxnMessage.TaskId taskId = lifecycleStore.getTaskId(txnId);
            done.run(taskId);
        }catch(IOException ioe){
            setControllerException(controller,ioe);
        }
    }

    @Override
    public void getActiveTransactionIds(RpcController controller,TxnMessage.ActiveTxnRequest request,RpcCallback<TxnMessage.ActiveTxnIdResponse> done){
        long endTxnId=request.getEndTxnId();
        long startTxnId=request.getStartTxnId();
        try (RpcUtils.RootEnv env = RpcUtils.getRootEnv()) {
            byte[] destTables=null;
            if(request.hasDestinationTables())
                destTables=request.getDestinationTables().toByteArray();
            long[] activeTxnIds=lifecycleStore.getActiveTransactionIds(destTables,startTxnId,endTxnId);
            TxnMessage.ActiveTxnIdResponse.Builder response=TxnMessage.ActiveTxnIdResponse.newBuilder();
            //noinspection ForLoopReplaceableByForEach
            for(int i=0;i<activeTxnIds.length;i++){
                response.addActiveTxnIds(activeTxnIds[i]);
            }
            done.run(response.build());
        }catch(IOException e){
            setControllerException(controller,e);
        }
    }

    public static void setControllerException(RpcController controller, IOException ioe) {
        if (controller != null) {
            if (controller instanceof ServerRpcController) {
                ((ServerRpcController)controller).setFailedOn(ioe);
            } else {
                controller.setFailed(StringUtils.stringifyException(ioe));
            }
        }
    }

    @Override
    public void getActiveTransactions(RpcController controller,TxnMessage.ActiveTxnRequest request,RpcCallback<TxnMessage.ActiveTxnResponse> done){
        long endTxnId=request.getEndTxnId();
        long startTxnId=request.getStartTxnId();
        try (RpcUtils.RootEnv env = RpcUtils.getRootEnv()) {
            byte[] destTables=null;
            if (request.hasDestinationTables()) {
                destTables = request.getDestinationTables().toByteArray();
            }
            TxnMessage.ActiveTxnResponse.Builder response = TxnMessage.ActiveTxnResponse.newBuilder();
            try (Source<TxnMessage.Txn> activeTxns = lifecycleStore.getActiveTransactions(destTables, startTxnId, endTxnId)) {
                while (activeTxns.hasNext()) {
                    response.addTxns(activeTxns.next());
                }
            }
            done.run(response.build());
        }catch(IOException e){
            setControllerException(controller,e);
        }

    }

    public long commit(long txnId) throws IOException{
        try (RpcUtils.RootEnv env = RpcUtils.getRootEnv()) {
            return lifecycleStore.commitTransaction(txnId);
        }
    }

    public void rollbackSubtransactions(long txnId, long[] subIds) throws IOException {
        try (RpcUtils.RootEnv env = RpcUtils.getRootEnv()) {
            lifecycleStore.rollbackSubtransactions(txnId, subIds);
        }
    }

    public void rollback(long txnId) throws IOException{
        try (RpcUtils.RootEnv env = RpcUtils.getRootEnv()) {
            lifecycleStore.rollbackTransaction(txnId);
        }
    }

    public boolean keepAlive(long txnId) throws IOException{
        try (RpcUtils.RootEnv env = RpcUtils.getRootEnv()) {
            return lifecycleStore.keepAlive(txnId);
        }
    }

    @Override
    public void rollbackTransactionsAfter(RpcController controller, TxnMessage.TxnRequest request, RpcCallback<TxnMessage.VoidResponse> done) {
        try (RpcUtils.RootEnv env = RpcUtils.getRootEnv()) {
            lifecycleStore.rollbackTransactionsAfter(request.getTxnId());
            done.run(TxnMessage.VoidResponse.getDefaultInstance());
        }catch(IOException ioe){
            setControllerException(controller,ioe);
        }
    }

    @Override
    public void getTxnAt(
            com.google.protobuf.RpcController controller,
            com.splicemachine.si.coprocessor.TxnMessage.TxnAtRequest request,
            com.google.protobuf.RpcCallback<com.splicemachine.si.coprocessor.TxnMessage.TxnAtResponse> done){
        try (RpcUtils.RootEnv ignored = RpcUtils.getRootEnv()) {
            TxnMessage.TxnAtResponse.Builder response = TxnMessage.TxnAtResponse.newBuilder();
            Pair<Long, Long> result = lifecycleStore.getTxnAt(request.getTs());
            response.setTxnId(result.getFirst());
            response.setTs(result.getSecond());
            done.run(response.build());
        }catch(IOException ioe){
            setControllerException(controller,ioe);
        }
    }
}
