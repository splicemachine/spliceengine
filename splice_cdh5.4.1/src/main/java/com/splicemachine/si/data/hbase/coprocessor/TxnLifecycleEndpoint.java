package com.splicemachine.si.data.hbase.coprocessor;

import com.google.common.base.Supplier;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.splicemachine.concurrent.CountedReference;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.constants.EnvUtils;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.txn.lifecycle.TxnLifecycleStore;
import com.splicemachine.si.api.txn.lifecycle.TxnPartition;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.impl.TransactionStorage;
import com.splicemachine.si.impl.data.StripedTxnLifecycleStore;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.region.RegionServerControl;
import com.splicemachine.si.impl.region.RegionTxnStore;
import com.splicemachine.si.impl.region.TransactionResolver;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.utils.Source;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 6/19/14
 */
public class TxnLifecycleEndpoint extends TxnMessage.TxnLifecycleService implements CoprocessorService, Coprocessor{
    private static final Logger LOG=Logger.getLogger(TxnLifecycleEndpoint.class);

    private TxnLifecycleStore lifecycleStore;
    private volatile boolean isTxnTable=false;

    public static final CountedReference<TransactionResolver> resolverRef=new CountedReference<>(new Supplier<TransactionResolver>(){
        @Override
        public TransactionResolver get(){
            return new TransactionResolver(TransactionStorage.getTxnSupplier(),2,128);
        }
    },new CountedReference.ShutdownAction<TransactionResolver>(){
        @Override
        public void shutdown(TransactionResolver instance){
            instance.shutdown();
        }
    });

    @Override
    public void start(CoprocessorEnvironment env){
        HRegion region=((RegionCoprocessorEnvironment)env).getRegion();
        SpliceConstants.TableEnv table=EnvUtils.getTableEnv((RegionCoprocessorEnvironment)env);
        if(table.equals(SpliceConstants.TableEnv.TRANSACTION_TABLE)){
            TransactionResolver resolver=resolverRef.get();
            TxnPartition regionStore=new RegionTxnStore(region,TransactionStorage.getTxnSupplier(),resolver,HDataLib.instance(),SIConstants.transactionKeepAliveInterval,new SystemClock());
            TimestampSource timestampSource=SIDriver.getTimestampSource();
            lifecycleStore = new StripedTxnLifecycleStore(SIConstants.transactionlockStripes,regionStore,new RegionServerControl(region),timestampSource);
            isTxnTable=true;
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env){
        SpliceLogUtils.info(LOG, "Shutting down TxnLifecycleEndpoint");
        if(isTxnTable) {
            resolverRef.release(true);
        }
    }

    @Override
    public Service getService(){
        SpliceLogUtils.trace(LOG,"Getting the TxnLifecycle Service");
        return this;
    }

    @Override
    public void beginTransaction(RpcController controller,TxnMessage.TxnInfo request,RpcCallback<TxnMessage.VoidResponse> done){
        try{
            lifecycleStore.beginTransaction(request);
            done.run(TxnMessage.VoidResponse.getDefaultInstance());
        }catch(IOException ioe){
            ResponseConverter.setControllerException(controller,ioe);
        }
    }

    @Override
    public void elevateTransaction(RpcController controller,TxnMessage.ElevateRequest request,RpcCallback<TxnMessage.VoidResponse> done){
        if(request.getNewDestinationTable()==null){
            LOG.warn("Attempting to elevate a transaction with no destination table. This is probably a waste of a network call");
            return;
        }
        try{
            lifecycleStore.elevateTransaction(request.getTxnId(),request.getNewDestinationTable().toByteArray());
            done.run(TxnMessage.VoidResponse.getDefaultInstance());
        }catch(IOException ioe){
            ResponseConverter.setControllerException(controller,ioe);
        }
    }

    @Override
    public void beginChildTransaction(RpcController controller,TxnMessage.CreateChildRequest request,RpcCallback<TxnMessage.Txn> done){

    }

    @Override
    public void lifecycleAction(RpcController controller,TxnMessage.TxnLifecycleMessage request,RpcCallback<TxnMessage.ActionResponse> done){
        try{
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
            }
            done.run(response);
        }catch(IOException ioe){
            ResponseConverter.setControllerException(controller,ioe);
        }
    }

    @Override
    public void getTransaction(RpcController controller,TxnMessage.TxnRequest request,RpcCallback<TxnMessage.Txn> done){
        try{
            long txnId=request.getTxnId();
            TxnMessage.Txn transaction = lifecycleStore.getTransaction(txnId);
            done.run(transaction);
        }catch(IOException ioe){
            ResponseConverter.setControllerException(controller,ioe);
        }
    }

    @Override
    public void getActiveTransactionIds(RpcController controller,TxnMessage.ActiveTxnRequest request,RpcCallback<TxnMessage.ActiveTxnIdResponse> done){
        long endTxnId=request.getEndTxnId();
        long startTxnId=request.getStartTxnId();
        try{
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
            ResponseConverter.setControllerException(controller,e);
        }
    }

    @Override
    public void getActiveTransactions(RpcController controller,TxnMessage.ActiveTxnRequest request,RpcCallback<TxnMessage.ActiveTxnResponse> done){
        long endTxnId=request.getEndTxnId();
        long startTxnId=request.getStartTxnId();
        try{
            Source<TxnMessage.Txn> activeTxns=lifecycleStore.getActiveTransactions(request.getDestinationTables().toByteArray(),startTxnId,endTxnId);
            TxnMessage.ActiveTxnResponse.Builder response=TxnMessage.ActiveTxnResponse.newBuilder();
            while(activeTxns.hasNext()){
                response.addTxns(activeTxns.next());
            }
            done.run(response.build());
        }catch(IOException e){
            ResponseConverter.setControllerException(controller,e);
        }

    }

    public long commit(long txnId) throws IOException{
        return lifecycleStore.commitTransaction(txnId);
    }

    public void rollback(long txnId) throws IOException{
        lifecycleStore.rollbackTransaction(txnId);
    }

    public boolean keepAlive(long txnId) throws IOException{
        return lifecycleStore.keepAlive(txnId);
    }

}