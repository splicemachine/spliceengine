package com.splicemachine.si.coprocessors;

import com.google.common.base.Supplier;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.splicemachine.concurrent.CountedReference;
import com.splicemachine.concurrent.LongStripedSynchronizer;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.environment.EnvUtils;
import com.splicemachine.hbase.HBaseServerUtils;
import com.splicemachine.si.api.*;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.si.impl.TransactionStorage;
import com.splicemachine.si.impl.TransactionTimestamps;
import com.splicemachine.si.impl.region.RegionTxnStore;
import com.splicemachine.si.impl.region.TransactionResolver;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * @author Scott Fines
 * Date: 6/19/14
 */
public class TxnLifecycleEndpoint extends TxnMessage.TxnLifecycleService implements CoprocessorService,Coprocessor {
		private static final Logger LOG = Logger.getLogger(TxnLifecycleEndpoint.class);
		private LongStripedSynchronizer<ReadWriteLock> lockStriper;
		private RegionTxnStore regionStore;
		private HRegion region;
		private TimestampSource timestampSource;
    private volatile boolean isTxnTable = false;

    public static CountedReference<TransactionResolver> resolverRef = new CountedReference<>(new Supplier<TransactionResolver>() {
        @Override
        public TransactionResolver get() {
            return new TransactionResolver(TransactionStorage.getTxnSupplier(),2,128);
        }
    }, new CountedReference.ShutdownAction<TransactionResolver>() {
        @Override
        public void shutdown(TransactionResolver instance) {
            instance.shutdown();
        }
    });

    @Override
		public void start(CoprocessorEnvironment env) {
				region = ((RegionCoprocessorEnvironment)env).getRegion();
        SpliceConstants.TableEnv table = EnvUtils.getTableEnv((RegionCoprocessorEnvironment) env);
        if(table.equals(SpliceConstants.TableEnv.TRANSACTION_TABLE)){
            lockStriper = LongStripedSynchronizer.stripedReadWriteLock(SIConstants.transactionlockStripes,false);
            TransactionResolver resolver = resolverRef.get();
            regionStore = new RegionTxnStore(region,resolver,TransactionStorage.getTxnSupplier(),SIFactoryDriver.siFactory.getDataLib(),SIFactoryDriver.siFactory.getTransactionLib());
            timestampSource = TransactionTimestamps.getTimestampSource();
            isTxnTable = true;
        }
		}

		@Override
		public void stop(CoprocessorEnvironment env) {
        SpliceLogUtils.trace(LOG, "Shutting down TxnLifecycleEndpoint");
        if(isTxnTable)
            resolverRef.release(true);
		}

    @Override
    public Service getService() {
        SpliceLogUtils.trace(LOG, "Getting the TxnLifecycle Service");
        return this;
    }

    @Override
    public void beginTransaction(RpcController controller, TxnMessage.TxnInfo request, RpcCallback<TxnMessage.VoidResponse> done) {
        Lock lock = lockStriper.get(request.getTxnId()).writeLock();
        try{
            acquireLock(lock);
            try{
                regionStore.recordTransaction(request);
            }finally{
                unlock(lock);
            }
            done.run(TxnMessage.VoidResponse.getDefaultInstance());
        }catch(IOException ioe){
            ResponseConverter.setControllerException(controller, ioe);
        }
    }

    @Override
    public void elevateTransaction(RpcController controller, TxnMessage.ElevateRequest request, RpcCallback<TxnMessage.VoidResponse> done) {
        if(request.getNewDestinationTable()==null){
            LOG.warn("Attempting to elevate a transaction with no destination table. This is probably a waste of a network call");
            return;
        }
        Lock lock = lockStriper.get(request.getTxnId()).writeLock();
        try{
            acquireLock(lock);
            try{
                regionStore.addDestinationTable(request.getTxnId(), request.getNewDestinationTable().toByteArray());
            }finally{
                unlock(lock);
            }
            done.run(TxnMessage.VoidResponse.getDefaultInstance());
        }catch(IOException ioe){
            ResponseConverter.setControllerException(controller,ioe);
        }
    }

    @Override
    public void beginChildTransaction(RpcController controller, TxnMessage.CreateChildRequest request, RpcCallback<TxnMessage.Txn> done) {

    }

    @Override
    public void lifecycleAction(RpcController controller, TxnMessage.TxnLifecycleMessage request, RpcCallback<TxnMessage.ActionResponse> done) {
        try{
            TxnMessage.ActionResponse response = null;
            switch(request.getAction()){
                case COMMIT:
                    response = TxnMessage.ActionResponse.newBuilder().setCommitTs(commit(request.getTxnId())).build();
                    break;
                case TIMEOUT:
                case ROLLBACk:
                    rollback(request.getTxnId());
                    response = TxnMessage.ActionResponse.getDefaultInstance();
                    break;
                case KEEPALIVE:
                    boolean b = keepAlive(request.getTxnId());
                    response = TxnMessage.ActionResponse.newBuilder().setContinue(b).build();
                    break;
            }
            done.run(response);
        }catch(IOException ioe){
            ResponseConverter.setControllerException(controller,ioe);
        }
    }

    @Override
    public void getTransaction(RpcController controller, TxnMessage.TxnRequest request, RpcCallback<TxnMessage.Txn> done) {
        try{
            long txnId = request.getTxnId();
            Lock lock = lockStriper.get(txnId).readLock();
            acquireLock(lock);
            try{
                TxnMessage.Txn transaction = (TxnMessage.Txn) regionStore.getTransaction(txnId);
                done.run(transaction);
            }finally{
                unlock(lock);
            }
        }catch(IOException ioe){
            ResponseConverter.setControllerException(controller,ioe);
        }
    }

    @Override
    public void getActiveTransactionIds(RpcController controller, TxnMessage.ActiveTxnRequest request, RpcCallback<TxnMessage.ActiveTxnIdResponse> done) {
        long endTxnId = request.getEndTxnId();
        if(endTxnId<0)
            endTxnId = Long.MAX_VALUE;
        long startTxnId = request.getStartTxnId();
        try {
            byte[] destTables = null;
            if(request.hasDestinationTables())
                destTables = request.getDestinationTables().toByteArray();
            long[] activeTxnIds = regionStore.getActiveTxnIds(startTxnId, endTxnId, destTables);
            TxnMessage.ActiveTxnIdResponse.Builder response = TxnMessage.ActiveTxnIdResponse.newBuilder();
            for(int i=0;i<activeTxnIds.length;i++){
                response.addActiveTxnIds(activeTxnIds[i]);
            }
            done.run(response.build());
        } catch (IOException e) {
            ResponseConverter.setControllerException(controller,e);
        }
    }

    @Override
    public void getActiveTransactions(RpcController controller, TxnMessage.ActiveTxnRequest request, RpcCallback<TxnMessage.ActiveTxnResponse> done) {
        long endTxnId = request.getEndTxnId();
        if(endTxnId<0)
            endTxnId = Long.MAX_VALUE;
        long startTxnId = request.getStartTxnId();
        try {
            Source<TxnMessage.Txn> activeTxns = regionStore.getActiveTxns(startTxnId, endTxnId,request.getDestinationTables().toByteArray());
            TxnMessage.ActiveTxnResponse.Builder response = TxnMessage.ActiveTxnResponse.newBuilder();
            while(activeTxns.hasNext()){
                response.addTxns(activeTxns.next());
            }
            done.run(response.build());
        } catch (IOException e) {
            ResponseConverter.setControllerException(controller,e);
        }

    }

    public long commit(long txnId) throws IOException {
				Lock lock = lockStriper.get(txnId).writeLock();
				acquireLock(lock);
				try{
						Txn.State state = regionStore.getState(txnId);
						if(state==null){
								LOG.warn("Attempting to commit a read-only transaction. Waste of a network call");
								return -1l; //no need to acquire a new timestamp if we have a read-only transaction
						}
						if(state==Txn.State.COMMITTED){
								LOG.info("Attempting to commit an already committed transaction. Possibly this is a waste of a network call");
								return regionStore.getCommitTimestamp(txnId);
						}
						if(state==Txn.State.ROLLEDBACK)
								throw new CannotCommitException(txnId,state);

						long commitTs = timestampSource.nextTimestamp();
						regionStore.recordCommit(txnId, commitTs);
						return commitTs;
				}finally{
            unlock(lock);
        }
		}

		public void rollback(long txnId) throws IOException {
				Lock lock = lockStriper.get(txnId).writeLock();
				acquireLock(lock);
				try{
						Txn.State state = regionStore.getState(txnId);
						if(state==null){
								LOG.warn("Attempting to roll back a read-only transaction. Waste of a network call");
								return;
						}
						switch(state){
								case COMMITTED:
										LOG.info("Attempting to roll back a committed transaction. Possibly a waste of a network call");
										return;
								case ROLLEDBACK:
										LOG.info("Attempting to roll back an already rolled back transaction. Possibly a waste of a network call");
										return;
								default:
										regionStore.recordRollback(txnId);
						}
				}finally{
            unlock(lock);
        }
		}

		public boolean keepAlive(long txnId) throws IOException {
				Lock lock = lockStriper.get(txnId).writeLock();
				acquireLock(lock);
				try{
						return regionStore.keepAlive(txnId);
				}finally{
            unlock(lock);
        }
		}

    private void acquireLock(Lock lock) throws IOException {
				//make sure that the region doesn't close while we are working on it
				region.startRegionOperation();
				boolean shouldContinue=true;
				while(shouldContinue){
						try {
								shouldContinue = !lock.tryLock(200, TimeUnit.MILLISECONDS);
								try{
								    /*
								     * Checks if the client has disconnected while acquiring this lock.
								     * If it has, we need to ensure that our lock is released (if it has been
	   							   * acquired).
    								 */
                    HBaseServerUtils.checkCallerDisconnect(region, region.getRegionNameAsString());
								}catch(IOException ioe){
										if(!shouldContinue) //the lock was acquired, so it needs to be unlocked
                        unlock(lock);
                    throw ioe;
								}
						} catch (InterruptedException e) {
								LOG.warn("Interrupted while acquiring transaction lock. " +
												"Likely this is because HBase is shutting down");
								throw new IOException(e);
						}
				}
		}

    private void unlock(Lock lock) throws IOException {
        lock.unlock();
        region.closeRegionOperation();
    }

    public RegionTxnStore getRegionTxnStore() {
        return regionStore;
    }
}