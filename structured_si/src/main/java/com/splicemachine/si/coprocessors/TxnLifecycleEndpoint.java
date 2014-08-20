package com.splicemachine.si.coprocessors;

import com.google.common.collect.Lists;
import com.splicemachine.concurrent.LongStripedSynchronizer;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.environment.EnvUtils;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.ThrowIfDisconnected;
import com.splicemachine.si.api.*;
import com.splicemachine.si.impl.DenseTxn;
import com.splicemachine.si.impl.region.RegionTxnStore;
import com.splicemachine.si.impl.SparseTxn;
import com.splicemachine.si.impl.region.TransactionResolver;
import com.splicemachine.utils.Source;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * @author Scott Fines
 * Date: 6/19/14
 */
public class TxnLifecycleEndpoint extends BaseEndpointCoprocessor implements TxnLifecycleProtocol {

		private static final Logger LOG = Logger.getLogger(TxnLifecycleEndpoint.class);

		private LongStripedSynchronizer<ReadWriteLock> lockStriper
						= LongStripedSynchronizer.stripedReadWriteLock(SIConstants.transactionlockStripes,false);

		private RegionTxnStore regionStore;
		private HRegion region;
		private TimestampSource timestampSource;

    private static volatile TransactionResolver txnResolver;
    @Override
		public void start(CoprocessorEnvironment env) {
				region = ((RegionCoprocessorEnvironment)env).getRegion();
        SpliceConstants.TableEnv table = EnvUtils.getTableEnv((RegionCoprocessorEnvironment)env);
        if(table.equals(SpliceConstants.TableEnv.TRANSACTION_TABLE)){
            TransactionResolver resolver = txnResolver;
            if(resolver==null){
                synchronized (LOG){
                    if(txnResolver!=null)
                        resolver = txnResolver;
                    else{
                        txnResolver = new TransactionResolver(TransactionStorage.getTxnSupplier(),2,128);
                        resolver = txnResolver;
                    }
                }
            }
            regionStore = new RegionTxnStore(region,resolver);
            timestampSource = TransactionTimestamps.getTimestampSource();
        }
		}

		@Override
		public void stop(CoprocessorEnvironment env) {
				super.stop(env);
		}

		@Override
		public void recordTransaction(long txnId, byte[] packedTxn) throws IOException {
				SparseTxn txn = SparseTxn.decodeFromNetwork(packedTxn);
				assert txn.getTxnId()==txnId: "Transaction Ids do not match"; //probably won't happen, but it catches programmer errors


				Lock lock = lockStriper.get(txnId).writeLock(); //use the write lock to record transactions
				acquireLock(lock);
				try{
					regionStore.recordTransaction(txn);
				}finally{
            unlock(lock);
        }
		}

    @Override
		public void elevateTransaction(long txnId, byte[] newDestinationTable) throws IOException {
				if(newDestinationTable==null){
						LOG.warn("Attempting to elevate a transaction with no destination table. This is probably a waste of a network call");
						return;
				}
				Lock lock = lockStriper.get(txnId).writeLock();
				acquireLock(lock);
				try{
						regionStore.addDestinationTable(txnId,newDestinationTable);
				}finally{
            unlock(lock);
        }
		}

		@Override
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

		@Override
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

		@Override
		public boolean keepAlive(long txnId) throws IOException {
				Lock lock = lockStriper.get(txnId).writeLock();
				acquireLock(lock);
				try{
						return regionStore.keepAlive(txnId);
				}finally{
            unlock(lock);
        }
		}

		@Override
		public byte[] getTransaction(long txnId, boolean getDestinationTables) throws IOException {
				Lock lock = lockStriper.get(txnId).readLock();
				acquireLock(lock);
				try{
						SparseTxn transaction = regionStore.getTransaction(txnId);
						return encodeForNetwork(transaction,getDestinationTables);
				}finally{
            unlock(lock);
        }
		}

		@Override
		public byte[] getActiveTransactionIds(long afterTs, long beforeTs, byte[] destinationTable) throws IOException {
				long[] activeTxnIds = regionStore.getActiveTxnIds(beforeTs,afterTs,destinationTable);
				MultiFieldEncoder encoder = MultiFieldEncoder.create(activeTxnIds.length);
				for(long activeTxnId:activeTxnIds){
						encoder.encodeNext(activeTxnId);
				}
				return encoder.build();
		}

    @Override
    public List<byte[]> getActiveTransactions(long afterTs, long beforeTs, byte[] destinationTable) throws IOException {
        Source<DenseTxn> activeTxns = regionStore.getActiveTxns(afterTs,beforeTs,destinationTable);
        List<byte[]> encodedData = Lists.newArrayList();
        MultiFieldEncoder txnEncoder = MultiFieldEncoder.create(11);
        while(activeTxns.hasNext()){
            DenseTxn txn = activeTxns.next();
            txnEncoder.reset();
            txn.encodeForNetwork(txnEncoder, true, true);
            encodedData.add(txnEncoder.build());
        }
        return encodedData;
    }

    private byte[] encodeForNetwork(SparseTxn transaction,boolean addDestinationTables) {
        if(transaction==null) return HConstants.EMPTY_BYTE_ARRAY;
        MultiFieldEncoder encoder = MultiFieldEncoder.create(addDestinationTables? 10:9);
        transaction.encodeForNetwork(encoder, false, addDestinationTables);

        return encoder.build();
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
										ThrowIfDisconnected.throwIfDisconnected throwIfDisconnected = ThrowIfDisconnected.getThrowIfDisconnected();
										String regionNameAsString = region.getRegionNameAsString();
										throwIfDisconnected.invoke(HBaseServer.getCurrentCall(), regionNameAsString);
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

    private void unlock(Lock lock) {
        lock.unlock();
        region.closeRegionOperation();
    }

    public RegionTxnStore getRegionTxnStore() {
        return regionStore;
    }
}
