package com.splicemachine.si.impl.txnclient;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnAccess;
import com.splicemachine.si.api.TxnStore;
import com.splicemachine.si.coprocessors.TxnLifecycleProtocol;
import com.splicemachine.utils.ThreadSafe;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;

import java.io.IOException;
import java.util.Collection;

/**
 * Transaction Store which uses the TxnLifecycleEndpoint to manage and access transactions
 * remotely.
 *
 * This class has no local cache. Callers are responsible for caching returned transactions
 * safely.
 *
 * @author Scott Fines
 * Date: 6/27/14
 */
@ThreadSafe
public class CoprocessorTxnStore implements TxnStore{
		private final HTableInterfaceFactory tableFactory;
		private final TxnAccess cache; //a transaction store which uses a global cache for us

		public CoprocessorTxnStore(HTableInterfaceFactory tableFactory,@ThreadSafe TxnAccess txnCache) {
				this.tableFactory = tableFactory;
				this.cache = txnCache;
		}

		@Override
		public void recordNewTransaction(Txn txn) throws IOException {
				HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config, SIConstants.TRANSACTION_TABLE_BYTES);
				try{
						byte[] rowKey = getTransactionRowKey(txn.getTxnId());
						TxnLifecycleProtocol txnLifecycleProtocol = table.coprocessorProxy(TxnLifecycleProtocol.class, rowKey);
						txnLifecycleProtocol.beginTransaction(txn.getTxnId(),encode(txn));
				}finally{
						table.close();
				}
		}


		@Override
		public void rollback(long txnId) throws IOException {
				HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config, SIConstants.TRANSACTION_TABLE_BYTES);
				try{
						byte[] rowKey = getTransactionRowKey(txnId);
						TxnLifecycleProtocol txnLifecycleProtocol = table.coprocessorProxy(TxnLifecycleProtocol.class, rowKey);
						txnLifecycleProtocol.rollback(txnId);
				}finally{
						table.close();
				}
		}

		@Override
		public long commit(long txnId) throws IOException {
				HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config, SIConstants.TRANSACTION_TABLE_BYTES);
				try{
						byte[] rowKey = getTransactionRowKey(txnId);
						TxnLifecycleProtocol txnLifecycleProtocol = table.coprocessorProxy(TxnLifecycleProtocol.class, rowKey);
						return txnLifecycleProtocol.commit(txnId);
				}finally{
						table.close();
				}
		}


		@Override
		public void elevateTransaction(Txn txn, byte[] newDestinationTable) throws IOException {
				HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config, SIConstants.TRANSACTION_TABLE_BYTES);
				try{
						byte[] rowKey = getTransactionRowKey(txn.getTxnId());
						TxnLifecycleProtocol txnLifecycleProtocol = table.coprocessorProxy(TxnLifecycleProtocol.class, rowKey);
						txnLifecycleProtocol.elevateTransaction(txn.getTxnId(),newDestinationTable);
				}finally{
						table.close();
				}
		}

		@Override
		public long[] getActiveTransactions(Txn txn, byte[] table) throws IOException {
				throw new UnsupportedOperationException("IMPLEMENT");
		}

		@Override
		public Txn getTransaction(long txnId) throws IOException {
				HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config, SIConstants.TRANSACTION_TABLE_BYTES);
				try{
						byte[] rowKey = getTransactionRowKey(txnId);
						TxnLifecycleProtocol txnLifecycleProtocol = table.coprocessorProxy(TxnLifecycleProtocol.class, rowKey);
						return decode(txnLifecycleProtocol.getTransaction(txnId));
				}finally{
						table.close();
				}
		}


		@Override public boolean transactionCached(long txnId) { return false; }

		@Override public void cache(Txn toCache) {  }

	/******************************************************************************************************************/
		/*private helper methods*/
		private Txn decode(byte[] txnPackedBytes) {
				throw new UnsupportedOperationException("IMPLEMENT");
		}

		private byte[] encode(Txn txn) {
				Collection<byte[]> destinationTables = txn.getDestinationTables();
				MultiFieldEncoder encoder = MultiFieldEncoder.create(9+destinationTables.size());
				encoder.encodeNext(txn.getTxnId());
				Txn parentTxn = txn.getParentTransaction();
				if(parentTxn!=null &&parentTxn.getTxnId()>=0)
						encoder = encoder.encodeNext(parentTxn.getTxnId());
				encoder.encodeNext(txn.getBeginTimestamp())
								.encodeNext(txn.getIsolationLevel().encode())
								.encodeNext(txn.isDependent())
								.encodeNext(txn.isAdditive());
				if(txn.getState()== Txn.State.COMMITTED){
						encoder = encoder.encodeNext(txn.getCommitTimestamp());
				}else{
						encoder.encodeEmpty();
				}
				if(txn.getEffectiveState()== Txn.State.COMMITTED){
						//record the effective commit timestamp as the global commit timestamp
						encoder = encoder.encodeNext(txn.getEffectiveCommitTimestamp());
				}else
						encoder.encodeEmpty();

				encoder.encodeNextUnsorted(txn.getState().encode());


				return encoder.build();

		}

		private byte[] getTransactionRowKey(long txnId) {
				throw new UnsupportedOperationException("IMPLEMENT!");
		}

}
