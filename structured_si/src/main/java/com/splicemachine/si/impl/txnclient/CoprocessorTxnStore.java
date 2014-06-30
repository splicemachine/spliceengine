package com.splicemachine.si.impl.txnclient;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnAccess;
import com.splicemachine.si.api.TxnStore;
import com.splicemachine.si.coprocessors.TxnLifecycleProtocol;
import com.splicemachine.si.impl.InheritingTxnView;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.utils.ThreadSafe;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

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
		private TxnAccess cache; //a transaction store which uses a global cache for us
		@ThreadSafe private final  TimestampSource timestampSource;

		public CoprocessorTxnStore(HTableInterfaceFactory tableFactory,
															 TimestampSource timestampSource,
															 @ThreadSafe TxnAccess txnCache) {
				this.tableFactory = tableFactory;
				if(txnCache==null)
						this.cache = this; //set itself to be the cache--not actually a cache, but just in case
				else
						this.cache = txnCache;
				this.timestampSource = timestampSource;
		}

		@Override
		public void recordNewTransaction(Txn txn) throws IOException {
				HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config, SIConstants.TRANSACTION_TABLE_BYTES);
				try{
						byte[] rowKey = TxnUtils.getRowKey(txn.getTxnId());
						TxnLifecycleProtocol txnLifecycleProtocol = table.coprocessorProxy(TxnLifecycleProtocol.class, rowKey);
						txnLifecycleProtocol.recordTransaction(txn.getTxnId(),encode(txn));
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
				return getActiveTransactions(timestampSource.retrieveTimestamp(),txn.getTxnId(),table);
		}

		@Override
		public long[] getActiveTransactions(final long minTxnId, final long maxTxnId, final byte[] writeTable) throws IOException {
				HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config,SIConstants.TRANSACTION_TABLE_BYTES);
				try{
						Map<byte[],byte[]> data = table.coprocessorExec(TxnLifecycleProtocol.class, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, new Batch.Call<TxnLifecycleProtocol, byte[]>() {
								@Override
								public byte[] call(TxnLifecycleProtocol instance) throws IOException {
										return instance.getActiveTransactions(minTxnId, maxTxnId, writeTable);
								}
						});

						LongOpenHashSet txns = LongOpenHashSet.newInstance(); //TODO -sf- do we really need to check for duplicates? In case of Transaction table splits?
						MultiFieldDecoder decoder = MultiFieldDecoder.create();
						for(byte[] packed:data.values()){
								decoder.set(packed);
								while(decoder.available())
										txns.add(decoder.decodeNextLong());
						}
						long[] finalTxns = txns.toArray();
						Arrays.sort(finalTxns);
						return finalTxns;

				} catch (Throwable throwable) {
						throw new IOException(throwable);
				} finally{
						table.close();
				}
		}

		@Override
		public Txn getTransaction(long txnId) throws IOException {
			return getTransaction(txnId,false);
		}

		@Override
		public Txn getTransaction(long txnId, boolean getDestinationTables) throws IOException {
				HTableInterface table =
								tableFactory.createHTableInterface(SpliceConstants.config,
												SIConstants.TRANSACTION_TABLE_BYTES);
				try{
						byte[] rowKey = TxnUtils.getRowKey(txnId);
						TxnLifecycleProtocol txnLifecycleProtocol = table.coprocessorProxy(TxnLifecycleProtocol.class, rowKey);

						byte[] transaction = txnLifecycleProtocol.getTransaction(txnId, getDestinationTables);
						if(transaction==null||transaction.length<=0) return null; //no transaction found
						return decode(transaction);
				}finally{
						table.close();
				}
		}

		@Override public boolean transactionCached(long txnId) { return false; }

		@Override public void cache(Txn toCache) {  }

	/******************************************************************************************************************/
		/*private helper methods*/
		private Txn decode(byte[] txnPackedBytes) throws IOException {
				assert txnPackedBytes.length>0: "No transaction found";
				MultiFieldDecoder decoder = MultiFieldDecoder.wrap(txnPackedBytes);
				long txnId = decoder.decodeNextLong();
				long parentTxnId = -1l;
				if(decoder.nextIsNull()) decoder.skip();
				else parentTxnId = decoder.decodeNextLong();

				long beginTs = decoder.decodeNextLong();

				Txn.IsolationLevel isolationLevel = null;
				if(decoder.nextIsNull()) decoder.skip();
				else isolationLevel = Txn.IsolationLevel.fromByte(decoder.decodeNextByte());

				boolean hasDependent = false;
				boolean dependent = false;
				if(decoder.nextIsNull()) decoder.skip();
				else{
						hasDependent = true;
						dependent = decoder.decodeNextBoolean();
				}

				boolean hasAdditive = false;
				boolean additive = false;
				if(decoder.nextIsNull()) decoder.skip();
				else{
						hasAdditive = true;
						additive = decoder.decodeNextBoolean();
				}

				long commitTs = -1l;
				long globalCommitTs = -1l;
				if(decoder.nextIsNull()) decoder.skip();
				else commitTs = decoder.decodeNextLong();

				if(decoder.nextIsNull()) decoder.skip();
				else globalCommitTs = decoder.decodeNextLong();

				Txn.State state = Txn.State.fromByte(decoder.decodeNextByte());

				Collection<byte[]> destinationTables;
				if(decoder.available()){
						destinationTables = Lists.newArrayList();
						while(decoder.available()){
							destinationTables.add(decoder.decodeNextBytesUnsorted());
						}
				}else
						destinationTables = Collections.emptyList();

				Txn parentTxn = cache.getTransaction(parentTxnId);
				return new InheritingTxnView(parentTxn,txnId,beginTs,
								isolationLevel,
								hasDependent,dependent,
								hasAdditive,additive,
								true,true,
								commitTs,globalCommitTs,
								state,destinationTables);
		}

		private byte[] encode(Txn txn) {
				Collection<byte[]> destinationTables = txn.getDestinationTables();
				MultiFieldEncoder encoder = MultiFieldEncoder.create(9+destinationTables.size());
				encoder.encodeNext(txn.getTxnId());

				Txn parentTxn = txn.getParentTransaction();
				if(parentTxn!=null &&parentTxn.getTxnId()>=0)
						encoder = encoder.encodeNext(parentTxn.getTxnId());
				else encoder.encodeEmpty();

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
						//record the effective commit timestamp as the global commit timestamp for efficiency
						encoder = encoder.encodeNext(txn.getEffectiveCommitTimestamp());
				}else
						encoder.encodeEmpty();

				encoder.encodeNext(txn.getState().getId());

				//encode the destination tables
				for(byte[] destTable:destinationTables){
						encoder = encoder.encodeNextUnsorted(destTable);
				}

				return encoder.build();
		}

		private byte[] getTransactionRowKey(long txnId) {
				byte[] newRowKey = new byte[10];
				newRowKey[0] = (byte)(txnId & (SpliceConstants.TRANSACTION_TABLE_BUCKET_COUNT-1)); //assign the bucket
				BytesUtil.longToBytes(txnId, newRowKey, 2);
				return newRowKey;
		}

		public void setCache(TxnAccess cacheStore) {
			this.cache = cacheStore;
		}
}
