package com.splicemachine.si.impl;

import com.splicemachine.concurrent.LongStripedSynchronizer;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnAccess;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Uses an HRegion to access Txn information.
 *
 * Intended <em>only</em> to be used within a coprocessor on a
 * region of the Transaction table.
 *
 * @author Scott Fines
 * Date: 6/19/14
 */
public class RegionTxnAccess implements TxnAccess {
		/*
		 * The region in which to access data
		 */
		private final HRegion region;

		/*
		 * A Transaction Store for fetching parent transactions, etc. as needed.
		 *
		 * This store is usually a Lazy store that will return a lazily-looked up transaction,
		 * in case the transaction being accessed does not need any parent information.
		 */
		private final TxnAccess externalStore;

		private final TxnDecoder oldTransactionDecoder;
		private final TxnDecoder newTransactionDecoder;

		public RegionTxnAccess(HRegion region,
													 TxnAccess externalStore) {
				this.region = region;
				this.externalStore = externalStore;
				this.oldTransactionDecoder = new OldVersionTxnDecoder();
				this.newTransactionDecoder = new NewVersionTxnDecoder();
		}

		@Override public boolean transactionCached(long txnId) { return false; } //this implementation does not have a local cache

		@Override
		public void cache(Txn toCache) {
				//no -op
		}

		@Override
		public Txn getTransaction(long txnId) throws IOException {
				Get get = new Get(TxnUtils.getRowKey(txnId));
				Result result = region.get(get);
				assert result!=null: "Attempted to retrieve a transaction that does not exist. " +
								"Probably a read-only transaction: "+ txnId;

						/*
						 * We want to support backwards-compatibility here, so we need to be able to read either format.
						 * If the data is encoded in the old format, read that. Otherwise, read the new format
						 */
				if(result.containsColumn(FAMILY,SIConstants.TRANSACTION_STATUS_COLUMN_BYTES)){
						//this transaction is encoded in the old way, so use the old decoder
						return oldTransactionDecoder.decode(txnId,result);
				}else
						return newTransactionDecoder.decode(txnId,result);
		}


		/**
		 * Update the Transaction's keepAlive field so that the transaction is known to still be active.
		 *
		 * This operation must occur under a lock to ensure that reads don't occur until after the transaction
		 * keep alive has been kept--otherwise, there is a race condition where some transactions may see
		 * a transaction as timed out, then have a keep alive come in and make it active again.
		 *
		 * @param txnId the transaction id to keep alive
		 * @throws IOException if something goes wrong, or if the keep alive comes after
		 * the timeout threshold
		 */
		public void keepAlive(long txnId) throws IOException{
				byte[] rowKey = TxnUtils.getRowKey(txnId);
				Get get = new Get(rowKey);
				get.addColumn(FAMILY,KEEP_ALIVE_QUALIFIER_BYTES);
				get.addColumn(FAMILY,SIConstants.TRANSACTION_KEEP_ALIVE_COLUMN_BYTES); //for backwards compatibility with pre-0.6 releases

				Result result = region.get(get);
				long lastKeepAliveTime;
				if(result.containsColumn(FAMILY,SIConstants.TRANSACTION_KEEP_ALIVE_COLUMN_BYTES)){
						KeyValue kv = result.getColumnLatest(FAMILY, SIConstants.TRANSACTION_KEEP_ALIVE_COLUMN_BYTES);
						lastKeepAliveTime = Bytes.toLong(kv.getBuffer(),kv.getValueOffset(),kv.getValueLength());
				}else{
						KeyValue kv = result.getColumnLatest(FAMILY, KEEP_ALIVE_QUALIFIER_BYTES);
						lastKeepAliveTime = Bytes.toLong(kv.getBuffer(),kv.getValueOffset(),kv.getValueLength());
				}

				long currTime = System.currentTimeMillis();
				if((currTime-lastKeepAliveTime)>TRANSACTION_TIMEOUT_WINDOW)
						throw new DoNotRetryIOException("Unable to perform Keep alive, transaction "+ txnId+" has already been timed out");

				byte[] kATime = Encoding.encode(System.currentTimeMillis());

				Put put = new Put(TxnUtils.getRowKey(txnId));
				put.add(FAMILY,KEEP_ALIVE_QUALIFIER_BYTES,kATime);

				region.put(put);
		}

		/**
		 * Change the state for the transaction, but only if it matches the previous state.
		 *
		 * It is assumed that this has been properly locked etc.
		 *
		 * @param txnId the transaction id to change state for
		 * @param newState the new state of the transaction
		 * @throws IOException if something goes wrong.
		 */
		public void setState(long txnId, Txn.State newState) throws IOException{
				byte[] rowKey = TxnUtils.getRowKey(txnId);

				Put put = new Put(rowKey);
				put.add(FAMILY,STATE_QUALIFIER_BYTES,newState.encode());

				region.put(put);
		}

		public long incrementCounter(long txnId) throws IOException{
				return region.incrementColumnValue(TxnUtils.getRowKey(txnId), FAMILY, COUNTER_QUALIFIER_BYTES, 1l, true);
		}

		/******************************************************************************************************************/
		/*private helper methods*/



		private static interface TxnDecoder{
				Txn decode(long txnId,Result result) throws IOException;
		}

		//easy reference for code clarity
		private static final byte[] FAMILY = SIConstants.DEFAULT_FAMILY_BYTES;

		private static final byte[] DATA_QUALIFIER_BYTES = Bytes.toBytes("d");
		private static final byte[] COUNTER_QUALIFIER_BYTES = Bytes.toBytes("c");
		private static final byte[] KEEP_ALIVE_QUALIFIER_BYTES = Bytes.toBytes("k");
		private static final byte[] COMMIT_QUALIFIER_BYTES = Bytes.toBytes("t");
		private static final byte[] GLOBAL_COMMIT_QUALIFIER_BYTES = Bytes.toBytes("g");
		private static final byte[] STATE_QUALIFIER_BYTES = Bytes.toBytes("s");
		private class NewVersionTxnDecoder implements TxnDecoder{
				/*
				 * Encodes transaction objects using the new, packed Encoding format
				 *
				 * The new way is a (more) compact representation which uses the Values CF (V) and compact qualifiers (using
				 * the Encoding.encodeX() methods) as follows:
				 *
				 * "d"	--	packed tuple of (beginTimestamp,parentTxnId,isDependent,additive,isolationLevel)
				 * "c"	--	counter (using a packed integer representation)
				 * "k"	--	keepAlive timestamp
				 * "t"	--	commit timestamp
				 * "g"	--	globalCommitTimestamp
				 * "s"	--	state
				 *
				 * The additional columns are kept separate so that they may be updated(and read) independently without
				 * reading and decoding the entire transaction.
				 *
				 * In the new format, if a transaction has been written to the table, then it automatically allows writes
				 */
				@Override
				public Txn decode(long txnId, Result result) throws IOException {
						KeyValue dataKv = result.getColumnLatest(FAMILY, DATA_QUALIFIER_BYTES);
						MultiFieldDecoder decoder = MultiFieldDecoder.wrap(dataKv.getBuffer(), dataKv.getValueOffset(), dataKv.getValueLength());
						long beginTs = decoder.decodeNextLong();
						long parentTxnId = -1l;
						if(!decoder.nextIsNull()) parentTxnId = decoder.decodeNextLong();
						else decoder.skip();

						boolean isDependent = false;
						boolean hasDependent = true;
						if(!decoder.nextIsNull())
								isDependent = decoder.decodeNextBoolean();
						else {
								hasDependent = false;
								decoder.skip();
						}

						boolean isAdditive = false;
						boolean hasAdditive = true;
						if(!decoder.nextIsNull())
								isAdditive = decoder.decodeNextBoolean();
						else {
								hasAdditive = false;
								decoder.skip();
						}
						Txn.IsolationLevel level = null;
						if(!decoder.nextIsNull())
								level = Txn.IsolationLevel.fromByte(decoder.decodeNextByte());
						else decoder.skip();

						KeyValue commitTsVal = result.getColumnLatest(FAMILY,COMMIT_QUALIFIER_BYTES);
						long commitTs = -1l;
						if(commitTsVal!=null)
								commitTs = Encoding.decodeLong(commitTsVal.getBuffer(),commitTsVal.getValueOffset(),false);
						else decoder.skip();

						KeyValue globalTsVal = result.getColumnLatest(FAMILY,GLOBAL_COMMIT_QUALIFIER_BYTES);
						long globalTs = -1l;
						if(globalTsVal!=null)
								globalTs = Encoding.decodeLong(globalTsVal.getBuffer(),globalTsVal.getValueOffset(),false);
						else decoder.skip();

						KeyValue stateKv = result.getColumnLatest(FAMILY,STATE_QUALIFIER_BYTES);
						Txn.State state = Txn.State.decode(stateKv.getBuffer(),stateKv.getValueOffset(),stateKv.getValueLength());

						if(state== Txn.State.ACTIVE){
								/*
								 * We need to check that the transaction hasn't been timed out (and therefore rolled back). This
								 * happens if the keepAliveTime is older than the configured transaction timeout. Of course,
								 * there is some network latency which could cause small keep alives to be problematic. To help out,
								 * we allow a little fudge factor in the timeout
								 */
								state = adjustStateForTimeout(state,result.getColumnLatest(FAMILY,KEEP_ALIVE_QUALIFIER_BYTES));
						}

						Txn parentTxn = parentTxnId>=0? externalStore.getTransaction(parentTxnId): Txn.ROOT_TRANSACTION;

						return new InheritingTxnView(parentTxn,txnId,beginTs,level,
										hasDependent,isDependent,hasAdditive,isAdditive,true,true,commitTs,globalTs,state);
				}

				private Txn.State adjustStateForTimeout(Txn.State currentState,KeyValue columnLatest) {
						long lastKATime = Encoding.decodeLong(columnLatest.getBuffer(), columnLatest.getValueOffset(), false);
						long currTime = System.currentTimeMillis();
						if((currTime-lastKATime)>TRANSACTION_TIMEOUT_WINDOW)
								return Txn.State.ROLLEDBACK; //time out the txn

						return currentState;
				}
		}

		private static final long TRANSACTION_TIMEOUT_WINDOW = SIConstants.transactionTimeout+1000;

		private class OldVersionTxnDecoder implements TxnDecoder{

				/*
				 * 1. The old way uses the Values CF (V) and integer qualifiers (Bytes.toBytes(int)) to encode data
				 * to the following columns:
				 * 	0 	--	beginTimestamp
				 *  1 	--	parentTxnId
				 *  2 	--	isDependent			(may not be present)
				 *  3 	--	allowWrites			(may not be present)
				 *  4 	--	readUncommitted	(may not be present)
				 *  5 	--	readCommitted		(may not be present)
				 *  6		--	status					(either ACTIVE, COMMITTED,ROLLED_BACK, or ERROR)
				 *  7		--	commitTimestamp	(may not be present)
				 *  8		--	keepAlive
				 *	14	--	transaction id
				 *	15	--	transaction counter
				 *	16	--	globalCommitTimestamp (may not be present)
				 *	17	--	additive				(may not be present)
				 */
				@Override
				public Txn decode(long txnId,Result result) throws IOException {
						KeyValue beginTsVal = result.getColumnLatest(FAMILY, SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES);
						long beginTs = Bytes.toLong(beginTsVal.getBuffer(),beginTsVal.getValueOffset(),beginTsVal.getValueLength());
						KeyValue parentTxnVal = result.getColumnLatest(FAMILY,SIConstants.TRANSACTION_PARENT_COLUMN_BYTES);
						long parentTs = -1l; //by default, the "root" transaction id is -1l
						if(parentTxnVal!=null)
								parentTs = Bytes.toLong(parentTxnVal.getBuffer(),parentTxnVal.getValueOffset(),parentTxnVal.getValueLength());
						boolean dependent = false;
						KeyValue dependentKv = result.getColumnLatest(FAMILY,SIConstants.TRANSACTION_DEPENDENT_COLUMN_BYTES);
						if(dependentKv!=null)
								dependent = BytesUtil.toBoolean(dependentKv.getBuffer(), dependentKv.getValueOffset());
						boolean additive = false;
						KeyValue additiveKv = result.getColumnLatest(FAMILY,SIConstants.TRANSACTION_ADDITIVE_COLUMN_BYTES);
						if(additiveKv!=null)
								additive = BytesUtil.toBoolean(additiveKv.getBuffer(),additiveKv.getValueOffset());
						boolean allowWrites = true; //if there's a record, then the transaction allows writes
						KeyValue awVal = result.getColumnLatest(FAMILY,SIConstants.TRANSACTION_ALLOW_WRITES_COLUMN_BYTES);
						if(awVal!=null)
								allowWrites = BytesUtil.toBoolean(awVal.getBuffer(),awVal.getValueOffset());
						boolean readUncommitted = false;
						KeyValue ruVal = result.getColumnLatest(FAMILY,SIConstants.TRANSACTION_READ_UNCOMMITTED_COLUMN_BYTES);
						if(ruVal!=null)
								readUncommitted = BytesUtil.toBoolean(ruVal.getBuffer(),ruVal.getValueOffset());
						boolean readCommitted = false;
						KeyValue rcVal = result.getColumnLatest(FAMILY,SIConstants.TRANSACTION_READ_UNCOMMITTED_COLUMN_BYTES);
						if(rcVal!=null)
								readCommitted = BytesUtil.toBoolean(rcVal.getBuffer(), rcVal.getValueOffset());
						KeyValue statusVal = result.getColumnLatest(FAMILY, SIConstants.TRANSACTION_STATUS_COLUMN_BYTES);
						Txn.State state = Txn.State.decode(statusVal.getBuffer(),statusVal.getValueOffset(),statusVal.getValueLength());
						long commitTs = -1l;
						KeyValue cTsVal = result.getColumnLatest(FAMILY,SIConstants.TRANSACTION_COMMIT_TIMESTAMP_COLUMN_BYTES);
						if(cTsVal!=null)
								commitTs = Bytes.toLong(cTsVal.getBuffer(),cTsVal.getValueOffset(),cTsVal.getValueLength());
						long globalCommitTs = -1l;
						KeyValue gTsVal = result.getColumnLatest(FAMILY,SIConstants.TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN_BYTES);
						if(gTsVal!=null)
								globalCommitTs = Bytes.toLong(gTsVal.getBuffer(),gTsVal.getValueOffset(),gTsVal.getValueLength());

						if(state== Txn.State.ACTIVE){
								/*
								 * We need to check that the transaction hasn't been timed out (and therefore rolled back). This
								 * happens if the keepAliveTime is older than the configured transaction timeout. Of course,
								 * there is some network latency which could cause small keep alives to be problematic. To help out,
								 * we allow a little fudge factor in the timeout
								 */
								state = adjustStateForTimeout(state,result.getColumnLatest(FAMILY,KEEP_ALIVE_QUALIFIER_BYTES));
						}

						//if we are a child transaction, look up the parent id for inherited values
						Txn parentTxn = parentTs>=0? externalStore.getTransaction(parentTs): Txn.ROOT_TRANSACTION;
						Txn.IsolationLevel level = null;
						if(rcVal!=null){
								if(readCommitted) level = Txn.IsolationLevel.READ_COMMITTED;
								else if(ruVal!=null && readUncommitted) level = Txn.IsolationLevel.READ_UNCOMMITTED;
						}else if(ruVal!=null && readUncommitted)
								level = Txn.IsolationLevel.READ_COMMITTED;

						return new InheritingTxnView(parentTxn,txnId,beginTs,level,
										dependentKv!=null,dependent,
										additiveKv!=null,additive,
										awVal!=null,allowWrites,
										commitTs,
										globalCommitTs,
										state);
				}

				private Txn.State adjustStateForTimeout(Txn.State currentState,KeyValue columnLatest) {
						long lastKATime = Bytes.toLong(columnLatest.getBuffer(),columnLatest.getValueOffset(),columnLatest.getValueLength());
						long currentTime = System.currentTimeMillis();

						long timeDiff = currentTime-lastKATime;
						if(timeDiff>TRANSACTION_TIMEOUT_WINDOW)
								return Txn.State.ROLLEDBACK; //roll back transactions which have timed out.
						else return currentState;
				}
		}

		public static void main(String...args) throws Exception{
				int stripes = 8192;
				long oldHeapTotal = Runtime.getRuntime().totalMemory();
				long oldHeapMax = Runtime.getRuntime().maxMemory();
				long oldHeapFree = Runtime.getRuntime().freeMemory();
				LongStripedSynchronizer<ReadWriteLock> lock
								= LongStripedSynchronizer.stripedReadWriteLock(stripes, false);
				long newHeapTotal = Runtime.getRuntime().totalMemory();
				long newHeapMax = Runtime.getRuntime().maxMemory();
				long newHeapFree = Runtime.getRuntime().freeMemory();
				System.out.printf("Total diff:%d%n",newHeapTotal-oldHeapTotal);
				System.out.printf("Max diff:%d%n",newHeapMax-oldHeapMax);
				long freeDiff = Math.abs(newHeapFree-oldHeapFree);
				System.out.printf("Free diff:%d%n",freeDiff);

				long bytesPerLock = freeDiff/stripes;
				System.out.printf("Bytes/Lock:%d%n",bytesPerLock);
		}
}
