package com.splicemachine.si.impl.region;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.KeyValueUtils;
import com.splicemachine.hbase.RegionScanIterator;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.*;
import com.splicemachine.si.impl.DenseTxn;
import com.splicemachine.si.impl.SparseTxn;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.Source;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Uses an HRegion to access Txn information.
 *
 * Intended <em>only</em> to be used within a coprocessor on a
 * region of the Transaction table.
 *
 * @author Scott Fines
 * Date: 6/19/14
 */
public class RegionTxnStore {
		/*
		 * The region in which to access data
		 */
		private final HRegion region;

		private final TxnDecoder oldTransactionDecoder;
		private final NewVersionTxnDecoder newTransactionDecoder;

		public RegionTxnStore(HRegion region) {
				this.region = region;
				this.oldTransactionDecoder = new OldVersionTxnDecoder();
				this.newTransactionDecoder = new NewVersionTxnDecoder();
		}

		/**
		 * Fetch all information about a transaction.
		 *
		 * @param txnId the transaction id to fetch
		 * @return all recorded transaction information for the specified transaction.
		 * @throws IOException if something goes wrong when fetching transactions
		 */
		public SparseTxn getTransaction(long txnId) throws IOException {
				Get get = new Get(TxnUtils.getRowKey(txnId));
				Result result = region.get(get);
				if(result==null || result.size()<=0) return null; //no transaction

						/*
						 * We want to support backwards-compatibility here, so we need to be able to read either format.
						 * If the data is encoded in the old format, read that. Otherwise, read the new format
						 */
        SparseTxn txn = newTransactionDecoder.decode(txnId,result);
        if(txn==null)
            txn = oldTransactionDecoder.decode(txnId,result);
        return txn;
		}

		/**
		 * Add a destination table to the transaction.
		 *
		 * This method is <em>not</em> thread-safe--it <em>must</em> be synchronized externally
		 * in order to avoid race conditions and erroneous state conditions.
		 *
		 * @param txnId the transaction id to add a destination table to
		 * @param destinationTable the destination table to record
		 * @throws IOException if something goes wrong
		 * @throws com.splicemachine.si.api.ReadOnlyModificationException if the transaction to be updated
		 * was improperly elevated from a read-only transaction (e.g. if it's not present on the table).
		 */
		public void addDestinationTable(long txnId,byte[] destinationTable) throws IOException {
				Get get = new Get(TxnUtils.getRowKey(txnId));
				get.addColumn(FAMILY,DESTINATION_TABLE_QUALIFIER_BYTES);
				/*
				 * We only need to check the new transaction format, because we will never attempt to elevate
				 * a transaction created using the old transaction format.
				 */

				Result result = region.get(get);
				//should never happen, this is in place to protect against programmer error
				if(result==null)
						throw new ReadOnlyModificationException("Transaction "+ txnId+" is read-only, and was not properly elevated.");
				KeyValue kv = result.getColumnLatest(FAMILY, DESTINATION_TABLE_QUALIFIER_BYTES);
				byte[] newBytes;
				if(kv==null){
						/*
						 * this shouldn't happen, but you never know--someone might create a writable transaction
						 * without specifying a table directly. In that case, this will still work
						 */
						newBytes = destinationTable;
				}else{
						int valueLength = kv.getValueLength();
						newBytes = new byte[valueLength+destinationTable.length+1];
						System.arraycopy(kv.getBuffer(),kv.getValueOffset(),newBytes,0,valueLength);
						System.arraycopy(destinationTable,0,newBytes,valueLength+1,destinationTable.length);
				}
				Put put = new Put(get.getRow());
				put.add(FAMILY,DESTINATION_TABLE_QUALIFIER_BYTES,newBytes);
				region.put(put);
		}

		/**
		 * Update the Transaction's keepAlive field so that the transaction is known to still be active.
		 *
		 * This operation must occur under a lock to ensure that reads don't occur until after the transaction
		 * keep alive has been kept--otherwise, there is a race condition where some transactions may see
		 * a transaction as timed out, then have a keep alive come in and make it active again.
		 *
		 * @param txnId the transaction id to keep alive
		 * @return true if keep alives should continue (e.g. the transaction is still active)
		 * @throws IOException if something goes wrong, or if the keep alive comes after
		 * the timeout threshold
		 */
		public boolean keepAlive(long txnId) throws IOException{
				byte[] rowKey = TxnUtils.getRowKey(txnId);
				Get get = new Get(rowKey);
				get.addColumn(FAMILY,KEEP_ALIVE_QUALIFIER_BYTES);
				get.addColumn(FAMILY,STATE_QUALIFIER_BYTES);
				//we don't try to keep alive transactions with an old form

				Result result = region.get(get);
				if(result==null) return false; //attempted to keep alive a read-only transaction? a waste, but whatever

				KeyValue stateKv = result.getColumnLatest(FAMILY,STATE_QUALIFIER_BYTES);
				Txn.State state = Txn.State.decode(stateKv.getBuffer(), stateKv.getValueOffset(), stateKv.getValueLength());
				if(state != Txn.State.ACTIVE) return false; //skip the put if we don't need to do it

				KeyValue oldKAKV = result.getColumnLatest(FAMILY,KEEP_ALIVE_QUALIFIER_BYTES);
				long currTime = System.currentTimeMillis();
				Txn.State adjustedState = adjustStateForTimeout(state,oldKAKV,currTime,false);
				if(adjustedState!= Txn.State.ACTIVE)
						throw new TransactionTimeoutException(txnId);

				Put newPut = new Put(TxnUtils.getRowKey(txnId));
				newPut.add(FAMILY,KEEP_ALIVE_QUALIFIER_BYTES,Encoding.encode(currTime));
				region.put(newPut); //TODO -sf- does this work when the region is splitting?

				return true;
		}

		/**
		 * Gets the current state of the transaction.
		 *
		 * If the transaction has been manually set to either COMMITTED or ROLLEDBACK, then this will
		 * return that setting (e.g. COMMITTED or ROLLEDBACK). If the transaction is set to the ACTIVE
		 * state, then this will also check the keep alive timestamp. If the time since the last keep alive
		 * timestamp has exceeded the maximum window (some multiple of the configured setting, to allow for network latency),
		 * then this will "convert" the transaction to ROLLEDBACK--e.g. it will not write any data, but it will
		 * return a ROLLEDBACK state for the transaction instead of ACTIVE.
		 *
		 * @param txnId the transaction id to get state for
		 * @return the current state of this transaction, or {@code null} if the transaction is not listed (e.g. it's
		 * a Read-only transaction)
		 * @throws IOException if something goes wrong fetching the transaction
		 */
		public Txn.State getState(long txnId) throws IOException{
				byte[] rowKey = TxnUtils.getRowKey(txnId);
				Get get = new Get(rowKey);
				//add the columns for the new encoding
				get.addColumn(FAMILY,STATE_QUALIFIER_BYTES);
				get.addColumn(FAMILY,KEEP_ALIVE_QUALIFIER_BYTES);
				//add the columns for the old encoding
				get.addColumn(FAMILY,OLD_STATUS_COLUMN);
				get.addColumn(FAMILY,OLD_KEEP_ALIVE_COLUMN);

				Result result = region.get(get);
				if(result==null)
						return null; //indicates that the transaction was probably read only--external callers can figure out what that means

				boolean oldForm = false;
				KeyValue keepAliveKv;
				KeyValue stateKv = result.getColumnLatest(FAMILY,STATE_QUALIFIER_BYTES);
				if(stateKv==null){
						oldForm=true;
					//used the old encoding
						stateKv =result.getColumnLatest(FAMILY,OLD_STATUS_COLUMN);
						keepAliveKv = result.getColumnLatest(FAMILY,OLD_KEEP_ALIVE_COLUMN);
				}else{
						keepAliveKv = result.getColumnLatest(FAMILY,KEEP_ALIVE_QUALIFIER_BYTES);
				}

				Txn.State state = Txn.State.decode(stateKv.getBuffer(),stateKv.getValueOffset(),stateKv.getValueLength());
				if(state== Txn.State.ACTIVE)
						state = adjustStateForTimeout(state,keepAliveKv,oldForm);

				return state;
		}

		public long incrementCounter(long txnId) throws IOException{
				return region.incrementColumnValue(TxnUtils.getRowKey(txnId), FAMILY, COUNTER_QUALIFIER_BYTES, 1l, true);
		}

		/**
		 * Write the full transaction information to the local storage.
		 *
		 * @param txn the transaction to write
		 * @throws IOException if something goes wrong in writing the transaction
		 */
		public void recordTransaction(SparseTxn txn) throws IOException{
				Put put = newTransactionDecoder.encodeForPut(txn);
				region.put(put);
		}

		/**
		 * Record that the transaction was committed, and assign the committed timestamp to it.
		 *
		 * If this method returns successfully, then the transaction can safely be considered committed,
		 * even if a later network call fails and forces a retry.
		 *
		 * Calling this method twice will have no effect on correctness <em>as long as there are not concurrent
		 * rollbacks being called.</em>. As such it is vitally important that this method be called from within
		 * external synchronization.
		 *
		 * @param txnId the transaction id to commit
		 * @param commitTs the timestamp at which the commit is said to occur
		 * @throws IOException if something goes wrong while committing.
		 */
		public void recordCommit(long txnId, long commitTs) throws IOException{
				Put put = new Put(TxnUtils.getRowKey(txnId));
				put.add(FAMILY,COMMIT_QUALIFIER_BYTES,Encoding.encode(commitTs));
				put.add(FAMILY, STATE_QUALIFIER_BYTES, Txn.State.COMMITTED.encode());

				region.put(put);
		}

		/**
		 * Get the actual commit timestamp of the transaction (e.g. not it's effective timestamp), or {@code -1l}
		 * if the transaction is still considered active, or is a read-only transaction.
		 *
		 * @param txnId the transaction id to acquire the recorded commit timestamp.
		 * @return the commit timestamp for the specified transaction
		 * @throws IOException if something goes wrong during the fetch
		 */
		public long getCommitTimestamp(long txnId) throws IOException {
				Get get = new Get(TxnUtils.getRowKey(txnId));
				get.addColumn(FAMILY,COMMIT_QUALIFIER_BYTES);
				get.addColumn(FAMILY,OLD_COMMIT_TIMESTAMP_COLUMN);

				Result result = region.get(get);
				if(result==null) return -1l; //no commit timestamp for read-only transactions

				KeyValue kv;
				if((kv=result.getColumnLatest(FAMILY,COMMIT_QUALIFIER_BYTES))!=null)
						return Encoding.decodeLong(kv.getBuffer(),kv.getValueOffset(),false);
				else{
						kv = result.getColumnLatest(FAMILY,OLD_COMMIT_TIMESTAMP_COLUMN);
						return Bytes.toLong(kv.getBuffer(),kv.getValueOffset(),kv.getValueLength());
				}
		}

		/**
		 * Record the transaction as rolled back.
		 *
		 * This call is <em>not</em> thread-safe and it does <em>not</em> validate that the state of the
		 * transaction is active when it writes it--it's up to callers to ensure that that is true.
		 *
		 * However, once a call to this method is completed, then the transaction <em>must</em> be considered
		 * to be rolled back (and it will in any subsequent GETs).
		 *
		 * @param txnId the transaction id to roll back
		 * @throws IOException if something goes wrong during the write.
		 */
		public void recordRollback(long txnId) throws IOException {
				Put put = new Put(TxnUtils.getRowKey(txnId));
				put.add(FAMILY, STATE_QUALIFIER_BYTES, Txn.State.ROLLEDBACK.encode());
				region.put(put);
		}

		/**
		 * Get a list of transaction ids which are considered ACTIVE <em>at the time that they are visited</em>.
		 *
		 * This call does not require explicit synchronization, but it will <em>not</em> return a perfect view
		 * of the table at call time. It is possible for a transaction to be added after this scan has passed (resulting
		 * in missed active transactions); it is also possible for a transaction to be returned as ACTIVE but which
		 * has subsequently been moved to ROLLEDBACK or COMMITTED. It is thus imperative for callers to understand
		 * this requirements and operate accordingly (generally by using the transaction timestamps to prevent caring
		 * about transactions outside of a particular bound).
		 *
		 * @param beforeTs the upper limit on transaction timestamps to return. Transactions with a begin timestamp >=
		 *                 {@code beforeTs} will not be returned.
		 * @param afterTs the lower limit on transaction timestamps to return. Transactions with a begin
		 *                  timestamp < {@code afterTs} will not be returned.
		 * @param destinationTable the table which the transaction must have been writing to, or {@code null} if all
		 *                         active transactions in the range are to be returned.
		 * @return a listing of all active write transactions between {@code afterTs} and {@code beforeTs}, and which
		 * optionally write to {@code destinationTable}
		 * @throws IOException
		 */
		public long[] getActiveTxnIds(long beforeTs, long afterTs, byte[] destinationTable) throws IOException{
				Scan scan = new Scan(region.getStartKey(),region.getEndKey());
				scan.addColumn(FAMILY,STATE_QUALIFIER_BYTES);
				scan.addColumn(FAMILY,KEEP_ALIVE_QUALIFIER_BYTES);
				//add old columns for backwards compatibility
				scan.addColumn(FAMILY,OLD_STATUS_COLUMN);
				scan.addColumn(FAMILY,OLD_KEEP_ALIVE_COLUMN);
				scan.setMaxVersions(1);

				if(destinationTable!=null){
						scan.addColumn(FAMILY,DESTINATION_TABLE_QUALIFIER_BYTES);
						scan.addColumn(FAMILY,OLD_WRITE_TABLE_COLUMN);
				}

				scan.setFilter(new ActiveTxnFilter(beforeTs, afterTs, destinationTable));

				RegionScanner scanner = region.getScanner(scan);
				LongArrayList txnIds = new LongArrayList();
				List<KeyValue> keyValues = Lists.newArrayListWithExpectedSize(4);
				boolean shouldContinue;
				do{
						keyValues.clear();
						shouldContinue = scanner.nextRaw(keyValues,null);
						if(keyValues.size()<=0) {
								break;
						}
						KeyValue elem = keyValues.get(0);
						txnIds.add(TxnUtils.txnIdFromRowKey(elem.getBuffer(),elem.getRowOffset(),elem.getRowLength()));
				}while(shouldContinue);

				return txnIds.toArray();
		}

    public Source<DenseTxn> getActiveTxns(long afterTs,long beforeTs, byte[] destinationTable) throws IOException{
         /*
         * Get the bucket id for the region.
         *
         * The way the transaction table is built, a region may have an empty start
         * OR an empty end, but will never have both
         */
        byte[] regionKey = region.getStartKey();
        byte bucket;
        if(regionKey.length<=0)
            bucket = 0;
        else
            bucket = regionKey[0];
        byte[] startKey = BytesUtil.concat(Arrays.asList(new byte[]{bucket}, Bytes.toBytes(afterTs)));
        if(BytesUtil.startComparator.compare(region.getStartKey(),startKey)>0)
            startKey = region.getStartKey();
        byte[] stopKey = BytesUtil.concat(Arrays.asList(new byte[]{bucket}, Bytes.toBytes(beforeTs)));
        if(BytesUtil.endComparator.compare(region.getEndKey(),stopKey)<0)
            stopKey = region.getEndKey();
        Scan scan = new Scan(startKey,stopKey);
        scan.setMaxVersions(1);
        scan.setFilter(new ActiveTxnFilter(beforeTs,afterTs,destinationTable));

        final TxnSupplier store = TransactionStorage.getTxnSupplier();
        RegionScanner baseScanner = region.getScanner(scan);

        final RegionScanner scanner = new BufferedRegionScanner(region,baseScanner,scan,1024, Metrics.noOpMetricFactory());
        return new RegionScanIterator<DenseTxn>(scanner,new RegionScanIterator.IOFunction< DenseTxn>() {
            @Override
            public DenseTxn apply(@Nullable List<KeyValue> keyValues) throws IOException{
                DenseTxn txn = newTransactionDecoder.decode(keyValues);
                if(txn==null)
                    txn = oldTransactionDecoder.decode(keyValues);

                //this transaction should be returned because it's active
                if(txn.isDependent()){
                    long parentTxnId =txn.getParentTxnId();
                    if(parentTxnId>0){
                    /*
                     * We need to make sure that this transaction is actually active,
                     * in case the parent has committed/rolledback already
                     */
                        TxnView parentTxn = store.getTransaction(parentTxnId);
                        if(parentTxn.getEffectiveState()!= Txn.State.ACTIVE){
                            // we are not actually active
                            return null;
                        }
                    }
                }

                return txn;
            }
        });

    }

		/******************************************************************************************************************/
		/*private helper methods*/


		private static interface TxnDecoder{
				SparseTxn decode(long txnId,Result result) throws IOException;

        DenseTxn decode(List<KeyValue> keyValues) throws IOException;
		}

		//easy reference for code clarity
		private static final byte[] FAMILY = SIConstants.DEFAULT_FAMILY_BYTES;

		private static final byte[] DATA_QUALIFIER_BYTES = Bytes.toBytes("d");
		private static final byte[] COUNTER_QUALIFIER_BYTES = Bytes.toBytes("c");
		private static final byte[] KEEP_ALIVE_QUALIFIER_BYTES = Bytes.toBytes("k");
		private static final byte[] COMMIT_QUALIFIER_BYTES = Bytes.toBytes("t");
		private static final byte[] GLOBAL_COMMIT_QUALIFIER_BYTES = Bytes.toBytes("g");
		private static final byte[] STATE_QUALIFIER_BYTES = Bytes.toBytes("s");
		private static final byte[] DESTINATION_TABLE_QUALIFIER_BYTES = Bytes.toBytes("e"); //had to pick a letter that was unique

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
				 *
				 * order: c,d,e,g,k,s,t
				 * order: counter,data,destinationTable,globalCommitTimestamp,keepAlive,state,commitTimestamp,
				 */

				public Put encodeForPut(SparseTxn txn) throws IOException{
						Put put = new Put(TxnUtils.getRowKey(txn.getTxnId()));
						MultiFieldEncoder metaFieldEncoder = MultiFieldEncoder.create(5);
						metaFieldEncoder.encodeNext(txn.getBeginTimestamp()).encodeNext(txn.getParentTxnId());

						if(txn.hasDependentField())
								metaFieldEncoder.encodeNext(txn.isDependent());
						else
								metaFieldEncoder.encodeEmpty();
						if(txn.hasAdditiveField())
								metaFieldEncoder.encodeNext(txn.isAdditive());
						else
								metaFieldEncoder.encodeEmpty();

						Txn.IsolationLevel level = txn.getIsolationLevel();
						if(level!=null)
								metaFieldEncoder.encodeNext(level.encode());
						else metaFieldEncoder.encodeEmpty();

						put.add(FAMILY,DATA_QUALIFIER_BYTES,metaFieldEncoder.build());
						put.add(FAMILY,COUNTER_QUALIFIER_BYTES,Encoding.encode(0l));
						put.add(FAMILY,KEEP_ALIVE_QUALIFIER_BYTES,Encoding.encode(System.currentTimeMillis()));
						put.add(FAMILY,STATE_QUALIFIER_BYTES,txn.getState().encode());
						if(txn.getState()== Txn.State.COMMITTED){
								put.add(FAMILY,COMMIT_QUALIFIER_BYTES,Encoding.encode(txn.getCommitTimestamp()));
								long globalCommitTs = txn.getGlobalCommitTimestamp();
								if(globalCommitTs>=0)
										put.add(FAMILY,GLOBAL_COMMIT_QUALIFIER_BYTES,Encoding.encode(globalCommitTs));
						}
						ByteSlice destTableBuffer = txn.getDestinationTableBuffer();
						if(destTableBuffer!=null && destTableBuffer.length()>0)
								put.add(FAMILY,DESTINATION_TABLE_QUALIFIER_BYTES,destTableBuffer.getByteCopy());
						return put;
				}

        @Override
        public DenseTxn decode(List<KeyValue> keyValues) throws IOException {
            if(keyValues.size()<=0) return null;
            KeyValue dataKv = null;
            KeyValue keepAliveKv = null;
            KeyValue commitKv = null;
            KeyValue globalCommitKv = null;
            KeyValue stateKv = null;
            KeyValue destinationTables = null;

            for(KeyValue kv: keyValues){
                if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,DATA_QUALIFIER_BYTES))
                    dataKv = kv;
                else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,KEEP_ALIVE_QUALIFIER_BYTES))
                    keepAliveKv = kv;
                else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,GLOBAL_COMMIT_QUALIFIER_BYTES))
                    globalCommitKv = kv;
                else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,COMMIT_QUALIFIER_BYTES))
                    commitKv = kv;
                else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,STATE_QUALIFIER_BYTES))
                    stateKv = kv;
                else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,DESTINATION_TABLE_QUALIFIER_BYTES))
                    destinationTables = kv;
            }
            if(dataKv==null) return null;

            long txnId = TxnUtils.txnIdFromRowKey(dataKv.getBuffer(),dataKv.getRowOffset(),dataKv.getRowLength());

            return decodeInternal(dataKv, keepAliveKv, commitKv, globalCommitKv, stateKv, destinationTables, txnId);
        }

        @Override
        public SparseTxn decode(long txnId, Result result) throws IOException {
            KeyValue dataKv = result.getColumnLatest(FAMILY, DATA_QUALIFIER_BYTES);
            KeyValue commitTsVal = result.getColumnLatest(FAMILY,COMMIT_QUALIFIER_BYTES);
            KeyValue globalTsVal = result.getColumnLatest(FAMILY,GLOBAL_COMMIT_QUALIFIER_BYTES);
            KeyValue stateKv = result.getColumnLatest(FAMILY,STATE_QUALIFIER_BYTES);
            KeyValue destinationTables = result.getColumnLatest(FAMILY,DESTINATION_TABLE_QUALIFIER_BYTES);
            KeyValue kaTime = result.getColumnLatest(FAMILY, KEEP_ALIVE_QUALIFIER_BYTES);

            if(dataKv==null) return null;
            return decodeInternal(dataKv,kaTime,commitTsVal,globalTsVal,stateKv,destinationTables,txnId);
        }

        protected DenseTxn decodeInternal(KeyValue dataKv, KeyValue keepAliveKv, KeyValue commitKv, KeyValue globalCommitKv, KeyValue stateKv, KeyValue destinationTables, long txnId) {
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

            long commitTs = -1l;
            if(commitKv!=null)
                commitTs = Encoding.decodeLong(commitKv.getBuffer(), commitKv.getValueOffset(), false);

            long globalTs = -1l;
            if(globalCommitKv!=null)
                globalTs = Encoding.decodeLong(globalCommitKv.getBuffer(), globalCommitKv.getValueOffset(), false);
            else {
								/*
								 * for independent transactions, global commit = commit. As a result,
								 * by putting this if check in here, we make the global commit timestamp purely
								 * a performance enhancement to avoid parent transaction lookups, and we avoid
								 * requiring an extra get() during commit time( the extra get is necessary to
								 * determine if a transaction is dependent or not).
								 */
                if(hasDependent && !isDependent)
                    globalTs = commitTs; //for independent transactions, global commit = commit
            }

            Txn.State state = Txn.State.decode(stateKv.getBuffer(),stateKv.getValueOffset(),stateKv.getValueLength());


            if(state== Txn.State.ACTIVE){
								/*
								 * We need to check that the transaction hasn't been timed out (and therefore rolled back). This
								 * happens if the keepAliveTime is older than the configured transaction timeout. Of course,
								 * there is some network latency which could cause small keep alives to be problematic. To help out,
								 * we allow a little fudge factor in the timeout
								 */
                state = adjustStateForTimeout(state, keepAliveKv,false);
            }
            long kaTime = decodeKeepAlive(keepAliveKv,false);

            ByteSlice destTableBuffer = null;
            if(destinationTables!=null){
                destTableBuffer = new ByteSlice();
                destTableBuffer.set(destinationTables.getBuffer(),destinationTables.getValueOffset(),destinationTables.getValueLength());
            }

            return new DenseTxn(txnId,beginTs,parentTxnId,
                    commitTs,globalTs,hasDependent,isDependent,hasAdditive,isAdditive,level,state,destTableBuffer,kaTime);
        }

		}

		private static final long TRANSACTION_TIMEOUT_WINDOW = SIConstants.transactionTimeout+1000;

    private static final byte[] OLD_COMMIT_TIMESTAMP_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_COMMIT_TIMESTAMP_COLUMN);
    private static final byte[] OLD_START_TIMESTAMP_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_START_TIMESTAMP_COLUMN);
    private static final byte[] OLD_PARENT_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_PARENT_COLUMN);
    private static final byte[] OLD_DEPENDENT_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_DEPENDENT_COLUMN);
    private static final byte[] OLD_ALLOW_WRITES_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_ALLOW_WRITES_COLUMN);
    private static final byte[] OLD_ADDITIVE_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_ADDITIVE_COLUMN);
    private static final byte[] OLD_READ_UNCOMMITTED_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_READ_UNCOMMITTED_COLUMN);
    private static final byte[] OLD_READ_COMMITTED_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_READ_COMMITTED_COLUMN);
    private static final byte[] OLD_KEEP_ALIVE_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_KEEP_ALIVE_COLUMN);
    private static final byte[] OLD_STATUS_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_STATUS_COLUMN);
    private static final byte[] OLD_GLOBAL_COMMIT_TIMESTAMP_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN);
    private static final byte[] OLD_COUNTER_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_COUNTER_COLUMN);
    private static final byte[] OLD_WRITE_TABLE_COLUMN = Bytes.toBytes(SIConstants.WRITE_TABLE_COLUMN);
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
				public SparseTxn decode(long txnId,Result result) throws IOException {
						KeyValue beginTsVal = result.getColumnLatest(FAMILY,OLD_START_TIMESTAMP_COLUMN);
            KeyValue parentTxnVal = result.getColumnLatest(FAMILY,OLD_PARENT_COLUMN);
            KeyValue ruVal = result.getColumnLatest(FAMILY, OLD_READ_UNCOMMITTED_COLUMN);
            KeyValue rcVal = result.getColumnLatest(FAMILY,OLD_READ_COMMITTED_COLUMN);
            KeyValue statusVal = result.getColumnLatest(FAMILY, OLD_STATUS_COLUMN);
            KeyValue cTsVal = result.getColumnLatest(FAMILY,OLD_COMMIT_TIMESTAMP_COLUMN);
            KeyValue gTsVal = result.getColumnLatest(FAMILY,OLD_GLOBAL_COMMIT_TIMESTAMP_COLUMN);
            KeyValue kaTimeKv = result.getColumnLatest(FAMILY, OLD_KEEP_ALIVE_COLUMN);
            KeyValue destinationTables = result.getColumnLatest(FAMILY,OLD_WRITE_TABLE_COLUMN);
            KeyValue dependentKv = result.getColumnLatest(FAMILY, OLD_DEPENDENT_COLUMN);
            KeyValue additiveKv = result.getColumnLatest(FAMILY,OLD_ADDITIVE_COLUMN);

            return decodeInternal(txnId, beginTsVal, parentTxnVal, dependentKv,additiveKv,ruVal, rcVal, statusVal, cTsVal, gTsVal, kaTimeKv, destinationTables);
				}

        @Override
        public DenseTxn decode(List<KeyValue> keyValues) throws IOException {
            if(keyValues.size()<=0) return null;
            KeyValue beginTsVal = null;
            KeyValue parentTxnVal = null;
            KeyValue ruVal = null;
            KeyValue rcVal = null;
            KeyValue statusVal = null;
            KeyValue cTsVal = null;
            KeyValue gTsVal = null;
            KeyValue kaTimeKv = null;
            KeyValue destinationTables = null;
            KeyValue dependentKv = null;
            KeyValue additiveKv = null;
            for(KeyValue kv:keyValues){
                if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_START_TIMESTAMP_COLUMN))
                    beginTsVal = kv;
                else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_PARENT_COLUMN))
                    parentTxnVal = kv;
                else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_READ_UNCOMMITTED_COLUMN))
                    ruVal = kv;
                else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_READ_COMMITTED_COLUMN))
                    rcVal = kv;
                else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_STATUS_COLUMN))
                    statusVal = kv;
                else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_COMMIT_TIMESTAMP_COLUMN))
                    cTsVal = kv;
                else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_GLOBAL_COMMIT_TIMESTAMP_COLUMN))
                    gTsVal = kv;
                else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_KEEP_ALIVE_COLUMN))
                    kaTimeKv = kv;
                else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_WRITE_TABLE_COLUMN))
                    destinationTables = kv;
                else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_DEPENDENT_COLUMN))
                    dependentKv = kv;
                else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_ADDITIVE_COLUMN))
                    additiveKv = kv;
            }
            if(beginTsVal==null) return null;
            long txnId = TxnUtils.txnIdFromRowKey(beginTsVal.getBuffer(),beginTsVal.getRowOffset(),beginTsVal.getRowLength());
            return decodeInternal(txnId,beginTsVal,parentTxnVal,dependentKv,additiveKv,ruVal,rcVal,statusVal,cTsVal,gTsVal,kaTimeKv,destinationTables);
        }

        protected DenseTxn decodeInternal(long txnId,
                                           KeyValue beginTsVal,
                                           KeyValue parentTxnVal,
                                           KeyValue dependentKv,
                                           KeyValue additiveKv,
                                           KeyValue readUncommittedKv,
                                           KeyValue readCommittedKv,
                                           KeyValue statusKv,
                                           KeyValue commitTimestampKv,
                                           KeyValue globalTimestampKv,
                                           KeyValue keepAliveTimeKv,
                                           KeyValue destinationTables) {
            long beginTs = Bytes.toLong(beginTsVal.getBuffer(), beginTsVal.getValueOffset(), beginTsVal.getValueLength());
            long parentTs = -1l; //by default, the "root" transaction id is -1l
            if(parentTxnVal!=null)
                parentTs = Bytes.toLong(parentTxnVal.getBuffer(),parentTxnVal.getValueOffset(),parentTxnVal.getValueLength());
            boolean dependent = false;
            boolean hasDependent = false;
            if(dependentKv!=null){
                hasDependent = true;
                dependent = BytesUtil.toBoolean(dependentKv.getBuffer(), dependentKv.getValueOffset());
            }
            boolean additive = false;
            boolean hasAdditive = false;
            if(additiveKv!=null){
                additive = BytesUtil.toBoolean(additiveKv.getBuffer(),additiveKv.getValueOffset());
                hasAdditive = true;
            }
						/*
						 * We can ignore the ALLOW_WRITES field, for the following reason:
						 *
						 * In the new structure, if the transaction does not allow writes, then it won't be recorded
						 * on the table itself. Thus, any new transaction automatically allows writes. However,
						 * we require that a full cluster restart occur (e.g. all nodes must go down, then come back up) in
						 * order for the new transactional system to work. Therefore, all transactions using the old system
						 * will no longer be writing data. Hence, the old transaction encoding format will only ever
						 * be seen when READING from the transaction table. Read only transactions will never be read from the
						 * table again (since they never wrote anything). Hence, we can safely assume that if someone is
						 * interested in this transaction, then they are interested in writable transactions only, and that
						 * this is therefore writable.
						 */
            boolean readUncommitted = false;
            if(readUncommittedKv!=null)
                readUncommitted = BytesUtil.toBoolean(readUncommittedKv.getBuffer(), readUncommittedKv.getValueOffset());
            boolean readCommitted = false;
            if(readCommittedKv!=null)
                readCommitted = BytesUtil.toBoolean(readCommittedKv.getBuffer(), readCommittedKv.getValueOffset());
            Txn.State state = Txn.State.decode(statusKv.getBuffer(), statusKv.getValueOffset(), statusKv.getValueLength());
            long commitTs = -1l;
            if(commitTimestampKv!=null)
                commitTs = Bytes.toLong(commitTimestampKv.getBuffer(), commitTimestampKv.getValueOffset(), commitTimestampKv.getValueLength());
            long globalCommitTs = -1l;
            if(globalTimestampKv!=null)
                globalCommitTs = Bytes.toLong(globalTimestampKv.getBuffer(),globalTimestampKv.getValueOffset(),globalTimestampKv.getValueLength());
            else if(hasDependent &&!dependent){
                //performance enhancement to avoid an extra region get() during commit time
                globalCommitTs = commitTs;
            }

            if(state== Txn.State.ACTIVE){
                /*
                 * We need to check that the transaction hasn't been timed out (and therefore rolled back). This
                 * happens if the keepAliveTime is older than the configured transaction timeout. Of course,
                 * there is some network latency which could cause small keep alives to be problematic. To help out,
                 * we allow a little fudge factor in the timeout
                 */
                state = adjustStateForTimeout(state, keepAliveTimeKv,true);
            }
            long kaTime = decodeKeepAlive(keepAliveTimeKv,true);

            Txn.IsolationLevel level = null;
            if(readCommittedKv!=null){
                if(readCommitted) level = Txn.IsolationLevel.READ_COMMITTED;
                else if(readUncommittedKv!=null && readUncommitted) level = Txn.IsolationLevel.READ_UNCOMMITTED;
            }else if(readUncommittedKv!=null && readUncommitted)
                level = Txn.IsolationLevel.READ_COMMITTED;


            ByteSlice destTableBuffer = null;
            if(destinationTables!=null){
                destTableBuffer = new ByteSlice();
                destTableBuffer.set(destinationTables.getBuffer(),destinationTables.getValueOffset(),destinationTables.getValueLength());
            }

            return new DenseTxn(txnId,beginTs,parentTs,
                    commitTs,globalCommitTs,hasDependent,dependent,hasAdditive,additive,level,state,destTableBuffer,kaTime);
        }
    }

		private static Txn.State adjustStateForTimeout(Txn.State currentState,KeyValue columnLatest,long currTime,boolean oldForm) {
        long lastKATime = decodeKeepAlive(columnLatest, oldForm);

				if((currTime-lastKATime)>TRANSACTION_TIMEOUT_WINDOW)
						return Txn.State.ROLLEDBACK; //time out the txn

				return currentState;
		}

    private static long decodeKeepAlive(KeyValue columnLatest, boolean oldForm) {
        long lastKATime;
        if(oldForm){
            /*
             * The old version would put an empty value into the Keep Alive column. If the transaction
             * committed before the keep alive was initiated, then the field will still be null.
             *
             * Since we only read transactions in the old form, and don't create new ones, we just have to decide
             * what to do with these situations. They can only arise if the transaction either
             *
             * A) commits/rolls back before the keep alive can be initiated
             * B) fails before the first keep alive.
             *
             * In the case of a commit/roll back, the value of the keep alive doesn't matter, and in the case
             * of B), we want to fail it. The easiest way to deal with this is just to return 0l.
             */
            int length = columnLatest.getValueLength();
            if(length==0) return 0l;
            else
                lastKATime = Bytes.toLong(columnLatest.getBuffer(), columnLatest.getValueOffset(), length);
        }else
            lastKATime = Encoding.decodeLong(columnLatest.getBuffer(), columnLatest.getValueOffset(), false);
        return lastKATime;
    }

    private static Txn.State adjustStateForTimeout(Txn.State currentState,KeyValue columnLatest,boolean oldForm) {
				return adjustStateForTimeout(currentState,columnLatest,System.currentTimeMillis(),oldForm);
		}

		private class ActiveTxnFilter extends FilterBase {
				private final long beforeTs;
				private final long afterTs;
				private final byte[] destinationTable;
				private byte[] encodedDestTable; //for new forms, which encode the table data

				private boolean isActive = false;
				private boolean isAlive = false;
				private boolean stateSeen= false;
				private boolean keepAliveSeen = false;
				private boolean destTablesSeen = false;

				private MultiFieldDecoder bytesDecoder;

				public ActiveTxnFilter(long beforeTs, long afterTs, byte[] destinationTable) {
						this.beforeTs = beforeTs;
						this.afterTs = afterTs;
						this.destinationTable = destinationTable;
				}

				@Override
				public boolean filterRowKey(byte[] buffer, int offset, int length) {
				/*
				 * Since the transaction id must necessarily be the begin timestamp in both
				 * the old and new format, we can filter transactions out based entirely on the
				 * row key here
				 */
						long txnId = TxnUtils.txnIdFromRowKey(buffer,offset,length);
						boolean withinRange = txnId >= afterTs && txnId <= beforeTs;
						return !withinRange;
				}

				@Override
				public ReturnCode filterKeyValue(KeyValue kv) {
						if(KeyValueUtils.singleMatchingQualifier(kv, STATE_QUALIFIER_BYTES) ||
										KeyValueUtils.singleMatchingQualifier(kv,OLD_STATUS_COLUMN)){
								return filterState(kv);
						}else if(KeyValueUtils.singleMatchingQualifier(kv,KEEP_ALIVE_QUALIFIER_BYTES)){
                return filterKeepAlive(kv,false);
						}else if(KeyValueUtils.singleMatchingQualifier(kv,DESTINATION_TABLE_QUALIFIER_BYTES)){
								if(destTablesSeen) return ReturnCode.SKIP; //just to be safe
								destTablesSeen = true;
                if(destinationTable==null) return ReturnCode.INCLUDE;

								if(encodedDestTable==null)
										encodedDestTable = Encoding.encodeBytesUnsorted(destinationTable);

								//try and match the destination table
								if(bytesDecoder==null)
										bytesDecoder = MultiFieldDecoder.wrap(kv.getBuffer(),kv.getOffset(),kv.getLength());
								else
										bytesDecoder.set(kv.getBuffer(),kv.getOffset(),kv.getLength());

                while(bytesDecoder.available()){
                    int offset = bytesDecoder.offset();
                    bytesDecoder.skip();
                    int length = bytesDecoder.offset()-offset-1;
                    if(Bytes.equals(destinationTable,0,destinationTable.length,bytesDecoder.array(),offset,length)){
                        //we match!
                        return ReturnCode.INCLUDE;
                    }
                }
                return ReturnCode.NEXT_ROW; //doesn't match the destination table
            }else{
                if(destinationTable!=null){
                    if (KeyValueUtils.singleMatchingQualifier(kv,OLD_KEEP_ALIVE_COLUMN)){
                        return filterKeepAlive(kv,true);
                    } else if(KeyValueUtils.singleMatchingQualifier(kv,OLD_WRITE_TABLE_COLUMN)){
                        if(destTablesSeen) return ReturnCode.SKIP;
                        destTablesSeen = true;
                        if(!Bytes.equals(destinationTable,0,destinationTable.length,kv.getBuffer(),kv.getValueOffset(),kv.getValueLength()))
                            return ReturnCode.NEXT_ROW;
                        return ReturnCode.INCLUDE;
                    }
                }
                return ReturnCode.INCLUDE;
            }
				}

        protected ReturnCode filterKeepAlive(KeyValue kv,boolean oldForm) {
            if(keepAliveSeen) return ReturnCode.SKIP; //just to be safe
            keepAliveSeen = true;
            Txn.State adjustedState = adjustStateForTimeout(Txn.State.ACTIVE,kv,oldForm);
            if(adjustedState!= Txn.State.ACTIVE){
                //if we've already determined that we're active, then we are actually timed out, so skip this entry
                if(isActive)
                    return ReturnCode.NEXT_ROW;
                else{
                    //we either don't know if we're active, or we are committed/rolledback anyway.
                    isAlive=false;
                }
            }else
            isAlive=true;
            return ReturnCode.INCLUDE;
        }

        protected ReturnCode filterState(KeyValue kv) {
						if(stateSeen) return ReturnCode.SKIP; //shouldn't happen, but just in case
						stateSeen = true;
						byte[] activeState = Txn.State.ACTIVE.encode();
						if(!Bytes.equals(activeState, 0, activeState.length,
										kv.getBuffer(), kv.getValueOffset(), kv.getValueLength())){
								return ReturnCode.NEXT_ROW; //skip the entire row and move on
						}
						//we are in the active state, so skip this column, but make sure to check keep alive
						if(!isAlive) {
								return ReturnCode.NEXT_ROW; //we are actually rolled back
						}
						else isActive=true;
						return ReturnCode.INCLUDE;
				}

				@Override
				public void reset() {
						isAlive=false;
						isActive=false;
						stateSeen=false;
						keepAliveSeen = false;
						destTablesSeen = false;
				}


				@Override public void write(DataOutput out) throws IOException {  }
				@Override public void readFields(DataInput in) throws IOException {  }
		}
}
