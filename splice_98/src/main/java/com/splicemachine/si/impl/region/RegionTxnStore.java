package com.splicemachine.si.impl.region;

import com.carrotsearch.hppc.LongArrayList;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.RegionScanIterator;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.*;
import com.splicemachine.si.impl.DenseTxn;
import com.splicemachine.si.impl.SparseTxn;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.utils.Source;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nullable;
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
		private final V2TxnDecoder newTransactionDecoder;
    private final TransactionResolver resolver;
    private final TxnSupplier txnStore;

		public RegionTxnStore(HRegion region,TransactionResolver resolver,TxnSupplier txnStore) {
				this.region = region;
				this.oldTransactionDecoder = V1TxnDecoder.INSTANCE;
				this.newTransactionDecoder = V2TxnDecoder.INSTANCE;
        this.resolver = resolver;
        this.txnStore = txnStore;
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

        return decode(txnId,result);
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
        byte[] destTableQualifier = V2TxnDecoder.DESTINATION_TABLE_QUALIFIER_BYTES;
        get.addColumn(FAMILY, destTableQualifier);
				/*
				 * We only need to check the new transaction format, because we will never attempt to elevate
				 * a transaction created using the old transaction format.
				 */

				Result result = region.get(get);
				//should never happen, this is in place to protect against programmer error
				if(result==null)
						throw new ReadOnlyModificationException("Transaction "+ txnId+" is read-only, and was not properly elevated.");
				KeyValue kv = result.getColumnLatest(FAMILY, destTableQualifier);
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
				put.add(FAMILY,destTableQualifier,newBytes);
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
				get.addColumn(FAMILY,V2TxnDecoder.KEEP_ALIVE_QUALIFIER_BYTES);
				get.addColumn(FAMILY,V2TxnDecoder.STATE_QUALIFIER_BYTES);
				//we don't try to keep alive transactions with an old form

				Result result = region.get(get);
				if(result==null) return false; //attempted to keep alive a read-only transaction? a waste, but whatever

				KeyValue stateKv = result.getColumnLatest(FAMILY,V2TxnDecoder.STATE_QUALIFIER_BYTES);
				Txn.State state = Txn.State.decode(stateKv.getBuffer(), stateKv.getValueOffset(), stateKv.getValueLength());
				if(state != Txn.State.ACTIVE) return false; //skip the put if we don't need to do it

				KeyValue oldKAKV = result.getColumnLatest(FAMILY,V2TxnDecoder.KEEP_ALIVE_QUALIFIER_BYTES);
				long currTime = System.currentTimeMillis();
				Txn.State adjustedState = TxnDecoder.adjustStateForTimeout(state,oldKAKV,currTime,false);
				if(adjustedState!= Txn.State.ACTIVE)
						throw new TransactionTimeoutException(txnId);

				Put newPut = new Put(TxnUtils.getRowKey(txnId));
				newPut.add(FAMILY,V2TxnDecoder.KEEP_ALIVE_QUALIFIER_BYTES,Encoding.encode(currTime));
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
				get.addColumn(FAMILY,V2TxnDecoder.STATE_QUALIFIER_BYTES);
				get.addColumn(FAMILY,V2TxnDecoder.KEEP_ALIVE_QUALIFIER_BYTES);
				//add the columns for the old encoding
				get.addColumn(FAMILY,V1TxnDecoder.OLD_STATUS_COLUMN);
				get.addColumn(FAMILY,V1TxnDecoder.OLD_KEEP_ALIVE_COLUMN);

				Result result = region.get(get);
				if(result==null)
						return null; //indicates that the transaction was probably read only--external callers can figure out what that means

				boolean oldForm = false;
				KeyValue keepAliveKv;
				KeyValue stateKv = result.getColumnLatest(FAMILY,V2TxnDecoder.STATE_QUALIFIER_BYTES);
				if(stateKv==null){
						oldForm=true;
					//used the old encoding
						stateKv =result.getColumnLatest(FAMILY,V1TxnDecoder.OLD_STATUS_COLUMN);
						keepAliveKv = result.getColumnLatest(FAMILY,V1TxnDecoder.OLD_KEEP_ALIVE_COLUMN);
				}else{
						keepAliveKv = result.getColumnLatest(FAMILY,V2TxnDecoder.KEEP_ALIVE_QUALIFIER_BYTES);
				}

				Txn.State state = Txn.State.decode(stateKv.getBuffer(),stateKv.getValueOffset(),stateKv.getValueLength());
				if(state== Txn.State.ACTIVE)
						state = TxnDecoder.adjustStateForTimeout(state,keepAliveKv,oldForm);

				return state;
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
				put.add(FAMILY,V2TxnDecoder.COMMIT_QUALIFIER_BYTES,Encoding.encode(commitTs));
				put.add(FAMILY,V2TxnDecoder.STATE_QUALIFIER_BYTES, Txn.State.COMMITTED.encode());

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
				get.addColumn(FAMILY,V2TxnDecoder.COMMIT_QUALIFIER_BYTES);
				get.addColumn(FAMILY,V1TxnDecoder.OLD_COMMIT_TIMESTAMP_COLUMN);

				Result result = region.get(get);
				if(result==null) return -1l; //no commit timestamp for read-only transactions

				KeyValue kv;
				if((kv=result.getColumnLatest(FAMILY,V2TxnDecoder.COMMIT_QUALIFIER_BYTES))!=null)
						return Encoding.decodeLong(kv.getBuffer(),kv.getValueOffset(),false);
				else{
						kv = result.getColumnLatest(FAMILY,V1TxnDecoder.OLD_COMMIT_TIMESTAMP_COLUMN);
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
				put.add(FAMILY,V2TxnDecoder.STATE_QUALIFIER_BYTES, Txn.State.ROLLEDBACK.encode());
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
        Source<DenseTxn> activeTxn = getActiveTxns(afterTs,beforeTs,destinationTable);
        LongArrayList lal = LongArrayList.newInstance();
        while(activeTxn.hasNext()){
            lal.add(activeTxn.next().getTxnId());
        }
        return lal.toArray();
		}

    public Source<DenseTxn> getAllTxns(long minTs, long maxTs) throws IOException{
        Scan scan = setupScanOnRange(minTs,maxTs);

        RegionScanner baseScanner = region.getScanner(scan);

        final RegionScanner scanner = new BufferedRegionScanner(region,baseScanner,scan,1024, Metrics.noOpMetricFactory());
        return new RegionScanIterator<DenseTxn>(scanner,new RegionScanIterator.IOFunction< DenseTxn>() {
            @Override
            public DenseTxn apply(@Nullable List<KeyValue> keyValues) throws IOException{
                return decode(keyValues);
            }
        });
    }

    public Source<DenseTxn> getActiveTxns(long afterTs,long beforeTs, byte[] destinationTable) throws IOException{
        Scan scan = setupScanOnRange(afterTs, beforeTs);

        scan.setFilter(new BaseActiveTxnFilter(beforeTs,afterTs,destinationTable));

        RegionScanner baseScanner = region.getScanner(scan);

        final RegionScanner scanner = new BufferedRegionScanner(region,baseScanner,scan,1024, Metrics.noOpMetricFactory());
        return new RegionScanIterator<DenseTxn>(scanner,new RegionScanIterator.IOFunction< DenseTxn>() {
            @Override
            public DenseTxn apply(@Nullable List<KeyValue> keyValues) throws IOException{
                DenseTxn txn = newTransactionDecoder.decode(keyValues);
                boolean oldForm = false;
                if(txn==null){
                    oldForm = true;
                    txn = oldTransactionDecoder.decode(keyValues);
                }

                /*
                 * In normal circumstances, we would say that this transaction is active
                 * (since it passed the ActiveTxnFilter).
                 *
                 * However, a child transaction may need to be returned even though
                 * he is committed, because a parent along the chain remains active. In this case,
                 * we need to resolve the effective commit timestamp of the parent, and if that value
                 * is -1, then we return it. Otherwise, just mark the child transaction with a global
                 * commit timestamp and move on.
                 */
                long parentTxnId =txn.getParentTxnId();
                if(parentTxnId<0){
                    //we are a top-level transaction
                    return txn;
                }

                switch(txnStore.getTransaction(parentTxnId).getEffectiveState()){
                    case ACTIVE:
                        return txn;
                    case ROLLEDBACK:
                        resolver.resolveTimedOut(region,txn,oldForm);
                        return null;
                    case COMMITTED:
                        resolver.resolveGlobalCommitTimestamp(region,txn,oldForm);
                        return null;
                }

                return txn;
            }
        });

    }


    /******************************************************************************************************************/
		/*private helper methods*/

		//easy reference for code clarity
		private static final byte[] FAMILY = SIConstants.DEFAULT_FAMILY_BYTES;

    private Scan setupScanOnRange(long afterTs, long beforeTs) {
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
        byte[] stopKey = BytesUtil.concat(Arrays.asList(new byte[]{bucket}, Bytes.toBytes(beforeTs+1)));
        if(BytesUtil.endComparator.compare(region.getEndKey(),stopKey)<0)
            stopKey = region.getEndKey();
        Scan scan = new Scan(startKey,stopKey);
        scan.setMaxVersions(1);
        return scan;
    }


    private SparseTxn decode(long txnId,Result result) throws IOException {
        SparseTxn txn = newTransactionDecoder.decode(txnId,result);
        boolean oldForm = false;
        if(txn==null){
            oldForm = true;
            txn = oldTransactionDecoder.decode(txnId,result);
        }

        resolveTxn(txn, oldForm);
        return txn;

    }
    private DenseTxn decode(List<KeyValue> keyValues) throws IOException {
        DenseTxn txn = newTransactionDecoder.decode(keyValues);
        boolean oldForm = false;
        if(txn==null){
            oldForm = true;
            txn = oldTransactionDecoder.decode(keyValues);
        }

        resolveTxn(txn, oldForm);
        return txn;
    }

    private void resolveTxn(SparseTxn txn, boolean oldForm) {
        switch(txn.getState()){
            case ROLLEDBACK:
                if(txn.isTimedOut()){
                    resolver.resolveTimedOut(region,txn,oldForm);
                }
                break;
            case COMMITTED:
                if(txn.getParentTxnId()>0 && txn.getGlobalCommitTimestamp()<0){
                /*
                 * Just because the transaction was committed and has a parent doesn't mean that EVERY parent
                 * has been committed; still, submit this to the resolver on the off chance that it
                 * has been fully committed, so we can get away with the global commit work.
                 */
                    resolver.resolveGlobalCommitTimestamp(region,txn,oldForm);
                }
        }
    }
}
