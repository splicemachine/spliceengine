package com.splicemachine.si.impl.region;

import com.splicemachine.si.api.data.SExceptionLib;
import com.splicemachine.si.api.txn.STransactionLib;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnDecoder;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.IHTable;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
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
public class RegionTxnStore<OperationWithAttributes,Data,Delete extends OperationWithAttributes,Filter,
        Get extends OperationWithAttributes,Mutation extends OperationWithAttributes, OperationStatus,Pair,
        Put extends OperationWithAttributes,RegionScanner,Result,ReturnCode,Scan extends OperationWithAttributes,
        TableBuffer,Transaction,TxnInfo> {
    private static final Logger LOG = Logger.getLogger(RegionTxnStore.class);
		/*
		 * The region in which to access data
		 */
		private final IHTable<OperationWithAttributes,Delete,
                Get,Mutation,OperationStatus,Pair,
                Put,Result,Scan> region;
        private final STransactionLib<Transaction,TableBuffer> transactionlib = SIDriver.getTransactionLib();

		private final TxnDecoder<OperationWithAttributes,Data,Delete,Filter,
                        Get,Put,RegionScanner,Result,Scan,Transaction,TxnInfo> newTransactionDecoder = transactionlib.getV2TxnDecoder();
		private final SDataLib<OperationWithAttributes,Data,Delete,Filter,Get,
                        Put,RegionScanner,Result,Scan> dataLib = SIDriver.getDataLib();
		private final TransactionResolver<OperationWithAttributes,Data,Delete,Filter,
                Get,Put,RegionScanner,Result,
                ReturnCode,Scan,Transaction,TableBuffer> resolver = SIDriver.getTransactionResolver();
		private final TxnSupplier txnSupplier = SIDriver.getTxnSupplier();
        private final SExceptionLib exceptionLib = SIDriver.getExceptionLib();

		public RegionTxnStore(IHTable region) {
				this.region = region;
		}

		/**
		 * Fetch all information about a transaction.
		 *
		 * @param txnId the transaction id to fetch
		 * @return all recorded transaction information for the specified transaction.
		 * @throws IOException if something goes wrong when fetching transactions
		 */
		public Transaction getTransaction(long txnId) throws IOException {
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "getTransaction txnId=%d",txnId);
            Get get = dataLib.newGet(TxnUtils.getRowKey(txnId));
			Result result = region.get(get);
			if(dataLib.noResult(result))
                return null; //no transaction
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
		 */
		public void addDestinationTable(long txnId,byte[] destinationTable) throws IOException {
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "addDestinationTable txnId=%d, desinationTable",txnId,destinationTable);
            Get get = dataLib.newGet(TxnUtils.getRowKey(txnId));
            byte[] destTableQualifier = AbstractV2TxnDecoder.DESTINATION_TABLE_QUALIFIER_BYTES;
            dataLib.addFamilyQualifierToGet(get, FAMILY, destTableQualifier);
				/*
				 * We only need to check the new transaction format, because we will never attempt to elevate
				 * a transaction created using the old transaction format.
				 */

				Result result = region.get(get);
				//should never happen, this is in place to protect against programmer error
				if(result==null)
						throw exceptionLib.getReadOnlyModificationException("Transaction "+ txnId+" is read-only, and was not properly elevated.");
				Data kv = dataLib.getColumnLatest(result,FAMILY, destTableQualifier);
				byte[] newBytes;
				if(kv==null){
						/*
						 * this shouldn't happen, but you never know--someone might create a writable transaction
						 * without specifying a table directly. In that case, this will still work
						 */
						newBytes = destinationTable;
				}else{
						int valueLength = dataLib.getDataValuelength(kv);
						newBytes = new byte[valueLength+destinationTable.length+1];
						System.arraycopy(dataLib.getDataValueBuffer(kv),dataLib.getDataValueOffset(kv),newBytes,0,valueLength);
						System.arraycopy(destinationTable,0,newBytes,valueLength+1,destinationTable.length);
				}
                Put put = dataLib.newPut(dataLib.getGetRow(get));
                dataLib.addKeyValueToPut(put, FAMILY, destTableQualifier, newBytes);
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
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "keepAlive txnId=%d",txnId);
				byte[] rowKey = TxnUtils.getRowKey(txnId);
				Get get = dataLib.newGet(rowKey);
                dataLib.addFamilyQualifierToGet(get,FAMILY,AbstractV2TxnDecoder.KEEP_ALIVE_QUALIFIER_BYTES);
                dataLib.addFamilyQualifierToGet(get, FAMILY, AbstractV2TxnDecoder.STATE_QUALIFIER_BYTES);
				//we don't try to keep alive transactions with an old form
				Result result = region.get(get);
				if(result==null) return false; //attempted to keep alive a read-only transaction? a waste, but whatever

				Data stateKv = dataLib.getColumnLatest(result, FAMILY, AbstractV2TxnDecoder.STATE_QUALIFIER_BYTES);
                if (stateKv==null) {
                    // couldn't find the transaction data, it's fine under Restore Mode, issue a warning nonetheless
                    LOG.warn("Couldn't load data for keeping alive transaction " + txnId + ". This isn't an issue under Restore Mode");
                    return false;
                }
				Txn.State state = Txn.State.decode(dataLib.getDataValueBuffer(stateKv),
						dataLib.getDataValueOffset(stateKv), dataLib.getDataValuelength(stateKv));
				if(state != Txn.State.ACTIVE) return false; //skip the put if we don't need to do it
				Data oldKAKV = dataLib.getColumnLatest(result, FAMILY, AbstractV2TxnDecoder.KEEP_ALIVE_QUALIFIER_BYTES);
				long currTime = System.currentTimeMillis();
				Txn.State adjustedState = AbstractV2TxnDecoder.adjustStateForTimeout(dataLib,state,oldKAKV,currTime,false);
				if(adjustedState!= Txn.State.ACTIVE)
						throw exceptionLib.getTransactionTimeoutException(txnId);

				Put newPut = dataLib.newPut(TxnUtils.getRowKey(txnId));
                dataLib.addKeyValueToPut(newPut, FAMILY, AbstractV2TxnDecoder.KEEP_ALIVE_QUALIFIER_BYTES, Encoding.encode(currTime));
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
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "getState txnId=%d",txnId);
				byte[] rowKey = TxnUtils.getRowKey(txnId);
				Get get = dataLib.newGet(rowKey);
                dataLib.addFamilyQualifierToGet(get,FAMILY,AbstractV2TxnDecoder.STATE_QUALIFIER_BYTES);
                dataLib.addFamilyQualifierToGet(get,FAMILY,AbstractV2TxnDecoder.KEEP_ALIVE_QUALIFIER_BYTES);
				//add the columns for the new encoding
				Result result = region.get(get);
				if(result==null)
						return null; //indicates that the transaction was probably read only--external callers can figure out what that means

				boolean oldForm = false;
				Data keepAliveKv;
				Data stateKv = dataLib.getColumnLatest(result, FAMILY,AbstractV2TxnDecoder.STATE_QUALIFIER_BYTES);
						keepAliveKv = dataLib.getColumnLatest(result, FAMILY,AbstractV2TxnDecoder.KEEP_ALIVE_QUALIFIER_BYTES);
				Txn.State state = Txn.State.decode(dataLib.getDataValueBuffer(stateKv),
						dataLib.getDataValueOffset(stateKv),dataLib.getDataValuelength(stateKv));
				if(state== Txn.State.ACTIVE)
						state = AbstractV2TxnDecoder.adjustStateForTimeout(dataLib,state,keepAliveKv,oldForm);

				if (LOG.isTraceEnabled())
					SpliceLogUtils.trace(LOG, "getState returnedState state=%s",state);
				
				return state;
		}

    /**
		 * Write the full transaction information to the local storage.
		 *
		 * @param txn the transaction to write
		 * @throws IOException if something goes wrong in writing the transaction
		 */
		public void recordTransaction(TxnInfo txn) throws IOException{
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "recordTransaction txn=%s",txn);
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
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "recordCommit txnId=%d, commitTs=%d",txnId, commitTs);
            Put put = dataLib.newPut(TxnUtils.getRowKey(txnId));
            dataLib.addKeyValueToPut(put,FAMILY,AbstractV2TxnDecoder.COMMIT_QUALIFIER_BYTES,Encoding.encode(commitTs));
            dataLib.addKeyValueToPut(put,FAMILY,AbstractV2TxnDecoder.STATE_QUALIFIER_BYTES, Txn.State.COMMITTED.encode());
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
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "getCommitTimestamp txnId=%d",txnId);
			Get get = dataLib.newGet(TxnUtils.getRowKey(txnId));
            dataLib.addFamilyQualifierToGet(get, FAMILY, AbstractV2TxnDecoder.COMMIT_QUALIFIER_BYTES);
            Result result = region.get(get);
            if(result==null) return -1l; //no commit timestamp for read-only transactions
            Data kv;
            if((kv=dataLib.getColumnLatest(result,FAMILY,AbstractV2TxnDecoder.COMMIT_QUALIFIER_BYTES))!=null)
                    return Encoding.decodeLong(dataLib.getDataValueBuffer(kv),dataLib.getDataValueOffset(kv),false);
            else{
                throw new IOException("V1 Decoder Required?");
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
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "recordRollback txnId=%d",txnId);
			Put put = dataLib.newPut(TxnUtils.getRowKey(txnId));
            dataLib.addKeyValueToPut(put, FAMILY, AbstractV2TxnDecoder.STATE_QUALIFIER_BYTES, Txn.State.ROLLEDBACK.encode());
            dataLib.addKeyValueToPut(put, FAMILY, AbstractV2TxnDecoder.COMMIT_QUALIFIER_BYTES, Encoding.encode(-1));
            dataLib.addKeyValueToPut(put, FAMILY, AbstractV2TxnDecoder.GLOBAL_COMMIT_QUALIFIER_BYTES, Encoding.encode(-1));
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
        /*
		public long[] getActiveTxnIds(long afterTs, long beforeTs, byte[] destinationTable) throws IOException{
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "getActiveTxnIds beforeTs=%d, afterTs=%s, destinationTable=%s",beforeTs, afterTs, destinationTable);
	
			Source<Transaction> activeTxn = getActiveTxns(afterTs,beforeTs,destinationTable);
	        LongArrayList lal = LongArrayList.newInstance();
	        while(activeTxn.hasNext()){
	            lal.add(transactionlib.getTxnId(activeTxn.next()));
	        }
	        return lal.toArray();
		}
		*/

    /*
    public Source<Transaction> getAllTxns(long minTs, long maxTs) throws IOException{
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "getAllTxns minTs=%d, maxTs=%s",minTs, maxTs);
    	Scan scan = setupScanOnRange(minTs,maxTs);

        CloseableIterator<Result> baseScanner = region.scan(scan);

        final RegionScanner scanner = dataLib.getBufferedRegionScanner(region,baseScanner,scan,1024, Metrics.noOpMetricFactory());
        
        return new RegionScanIterator<Data,Put,Delete,Get,Scan,Transaction>(scanner,new RegionScanIterator.IOFunction<Transaction,Data>() {
            @Override
            public Transaction apply(@Nullable List<Data> keyValues) throws IOException{
                return decode(keyValues);
            }
        },dataLib);
    }

    public Source<Transaction> getActiveTxns(long afterTs,long beforeTs, byte[] destinationTable) throws IOException{
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "getActiveTxns afterTs=%d, beforeTs=%s",afterTs, beforeTs);
    	Scan scan = setupScanOnRange(afterTs, beforeTs);
        dataLib.setFilterOnScan(scan, dataLib.getActiveTransactionFilter(beforeTs, afterTs, destinationTable));
        CloseableIterator<Result> baseScanner = region.scan(scan);

        final RegionScanner scanner = dataLib.getBufferedRegionScanner(region,baseScanner,scan,1024, Metrics.noOpMetricFactory());
        return new RegionScanIterator<Data,Put,Delete,Get,Scan,Transaction>(scanner,new RegionScanIterator.IOFunction<Transaction,Data>() {
            @Override
            public Transaction apply(@Nullable List<Data> keyValues) throws IOException{
                Transaction txn = newTransactionDecoder.decode(dataLib,keyValues);
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

    /*
                long parentTxnId =transactionlib.getParentTxnId(txn);
                if(parentTxnId<0){
                    //we are a top-level transaction
                    return txn;
                }

                switch(txnStore.getTransaction(parentTxnId).getEffectiveState()){
                    case ACTIVE:
                        return txn;
                    case ROLLEDBACK:
                        resolver.resolveTimedOut(region,txn);
                        return null;
                    case COMMITTED:
                        resolver.resolveGlobalCommitTimestamp(region,txn);
                        return null;
                }

                return txn;
            }
        },dataLib);

    }
    */

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
        byte[] startKey = Bytes.concat(Arrays.asList(new byte[]{bucket}, Bytes.toBytes(afterTs)));
        if(Bytes.startComparator.compare(region.getStartKey(),startKey)>0)
            startKey = region.getStartKey();
        byte[] stopKey = Bytes.concat(Arrays.asList(new byte[]{bucket}, Bytes.toBytes(beforeTs+1)));
        if(Bytes.endComparator.compare(region.getEndKey(),stopKey)<0)
            stopKey = region.getEndKey();
        Scan scan = dataLib.newScan(startKey,stopKey);
        dataLib.setScanMaxVersions(scan, 1);
        return scan;
    }


    private Transaction decode(long txnId,Result result) throws IOException {
        Transaction txn = newTransactionDecoder.decode(dataLib,txnId,result);
        resolveTxn(txn);
        return txn;

    }
    private Transaction decode(List<Data> keyValues) throws IOException {
        Transaction txn = newTransactionDecoder.decode(dataLib,keyValues);
        resolveTxn(txn);
        return txn;
    }

    private void resolveTxn(Transaction txn) {
        switch(transactionlib.getTransactionState(txn)){
            case ROLLEDBACK:
                if(transactionlib.isTimedOut(txn)){
                    resolver.resolveTimedOut(region,txn);
                }
                break;
            case COMMITTED:
                if(transactionlib.getParentTxnId(txn)>0 && transactionlib.getGlobalCommitTimestamp(txn)<0){
                /*
                 * Just because the transaction was committed and has a parent doesn't mean that EVERY parent
                 * has been committed; still, submit this to the resolver on the off chance that it
                 * has been fully committed, so we can get away with the global commit work.
                 */
                    resolver.resolveGlobalCommitTimestamp(region,txn);
                }
        }
    }
}
