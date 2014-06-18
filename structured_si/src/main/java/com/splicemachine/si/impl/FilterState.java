package com.splicemachine.si.impl;

import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.data.api.SDataLib;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.IOException;
import static org.apache.hadoop.hbase.filter.Filter.ReturnCode.*;

/**
 * Contains the logic for performing an HBase-style filter using "snapshot isolation" logic. This means it filters out
 * data that should not be seen by the transaction that is performing the read operation (either a "get" or a "scan").
 */
public class FilterState<Result, Put extends OperationWithAttributes, Delete, Get extends OperationWithAttributes,
        Scan, Lock, OperationStatus, Mutation, IHTable>
        implements IFilterState {
    static final Logger LOG = Logger.getLogger(FilterState.class);
    /**
     * The transactions that have been loaded as part of running this filter.
     */
    final LongPrimitiveCacheMap<Transaction> transactionCache;
    final LongPrimitiveCacheMap<VisibleResult> visibleCache;
    private final ImmutableTransaction myTransaction;
    private final SDataLib<Put, Delete, Get, Scan> dataLib;
    private final DataStore<Mutation, Put,Delete, Get, Scan, IHTable> dataStore;
    private final TransactionStore transactionStore;
    private final RollForwardQueue rollForwardQueue;
    final FilterRowState<Result, Put, Delete, Get, Scan, Lock, OperationStatus> rowState;
    KeyValue keyValue;
    KeyValueType type;
    private final TransactionSource transactionSource;

    public FilterState(SDataLib dataLib,
											 DataStore dataStore,
											 TransactionStore transactionStore,
                RollForwardQueue rollForwardQueue,
								ImmutableTransaction myTransaction) {
        this.transactionSource = new TransactionSource() {
            @Override
            public Transaction getTransaction(long timestamp) throws IOException {
                return getTransactionFromFilterCache(timestamp);
            }
        };
        this.dataLib = dataLib;
        this.dataStore = dataStore;
        this.transactionStore = transactionStore;
        this.rollForwardQueue = rollForwardQueue;
        this.myTransaction = myTransaction;
        transactionCache = new LongPrimitiveCacheMap<Transaction>();
        visibleCache = new LongPrimitiveCacheMap<VisibleResult>();
        this.rowState = new FilterRowState();
    }


    /**
     * The public entry point. This returns an HBase filter code and is expected to serve as the heart of an HBase filter
     * implementation.
     * The order of the column families is important. It is expected that the SI family will be processed first.
		 * @param dataKeyValue
		 */
    @Override
    public Filter.ReturnCode filterKeyValue(KeyValue dataKeyValue) throws IOException {
        setKeyValue(dataKeyValue);
        return filterByColumnType();
    }

		@Override
		public KeyValueType getType(KeyValue keyValue) throws IOException {
				return type;
		}

		void setKeyValue(KeyValue dataKeyValue) {
        keyValue = dataKeyValue;
        rowState.updateCurrentRow(keyValue);
        type = dataStore.getKeyValueType(dataKeyValue);
    }

    @Override
    public void nextRow() {
        rowState.resetCurrentRow();
    }

    /**
     * Look at the column family and qualifier to determine how to "dispatch" the current keyValue.
     */
    Filter.ReturnCode filterByColumnType() throws IOException {
    	switch(type) {
		case USER_DATA:
        	processDataTimestamp();
            return processUserData();
		case COMMIT_TIMESTAMP:
			return processCommitTimestamp();
		case TOMBSTONE:
	       	processDataTimestamp();
            return processTombstone();
		case ANTI_TOMBSTONE:
        	processDataTimestamp();
            return processAntiTombstone();
		case OTHER:
		default:
            return processUnknownFamilyData();    	
    	}
    }

    /**
     * Handles the "commit timestamp" values that SI writes to each data row. Processing these values causes the
     * relevant transaction data to be loaded up into the local cache.
     */
    private Filter.ReturnCode processCommitTimestamp() throws IOException {
        log("processCommitTimestamp");
        Transaction transaction = transactionCache.get(keyValue.getTimestamp());
        if (transaction == null) {
            transaction = processCommitTimestampDirect();
        }
        rowState.transactionCache.put(transaction.getTransactionId().getId(), transaction);
        return SKIP;
    }
    
    /**
     * Handles the "commit timestamp" values that SI writes to each data row. Processing these values causes the
     * relevant transaction data to be loaded up into the local cache.
     */
    private void processDataTimestamp() throws IOException {
        log("processCommitTimestamp");
        if (rowState.transactionCache.get(keyValue.getTimestamp()) != null) { // Already processed, must have been committed or failed...
        	return;
        }
        Transaction transaction = transactionCache.get(keyValue.getTimestamp()); // hit the major cache
        if (transaction == null) {
            transaction = processDataTimestampDirect(); // lookup
        }
        rowState.transactionCache.put(transaction.getTransactionId().getId(), transaction);
        return;
    }

    private Transaction processDataTimestamp(long timestamp) throws IOException {
        log("processCommitTimestamp");
        Transaction transaction;
        if ((transaction = rowState.transactionCache.get(timestamp)) != null) { // Already processed, must have been committed or failed...
        	return transaction;
        }
        transaction = transactionCache.get(timestamp); // hit the major cache
        if (transaction == null) {
            transaction = processDataTimestampDirect(timestamp); // lookup
        }
        rowState.transactionCache.put(transaction.getTransactionId().getId(), transaction);
        return transaction;
    }

    
    private Transaction processCommitTimestampDirect() throws IOException {
        Transaction transaction;
        if (dataStore.isSIFail(keyValue)) {
            transaction = transactionStore.makeStubFailedTransaction(keyValue.getTimestamp());
            transactionCache.put(keyValue.getTimestamp(), transaction);
        } else {
						long ts = Bytes.toLong(keyValue.getBuffer(), keyValue.getValueOffset(), keyValue.getValueLength());
//						Long ts = dataLib.decode(keyValue.getBuffer(), keyValue.getValueOffset(), keyValue.getValueLength(), Long.class);
						transaction = transactionStore.makeStubCommittedTransaction(keyValue.getTimestamp(),ts);
            transactionCache.put(keyValue.getTimestamp(), transaction);
        }
        return transaction;
    }

    private Transaction processDataTimestampDirect(long timestamp) throws IOException {
        return handleUnknownTransactionStatus(timestamp);
   }

    
    private Transaction processDataTimestampDirect() throws IOException {
         return handleUnknownTransactionStatus();
    }

    
    private Transaction getTransactionFromFilterCache(long timestamp) throws IOException {
        Transaction transaction = transactionCache.get(timestamp);
        if (transaction == null) {
            if (!myTransaction.getEffectiveReadCommitted() && !myTransaction.getEffectiveReadUncommitted()
                    && !myTransaction.isAncestorOf(transactionStore.getImmutableTransaction(timestamp))) {
                transaction = transactionStore.getTransactionAsOf(timestamp, myTransaction.getLongTransactionId());
            } else {
                transaction = transactionStore.getTransaction(timestamp);
            }
            transactionCache.put(transaction.getTransactionId().getId(), transaction);
        }
        return transaction;
    }

    /**
     * Handles the case where the commit timestamp cell contains a begin timestamp, but doesn't have an
     * associated commit timestamp in the cell. This means the transaction table needs to be consulted to find out
     * the current status of the transaction.
     */
    private Transaction handleUnknownTransactionStatus() throws IOException {
        final Transaction cachedTransaction = rowState.transactionCache.get(keyValue.getTimestamp());
        if (cachedTransaction == null) {
            final Transaction dataTransaction = getTransactionFromFilterCache(keyValue.getTimestamp());
            final VisibleResult visibleResult = checkVisibility(dataTransaction);
            if (visibleResult.effectiveStatus.isFinished()) {
                rollForward(dataTransaction);
            }
            return dataTransaction;
        } else {
            return cachedTransaction;
        }
    }
    
    private Transaction handleUnknownTransactionStatus(long transactionId) throws IOException {
        final Transaction cachedTransaction = rowState.transactionCache.get(transactionId);
        if (cachedTransaction == null) {
            final Transaction dataTransaction = getTransactionFromFilterCache(transactionId);
            final VisibleResult visibleResult = checkVisibility(dataTransaction);
            if (visibleResult.effectiveStatus.isFinished()) {
                rollForward(dataTransaction);
            }
            return dataTransaction;
        } else {
            return cachedTransaction;
        }
    }

    private VisibleResult checkVisibility(Transaction dataTransaction) throws IOException {
        final long timestamp = dataTransaction.getLongTransactionId();
        VisibleResult result = visibleCache.get(timestamp);
        if (result == null) {
            result = myTransaction.canSee(dataTransaction, transactionSource);
            visibleCache.put(timestamp, result);
        }
        return result;
    }

    /**
     * Update the data row to remember the commit timestamp of the transaction. This avoids the need to look the
     * transaction up in the transaction table again the next time this row is read.
     */
    private void rollForward(Transaction transaction) throws IOException {
				LOG.trace("Rolling forward");
        TransactionStatus status = transaction.getEffectiveStatus();
        if (rollForwardQueue != null && status.isFinished() && status.isCommitted()) {
            dataStore.recordRollForward(rollForwardQueue, transaction.getLongTransactionId(), keyValue.getRow(), transaction.getEffectiveCommitTimestamp());
        }
    }

    /**
     * Under SI, deleting a row is handled as adding a row level tombstone record in the SI family. This function reads
     * those values in for a given row and stores the tombstone timestamp so it can be used later to filter user data.
     */
    private Filter.ReturnCode processTombstone() throws IOException {
        log("processTombstone");
        rowState.setTombstoneTimestamp(keyValue.getTimestamp());
        return SKIP;
    }

    private Filter.ReturnCode processAntiTombstone() throws IOException {
        log("processAntiTombstone");
        rowState.setAntiTombstoneTimestamp(keyValue.getTimestamp());
        return SKIP;
    }

    /**
     * Handle a cell that represents user data. Filter it based on the various timestamps and transaction status.
     */
    private Filter.ReturnCode processUserData() throws IOException {
            return filterUserDataByTimestamp();
    }

    /**
     * Consider a cell of user data and decide whether to use it as "the" value for the column (in the context of the
     * current transaction) based on the various timestamp values and transaction status.
     */
    private Filter.ReturnCode filterUserDataByTimestamp() throws IOException {
        if (tombstoneAfterData()) {
            return NEXT_COL;
        } else if (isVisibleToCurrentTransaction()) {
            proceedToNextColumn();
            return INCLUDE;
        } else {
            return SKIP;
        }
    }

    /**
     * The version of HBase we are using does not support the newer "INCLUDE & NEXT_COL" return code
     * so this manually does the equivalent.
     */
    private void proceedToNextColumn() {
        rowState.lastValidQualifier = keyValue;
    }

    /**
     * Is there a row level tombstone that supercedes the current cell?
     */
    private boolean tombstoneAfterData() throws IOException {
    	final long[] buffer = rowState.tombstoneTimestamps.buffer;
    	final int size = rowState.tombstoneTimestamps.size();
    	for (int i = 0; i < size; i++) {
            Transaction tombstoneTransaction = rowState.transactionCache.get(buffer[i]);
            if (tombstoneTransaction == null) {
            	tombstoneTransaction = processDataTimestamp(buffer[i]);	
            }
            final VisibleResult visibleResult = checkVisibility(tombstoneTransaction);
            if (visibleResult.visible && (keyValue.getTimestamp() <= buffer[i])) {
                return true;
            }
        }

    	final long[] buffer2 = rowState.antiTombstoneTimestamps.buffer;
    	final int size2 = rowState.antiTombstoneTimestamps.size();
    	for (int i = 0; i < size2; i++) {
            Transaction tombstoneTransaction = rowState.transactionCache.get(buffer2[i]);
            if (tombstoneTransaction == null) {
            	tombstoneTransaction = processDataTimestamp(buffer2[i]);	
            }
            final VisibleResult visibleResult = checkVisibility(tombstoneTransaction);
            if (visibleResult.visible && (keyValue.getTimestamp() < buffer2[i])) {
                return true;
            }
        }

        return false;
    }

    /**
     * Should the current cell be visible to the current transaction? This is the core of the filtering of the data
     * under SI. It handles the various cases of when data should be visible.
     */
    private boolean isVisibleToCurrentTransaction() throws IOException {
        if (keyValue.getTimestamp() == myTransaction.getTransactionId().getId() && !myTransaction.getTransactionId().independentReadOnly) {
            return true;
        } else {
            final Transaction transaction = loadTransaction();
            return checkVisibility(transaction).visible;
        }
    }

    /**
     * Retrieve the transaction for the current cell from the local cache.
     */
    private Transaction loadTransaction() throws IOException {
        final Transaction transaction = rowState.transactionCache.get(keyValue.getTimestamp());
				assert transaction!=null : "All transaction should already be loaded from the si family for the data row, txn id: " + keyValue.getTimestamp();

        return transaction;
    }

    /**
     * This is not expected to happen, but if there are extra unknown column families in the results they will be skipped.
     */
    private Filter.ReturnCode processUnknownFamilyData() {
        log("processUnknownFamilyData");
        return SKIP;
    }

    private void log(String message) {
        //System.out.println("  " + message);
    }

    @Override
    public org.apache.hadoop.hbase.KeyValue produceAccumulatedKeyValue() {
        return null;
    }

    @Override
    public boolean getExcludeRow() {
        return false;
    }
}
