package com.splicemachine.si.impl;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.RollForward;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.store.ActiveTxnCacheSupplier;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Captures the SI logic to perform when a data table is compacted (without explicit HBase dependencies). Provides the
 * guts for SICompactionScanner.
 * <p/>
 * It is handed key-values and can change them.
 */
public class SICompactionState<Mutation,
        Put extends OperationWithAttributes, Delete, Get extends OperationWithAttributes, Scan, IHTable> {
    private static final Logger LOG = Logger.getLogger(SICompactionState.class);
//    private final SDataLib<Put, Delete, Get, Scan> dataLib;
    private final DataStore<Mutation, Put, Delete, Get, Scan, IHTable> dataStore;
    private final TxnSupplier transactionStore;
    public SortedSet<KeyValue> dataToReturn;
    private final RollForward rollForward;
    private ByteSlice rowSlice = new ByteSlice();

    public SICompactionState(DataStore dataStore, TxnSupplier transactionStore,RollForward rollForward) {
        this.dataStore = dataStore;
        this.rollForward = rollForward;
        this.transactionStore = new ActiveTxnCacheSupplier(transactionStore,SIConstants.activeTransactionCacheSize); //cache active transactions during our scan
        this.dataToReturn  = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
    }

    /**
     * Given a list of key-values, populate the results list with possibly mutated values.
     *
     * @param rawList - the input of key values to process
     * @param results - the output key values
     */
    public void mutate(List<KeyValue> rawList, List<KeyValue> results) throws IOException {
        dataToReturn.clear();
        for (KeyValue aRawList : rawList) {
            mutate(aRawList);
        }
        results.addAll(dataToReturn);
    }

    /**
     * Apply SI mutation logic to an individual key-value. Return the "new" key-value.
     */
    private void mutate(KeyValue keyValue) throws IOException {
        final KeyValueType keyValueType = dataStore.getKeyValueType(keyValue);
        long timestamp = keyValue.getTimestamp();
        switch (keyValueType) {
            case COMMIT_TIMESTAMP:
                /*
                 * Older versions of SI code would put an "SI Fail" element in the commit timestamp
                 * field when a row has been rolled back. While newer versions will just outright delete the entry,
                 * we still need to deal with entries which are in the old form. As time goes on, this should
                 * be less and less frequent, but you still have to check
                 */
                ensureTransactionCached(timestamp,keyValue);
                dataToReturn.add(keyValue);
                return;
            case TOMBSTONE:
            case ANTI_TOMBSTONE:
            case USER_DATA:
                if(mutateCommitTimestamp(timestamp,keyValue))
                    dataToReturn.add(keyValue);
                return;
            default:
                if(LOG.isDebugEnabled()){
                    String fam = Bytes.toString(keyValue.getBuffer(),keyValue.getFamilyOffset(),keyValue.getFamilyLength());
                    String col = Bytes.toString(keyValue.getBuffer(),keyValue.getQualifierOffset(),keyValue.getQualifierLength());
                    LOG.debug("KeyValue with family " + fam + " and column " + col + " are not SI-managed, ignoring");
                }
                dataToReturn.add(keyValue);
        }
    }

    private void ensureTransactionCached(long timestamp,KeyValue keyValue) {
        if(!transactionStore.transactionCached(timestamp)){
            if(isFailedCommitTimestamp(keyValue)){
                transactionStore.cache(new RolledBackTxn(timestamp));
            }else if (keyValue.getValueLength()>0){ //shouldn't happen, but you never know
                long commitTs = Bytes.toLong(keyValue.getBuffer(), keyValue.getValueOffset(), keyValue.getValueLength());
                transactionStore.cache(new CommittedTxn(timestamp,commitTs));
            }
        }
    }

    /**
     * Replace unknown commit timestamps with actual commit times.
     */
    private boolean mutateCommitTimestamp(long timestamp,KeyValue keyValue) throws IOException {
        TxnView transaction = transactionStore.getTransaction(timestamp);
        if(transaction.getEffectiveState()== Txn.State.ROLLEDBACK){
            /*
             * This transaction has been rolled back, so just remove the data
             * from physical storage
             */
            recordResolved(keyValue,transaction);
            return false;
        }
        TxnView t = transaction;
        while(t.getState()== Txn.State.COMMITTED){
            t = t.getParentTxnView();
        }
        if(t==Txn.ROOT_TRANSACTION){
            /*
             * This element has been committed all the way to the user level, so a
             * commit timestamp can be placed on it.
             */
            long globalCommitTimestamp = transaction.getEffectiveCommitTimestamp();
            dataToReturn.add(newTransactionTimeStampKeyValue(keyValue,Bytes.toBytes(globalCommitTimestamp)));
            recordResolved(keyValue, transaction);
        }
        return true;
    }

    private void recordResolved(KeyValue keyValue, TxnView transaction) {
        rowSlice.set(keyValue.getBuffer(),keyValue.getRowOffset(),keyValue.getRowLength());
        rollForward.recordResolved(rowSlice,transaction.getTxnId());
    }

//    private Txn getFromCache(long timestamp) throws IOException {
//        Txn result = transactionStore.getTransaction(timestamp);
//        return result;
//    }

    public static KeyValue newTransactionTimeStampKeyValue(KeyValue keyValue, byte[] value) {
        return new KeyValue(keyValue.getBuffer(),keyValue.getRowOffset(),keyValue.getRowLength(),SIConstants.DEFAULT_FAMILY_BYTES,0,1,SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,0,1,keyValue.getTimestamp(), KeyValue.Type.Put,value,0,value==null ? 0 : value.length);
    }

//	public void close() {
//		evaluatedTransactions.close();
//	}

    public static boolean isFailedCommitTimestamp(KeyValue keyValue) {
        return keyValue.getValueLength() == 1 && keyValue.getBuffer()[keyValue.getValueOffset()] == SIConstants.SNAPSHOT_ISOLATION_FAILED_TIMESTAMP[0];
    }

//    public static int compareKeyValuesByTimestamp(KeyValue first, KeyValue second) {
//        return Bytes.compareTo(first.getBuffer(), first.getTimestampOffset(), KeyValue.TIMESTAMP_SIZE,
//                second.getBuffer(), second.getTimestampOffset(), KeyValue.TIMESTAMP_SIZE);
//    }
//	public static int compareKeyValuesByColumnAndTimestamp(KeyValue first, KeyValue second) {
//		int compare = Bytes.compareTo(first.getBuffer(), first.getQualifierOffset(), first.getQualifierLength(),
//				second.getBuffer(), second.getQualifierOffset(), second.getQualifierLength());
//		if (compare != 0)
//			return compare;
//		return compareKeyValuesByTimestamp(first,second);
//	}

}
