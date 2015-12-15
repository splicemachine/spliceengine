package com.splicemachine.si.impl.server;

import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.storage.CellType;
import com.splicemachine.si.impl.store.ActiveTxnCacheSupplier;
import com.splicemachine.si.impl.txn.CommittedTxn;
import com.splicemachine.si.impl.txn.RolledBackTxn;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceLogUtils;
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
public class SICompactionState<OperationWithAttributes,Data,Delete extends OperationWithAttributes,
    Put extends OperationWithAttributes, Filter, Get extends OperationWithAttributes,
        OperationStatus,RegionScanner,Result,Scan extends OperationWithAttributes> {
    private static final Logger LOG = Logger.getLogger(SICompactionState.class);
    private final DataStore<OperationWithAttributes,Data,Delete,Filter,
                Get,
            Put,RegionScanner,Result, Scan> dataStore;
    private final TxnSupplier transactionStore;
    public SortedSet<Data> dataToReturn;
    private final RollForward rollForward;
    private ByteSlice rowSlice = new ByteSlice();

    public SICompactionState(DataStore dataStore, TxnSupplier transactionStore,RollForward rollForward,int activeTransactionCacheSize) {
        this.dataStore = dataStore;
        this.rollForward = rollForward;
        this.transactionStore = new ActiveTxnCacheSupplier(transactionStore,activeTransactionCacheSize);
        this.dataToReturn  = new TreeSet<Data>(dataStore.dataLib.getComparator());
    }

    /**
     * Given a list of key-values, populate the results list with possibly mutated values.
     *
     * @param rawList - the input of key values to process
     * @param results - the output key values
     */
    public void mutate(List<Data> rawList, List<Data> results) throws IOException {
        dataToReturn.clear();
        for (Data aRawList : rawList) {
            mutate(aRawList);
        }
        results.addAll(dataToReturn);
    }

    /**
     * Apply SI mutation logic to an individual key-value. Return the "new" key-value.
     */
    private void mutate(Data element) throws IOException {
        final CellType cellType= dataStore.getKeyValueType(element);
        long timestamp = dataStore.dataLib.getTimestamp(element);
        switch (cellType) {
            case COMMIT_TIMESTAMP:
                /*
                 * Older versions of SI code would put an "SI Fail" element in the commit timestamp
                 * field when a row has been rolled back. While newer versions will just outright delete the entry,
                 * we still need to deal with entries which are in the old form. As time goes on, this should
                 * be less and less frequent, but you still have to check
                 */
                ensureTransactionCached(timestamp,element);
                dataToReturn.add(element);
                return;
            case TOMBSTONE:
            case ANTI_TOMBSTONE:
            case USER_DATA:
                if(mutateCommitTimestamp(timestamp,element))
                    dataToReturn.add(element);
                return;
            default:
                if(LOG.isDebugEnabled()){
                       SpliceLogUtils.debug(LOG,"KeyValue with family %s and column %s are not SI-managed, ignoring",dataStore.dataLib.getFamilyAsString(element),dataStore.dataLib.getQualifierAsString(element));
                }
                dataToReturn.add(element);
        }
    }

    private void ensureTransactionCached(long timestamp,Data element) {
        if(!transactionStore.transactionCached(timestamp)){
            if(isFailedCommitTimestamp(element)){
                transactionStore.cache(new RolledBackTxn(timestamp));
            }else if (dataStore.dataLib.getValueLength(element)>0){ //shouldn't happen, but you never know
                long commitTs = dataStore.dataLib.getValueToLong(element);
                transactionStore.cache(new CommittedTxn(timestamp,commitTs));
            }
        }
    }

    /**
     * Replace unknown commit timestamps with actual commit times.
     */
    private boolean mutateCommitTimestamp(long timestamp,Data element) throws IOException {
        TxnView transaction = transactionStore.getTransaction(timestamp);
        if(transaction.getEffectiveState()== Txn.State.ROLLEDBACK){
            /*
             * This transaction has been rolled back, so just remove the data
             * from physical storage
             */
            recordResolved(element,transaction);
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
            dataToReturn.add(newTransactionTimeStampKeyValue(element, Bytes.toBytes(globalCommitTimestamp)));
            recordResolved(element, transaction);
        }
        return true;
    }

    private void recordResolved(Data element, TxnView transaction) {
    	dataStore.dataLib.setRowInSlice(element, rowSlice);
        rollForward.recordResolved(rowSlice,transaction.getTxnId());
    }

    public Data newTransactionTimeStampKeyValue(Data element, byte[] value) {
    	return dataStore.dataLib.newTransactionTimeStampKeyValue(element, value);
    }


    public boolean isFailedCommitTimestamp(Data element) {
    	return dataStore.dataLib.isFailedCommitTimestamp(element);
    }

}
