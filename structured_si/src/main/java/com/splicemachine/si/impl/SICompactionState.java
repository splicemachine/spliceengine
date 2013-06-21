package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Captures the SI logic to perform when a data table is compacted (without explicit HBase dependencies). Provides the
 * guts for SICompactionScanner.
 * <p/>
 * It is handed key-values and can change them.
 */
public class SICompactionState<Data, Hashable, Result, KeyValue, OperationWithAttributes, Put extends OperationWithAttributes, Delete, Get extends OperationWithAttributes, Scan, IHTable, Lock> {
    private final SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock> dataLib;
    private final DataStore<Data, Hashable, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, IHTable, Lock> dataStore;
    private final TransactionStore transactionStore;
    private final DecodedKeyValue<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Lock> keyValue;

    /**
     * Cache of transactions that have been read during the execution of this compaction.
     */
    private final Map<Long, Transaction> transactionCache = new HashMap<Long, Transaction>();

    public SICompactionState(SDataLib dataLib, DataStore dataStore, TransactionStore transactionStore) {
        this.dataLib = dataLib;
        this.keyValue = new DecodedKeyValue(dataLib);
        this.dataStore = dataStore;
        this.transactionStore = transactionStore;
    }

    /**
     * Given a list of key-values, populate the results list with possibly mutated values.
     *
     * @param rawList - the input of key values to process
     * @param results - the output key values
     */
    void mutate(List<KeyValue> rawList, List<KeyValue> results) throws IOException {
        for (KeyValue kv : rawList) {
            keyValue.setKeyValue(kv);
            results.add(mutate(keyValue));
        }
    }

    /**
     * Apply SI mutation logic to an individual key-value. Return the "new" key-value.
     */
    private KeyValue mutate(DecodedKeyValue<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Lock> kv) throws IOException {
        final KeyValueType keyValueType = dataStore.getKeyValueType(kv.family(), kv.qualifier());
        if (keyValueType.equals(KeyValueType.COMMIT_TIMESTAMP)){
            return mutateCommitTimestamp(kv);
        }else{
            return kv.keyValue();
        }
    }

    /**
     * Replace unknown commit timestamps with actual commit times.
     */
    private KeyValue mutateCommitTimestamp(DecodedKeyValue<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Lock> kv) throws IOException {
        KeyValue result = kv.keyValue();
        if (dataStore.isSINull(kv.value())) {
            final Transaction transaction = getFromCache(kv.timestamp());
            final TransactionStatus effectiveStatus = transaction.getEffectiveStatus();
            if (effectiveStatus.isFinished()) {
                final Long globalCommitTimestamp = transaction.getEffectiveCommitTimestamp();
                final Data commitTimestampValue = effectiveStatus.isCommitted() ?
                        dataLib.encode(globalCommitTimestamp) :
                        dataStore.siFail;
                result = dataLib.newKeyValue(kv.row(), kv.family(), kv.qualifier(), kv.timestamp(), commitTimestampValue);
            }
        }
        return result;
    }

    private Transaction getFromCache(long timestamp) throws IOException {
        Transaction result = transactionCache.get(timestamp);
        if (result == null) {
            result = transactionStore.getTransaction(timestamp);
            transactionCache.put(timestamp, result);
        }
        return result;
    }

}
