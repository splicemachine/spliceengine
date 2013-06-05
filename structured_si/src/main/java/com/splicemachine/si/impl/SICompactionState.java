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
public class SICompactionState {
    private final SDataLib dataLib;
    private final DataStore dataStore;
    private final TransactionStore transactionStore;

    /**
     * Cache of transactions that have been read during the execution of this compaction.
     */
    private final Map<Long, Transaction> transactionCache = new HashMap<Long, Transaction>();

    public SICompactionState(SDataLib dataLib, DataStore dataStore, TransactionStore transactionStore) {
        this.dataLib = dataLib;
        this.dataStore = dataStore;
        this.transactionStore = transactionStore;
    }

    /**
     * Given a list of key-values, populate the results list with possibly mutated values.
     *
     * @param rawList - the input of key values to process
     * @param results - the output key values
     */
    public void mutate(List rawList, List results) throws IOException {
        for (Object kv : rawList) {
            results.add(mutate(kv));
        }
    }

    /**
     * Apply SI mutation logic to an individual key-value. Return the "new" key-value.
     */
    private Object mutate(Object kv) throws IOException {
        final KeyValueType keyValueType = dataStore.getKeyValueType(dataLib.getKeyValueFamily(kv),
                dataLib.getKeyValueQualifier(kv));
        if (keyValueType.equals(KeyValueType.COMMIT_TIMESTAMP)){
            return mutateCommitTimestamp(kv);
        }else{
            return kv;
        }
    }

    /**
     * Replace unknown commit timestamps with actual commit times.
     */
    private Object mutateCommitTimestamp(Object kv) throws IOException {
        Object result = kv;
        if (dataStore.isSINull(dataLib.getKeyValueValue(kv))) {
            final Transaction transaction = getFromCache(dataLib.getKeyValueTimestamp(kv));
            final TransactionStatus effectiveStatus = transaction.getEffectiveStatus();
            if (effectiveStatus.equals(TransactionStatus.COMMITTED)
                    || effectiveStatus.equals(TransactionStatus.ROLLED_BACK)
                    || effectiveStatus.equals(TransactionStatus.ERROR)) {
                final Long globalCommitTimestamp = transaction.getCommitTimestamp();
                final Object commitTimestampValue = effectiveStatus.equals(TransactionStatus.COMMITTED) ?
                        dataLib.encode(globalCommitTimestamp) :
                        dataStore.siFail;
                result = dataLib.newKeyValue(dataLib.getKeyValueRow(kv), dataLib.getKeyValueFamily(kv),
                        dataLib.getKeyValueQualifier(kv), dataLib.getKeyValueTimestamp(kv), commitTimestampValue);
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
