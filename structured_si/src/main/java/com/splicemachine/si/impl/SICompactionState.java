package com.splicemachine.si.impl;

import com.splicemachine.hbase.KeyValueUtils;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.data.api.SDataLib;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.OperationWithAttributes;

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
public class SICompactionState<Result,  Mutation,
        Put extends OperationWithAttributes, Delete, Get extends OperationWithAttributes, Scan, IHTable, Lock, OperationStatus> {
    private final SDataLib<Put, Delete, Get, Scan> dataLib;
    private final DataStore<Mutation, Put, Delete, Get, Scan, IHTable> dataStore;
    private final TransactionStore transactionStore;
    private final DecodedKeyValue<Result,  Put, Delete, Get, Scan> keyValue;

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
    public void mutate(List<KeyValue> rawList, List<KeyValue> results) throws IOException {
        for (KeyValue kv : rawList) {
            keyValue.setKeyValue(kv);
            results.add(mutate(keyValue));
        }
    }
    
    /**
     * Apply SI mutation logic to an individual key-value. Return the "new" key-value.
     */
    private KeyValue mutate(DecodedKeyValue<Result, Put, Delete, Get, Scan> kv) throws IOException {
        final KeyValueType keyValueType = dataStore.getKeyValueType(kv.keyValue());
        if (keyValueType.equals(KeyValueType.COMMIT_TIMESTAMP)){
            return mutateCommitTimestamp(kv);
        }else{
            return kv.keyValue();
        }
    }

    /**
     * Replace unknown commit timestamps with actual commit times.
     */
    private KeyValue mutateCommitTimestamp(DecodedKeyValue<Result, Put, Delete, Get, Scan> kv) throws IOException {
        KeyValue result = kv.keyValue();
        if (dataStore.isSINull(result)) {
            final Transaction transaction = getFromCache(kv.timestamp());
            final TransactionStatus effectiveStatus = transaction.getEffectiveStatus();
            if (effectiveStatus.isFinished()) {
                final Long globalCommitTimestamp = transaction.getEffectiveCommitTimestamp();
                final byte[] commitTimestampValue = effectiveStatus.isCommitted() ?
                        dataLib.encode(globalCommitTimestamp) :
                        dataStore.siFail;
                result = KeyValueUtils.newKeyValue(kv.keyValue(), commitTimestampValue);
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
