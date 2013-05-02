package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SICompactionState {
    private final SDataLib dataLib;
    private final DataStore dataStore;
    private final TransactionStore transactionStore;
    private final Map<Long, Transaction> transactionCache = new HashMap<Long, Transaction>();

    public SICompactionState(SDataLib dataLib, DataStore dataStore, TransactionStore transactionStore) {
        this.dataLib = dataLib;
        this.dataStore = dataStore;
        this.transactionStore = transactionStore;
    }

    public void mutate(List rawList, List results) throws IOException {
        for (Object kv : rawList) {
            results.add(mutate(kv));
        }
    }

    private Object mutate(Object kv) throws IOException {
        DecodedKeyValue keyValue = new DecodedKeyValue(dataLib, kv);
        final KeyValueType keyValueType = dataStore.getKeyValueType(keyValue.family, keyValue.qualifier);
        if (keyValueType.equals(KeyValueType.COMMIT_TIMESTAMP)) {
            final Transaction transaction = getFromCache(keyValue.timestamp);
            if (transaction.isCommitted()) {
                return dataLib.newKeyValue(keyValue.row, keyValue.family, keyValue.qualifier, keyValue.timestamp,
                        dataLib.encode(transaction.commitTimestamp));
            } else {
                return kv;
            }
        } else {
            return kv;
        }
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
