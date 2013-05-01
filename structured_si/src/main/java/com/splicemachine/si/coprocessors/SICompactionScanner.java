package com.splicemachine.si.coprocessors;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.DecodedKeyValue;
import com.splicemachine.si.impl.KeyValueType;
import com.splicemachine.si.impl.Transaction;
import com.splicemachine.si.impl.TransactionStore;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SICompactionScanner implements InternalScanner {
    private final InternalScanner delegate;
    private final SDataLib dataLib;
    private final DataStore dataStore;
    private final TransactionStore transactionStore;
    private final Map<Long, Transaction> transactionCache = new HashMap<Long, Transaction>();

    public SICompactionScanner(DataStore dataStore, SDataLib dataLib, TransactionStore transactionStore,
                               InternalScanner scanner) {
        this.dataStore = dataStore;
        this.dataLib = dataLib;
        this.transactionStore = transactionStore;
        this.delegate = scanner;
    }

    @Override
    public boolean next(List<KeyValue> results) throws IOException {
        List<KeyValue> rawList = new ArrayList<KeyValue>();
        final boolean more = delegate.next(rawList);
        mutate(rawList, results);
        return more;
    }

    @Override
    public boolean next(List<KeyValue> results, int limit) throws IOException {
        List<KeyValue> rawList = new ArrayList<KeyValue>(limit);
        final boolean more = delegate.next(rawList, limit);
        mutate(rawList, results);
        return more;
    }

    private void mutate(List<KeyValue> rawList, List<KeyValue> results) throws IOException {
        for (KeyValue kv : rawList) {
            results.add(mutate(kv));
        }
    }

    private KeyValue mutate(KeyValue kv) throws IOException {
        DecodedKeyValue keyValue = new DecodedKeyValue(dataLib, kv);
        final KeyValueType keyValueType = dataStore.getKeyValueType(keyValue.family, keyValue.qualifier);
        if (keyValueType.equals(KeyValueType.COMMIT_TIMESTAMP)) {
            final Transaction transaction = getFromCache(keyValue.timestamp);
            if (transaction.isCommitted()) {
                return (KeyValue) dataLib.newKeyValue(keyValue.row, keyValue.family, keyValue.qualifier, keyValue.timestamp,
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

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}