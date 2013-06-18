package com.splicemachine.si.impl;

import com.google.common.cache.Cache;
import com.splicemachine.si.api.TransactorListener;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Library of functions used by the SI module when accessing the transaction table. Encapsulates low-level data access
 * calls so the other classes can be expressed at a higher level.
 */
public class TransactionStore<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Lock, Table> {
    static final Logger LOG = Logger.getLogger(TransactionStore.class);

    private final SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock> dataLib;
    private final STableReader<Table, Result, Get, Scan> reader;
    private final Cache<Long, ImmutableTransaction> immutableTransactionCache;
    private final Cache<Long, ActiveTransactionCacheEntry> activeTransactionCache;
    private final Cache<Long, Transaction> transactionCache;
    private final Cache<Long, Transaction> committedTransactionCache;
    private final Cache<Long, Transaction> failedTransactionCache;
    private final STableWriter writer;

    private final TransactionSchema transactionSchema;
    private final EncodedTransactionSchema<Data> encodedSchema;
    private int waitForCommittingMS;

    private final TransactorListener listener;

    public TransactionStore(TransactionSchema transactionSchema, SDataLib dataLib,
                            STableReader reader, STableWriter writer,
                            Cache<Long, ImmutableTransaction> immutableTransactionCache,
                            Cache<Long, ActiveTransactionCacheEntry> activeTransactionCache,
                            Cache<Long, Transaction> transactionCache,
                            Cache<Long, Transaction> committedTransactionCache,
                            Cache<Long, Transaction> failedTransactionCache,
                            int waitForCommittingMS, TransactorListener listener) {
        this.transactionSchema = transactionSchema;
        this.encodedSchema = transactionSchema.encodedSchema(dataLib);
        this.dataLib = dataLib;
        this.reader = reader;
        this.activeTransactionCache = activeTransactionCache;
        this.transactionCache = transactionCache;
        this.committedTransactionCache = committedTransactionCache;
        this.failedTransactionCache = failedTransactionCache;
        this.immutableTransactionCache = immutableTransactionCache;
        this.writer = writer;
        this.waitForCommittingMS = waitForCommittingMS;
        this.listener = listener;
    }

    public Transaction makeCommittedTransaction(final long timestamp, final long globalCommitTimestamp) {
        // avoid using the cache get() that takes a loader to avoid the object creation cost of the anonymous inner class
        Transaction result = committedTransactionCache.getIfPresent(timestamp);
        if (result == null) {
            result = new Transaction(DefaultTransactionBehavior.instance, timestamp,
                    timestamp, 0, Transaction.getRootTransaction(), true, null, false, false, false,
                    TransactionStatus.COMMITTED, globalCommitTimestamp, null, null);
            committedTransactionCache.put(timestamp, result);
        }
        return result;
    }

    public Transaction makeFailedTransaction(final long timestamp) {
        // avoid using the cache get() that takes a loader to avoid the object creation cost of the anonymous inner class
        Transaction result = failedTransactionCache.getIfPresent(timestamp);
        if (result == null) {
            result = new Transaction(DefaultTransactionBehavior.instance, timestamp,
                    timestamp, 0, Transaction.getRootTransaction(), true, null, false, false, false,
                    TransactionStatus.ERROR, null, null, null);
            failedTransactionCache.put(timestamp, result);
        }
        return result;
    }

    public void recordNewTransaction(long startTransactionTimestamp, TransactionParams params,
                                     TransactionStatus status, long beginTimestamp, long counter) throws IOException {
        writePut(makeCreateTuple(startTransactionTimestamp, params, status, beginTimestamp, counter));
    }

    public void addChildToTransaction(long transactionId, long childTransactionId) throws IOException {
        if (transactionId != Transaction.ROOT_ID) {
            Put put = makeBasePut(transactionId);
            dataLib.addKeyValueToPut(put, encodedSchema.siChildrenFamily,
                    dataLib.encode(String.valueOf(childTransactionId)), null,
                    dataLib.encode(true));
            writePut(put);
        }
    }

    public boolean recordTransactionEnd(long startTransactionTimestamp, long commitTimestamp,
                                        Long globalCommitTimestamp, TransactionStatus expectedStatus,
                                        TransactionStatus newStatus) throws IOException {
        Tracer.traceStatus(startTransactionTimestamp, newStatus, true);
        try {
            return writePut(makeCommitPut(startTransactionTimestamp, commitTimestamp, globalCommitTimestamp, newStatus),
                    (expectedStatus == null) ? null : encodedStatus(expectedStatus));
        } finally {
            Tracer.traceStatus(startTransactionTimestamp, newStatus, false);
        }
    }

    private Data encodedStatus(TransactionStatus status) {
        if (status == null) {
            return encodedSchema.siNull;
        } else {
            return dataLib.encode(status.ordinal());
        }
    }

    public boolean recordTransactionStatusChange(long startTransactionTimestamp, TransactionStatus expectedStatus,
                                                 TransactionStatus newStatus)
            throws IOException {
        Tracer.traceStatus(startTransactionTimestamp, newStatus, true);
        try {
            return writePut(makeStatusUpdateTuple(startTransactionTimestamp, newStatus), encodedStatus(expectedStatus));
        } finally {
            Tracer.traceStatus(startTransactionTimestamp, newStatus, false);
        }
    }

    public void recordKeepAlive(long startTransactionTimestamp)
            throws IOException {
        writePut(makeKeepAliveTuple(startTransactionTimestamp));
    }

    public ImmutableTransaction getImmutableTransaction(long beginTimestamp) throws IOException {
        return getImmutableTransaction(new TransactionId(beginTimestamp));
    }

    public ImmutableTransaction getImmutableTransaction(TransactionId transactionId) throws IOException {
        final ImmutableTransaction result = getImmutableTransactionFromCache(transactionId.getId());
        if (result.getTransactionId().equals(transactionId)) {
            return result;
        } else {
            return result.cloneWithId(transactionId, result);
        }
    }

    private ImmutableTransaction getImmutableTransactionFromCache(long transactionId) throws IOException {
        final Transaction cachedTransaction = transactionCache.getIfPresent(transactionId);
        if (cachedTransaction != null) {
            return cachedTransaction;
        }
        ImmutableTransaction immutableCachedTransaction = immutableTransactionCache.getIfPresent(transactionId);
        if (immutableCachedTransaction != null) {
            return immutableCachedTransaction;
        }
        immutableCachedTransaction = getImmutableTransactionDirect(transactionId);
        immutableTransactionCache.put(transactionId, immutableCachedTransaction);
        return immutableCachedTransaction;
    }

    public Transaction getTransaction(TransactionId transactionId) throws IOException {
        return getTransaction(transactionId.getId());
    }

    public Transaction getTransaction(long transactionId) throws IOException {
        return getTransactionDirect(transactionId, false);
    }

    public Transaction getImmutableTransactionDirect(long transactionId) throws IOException {
        return getTransactionDirect(transactionId, true);
    }

    public Transaction getTransactionAsOf(long beginTimestamp, long perspective) throws IOException {
        final Transaction cachedTransaction = transactionCache.getIfPresent(beginTimestamp);
        if (cachedTransaction != null) {
            return cachedTransaction;
        }
        final ActiveTransactionCacheEntry activeEntry = activeTransactionCache.getIfPresent(beginTimestamp);
        if (activeEntry != null && activeEntry.effectiveTimestamp >= perspective) {
            return activeEntry.transaction;
        }
        final Transaction transaction = loadTransaction(beginTimestamp, false);
        activeTransactionCache.put(beginTimestamp, new ActiveTransactionCacheEntry(perspective, transaction));
        return transaction;
    }

    private Transaction getTransactionDirect(long transactionId, boolean immutableOnly) throws IOException {
        final Transaction cachedTransaction = transactionCache.getIfPresent(transactionId);
        if (cachedTransaction != null) {
            //LOG.warn("cache HIT " + transactionId.getTransactionIdString());
            return cachedTransaction;
        }
        return loadTransaction(transactionId, immutableOnly);
    }

    private Transaction loadTransaction(long transactionId, boolean immutableOnly) throws IOException {
        if (immutableOnly) {
            return loadTransactionDirect(transactionId);
        } else {
            Transaction transaction = loadTransactionDirect(transactionId);
            if (transaction.isCommitting()) {
                try {
                    Tracer.traceWaiting(transactionId);
                    Thread.sleep(waitForCommittingMS);
                } catch (InterruptedException e) {
                    //ignore this
                }
                transaction = loadTransactionDirect(transactionId);
                if (transaction.isCommitting()) {
                    throw new DoNotRetryIOException("Transaction is committing: " + transactionId);
                }
            }
            return transaction;
        }
    }

    private Transaction loadTransactionDirect(long transactionId) throws IOException {
        if (transactionId == Transaction.ROOT_ID) {
            return Transaction.getRootTransaction();
        }
        Data tupleKey = dataLib.newRowKey(new Object[]{transactionIdToRowKey(transactionId)});

        Table transactionSTable = reader.open(transactionSchema.tableName);
        try {
            Get get = dataLib.newGet(tupleKey, null, null, null);
            Result resultTuple = reader.get(transactionSTable, get);
            if (resultTuple != null) {
                final List<KeyValue> keepAliveValues = dataLib.getResultColumn(resultTuple, encodedSchema.siFamily, encodedSchema.keepAliveQualifier);
                final KeyValue keepAliveValue = keepAliveValues.get(0);
                final long keepAlive = dataLib.getKeyValueTimestamp(keepAliveValue);

                TransactionStatus status = getTransactionStatusField(resultTuple, encodedSchema.statusQualifier);
                Long parentId = getLongField(resultTuple, encodedSchema.parentQualifier);
                if (parentId == null) {
                    parentId = Transaction.ROOT_ID;
                }
                Transaction parent = null;
                if (parentId != null) {
                    parent = getTransaction(parentId);
                }
                Long beginTimestamp = getLongField(resultTuple, encodedSchema.startQualifier);
                Long commitTimestamp = getLongField(resultTuple, encodedSchema.commitQualifier);
                Long globalCommitTimestamp = getLongField(resultTuple, encodedSchema.globalCommitQualifier);
                Map<Data, Data> childrenMap = dataLib.getResultFamilyMap(resultTuple, encodedSchema.siChildrenFamily);
                Long counter = getLongField(resultTuple, encodedSchema.counterQualifier);
                Set<Long> children = new HashSet<Long>();
                for (Data child : childrenMap.keySet()) {
                    children.add(Long.valueOf((String) dataLib.decode(child, String.class)));
                }

                final Boolean dependent = getBooleanFieldFromResult(resultTuple, encodedSchema.dependentQualifier);
                final Transaction result = new Transaction(
                        dependent ? DefaultTransactionBehavior.instance : IndependentTransactionBehavior.instance,
                        transactionId, beginTimestamp, keepAlive, parent, dependent, children,
                        getBooleanFieldFromResult(resultTuple, encodedSchema.allowWritesQualifier),
                        getBooleanFieldFromResult(resultTuple, encodedSchema.readUncommittedQualifier),
                        getBooleanFieldFromResult(resultTuple, encodedSchema.readCommittedQualifier),
                        status, commitTimestamp, globalCommitTimestamp, counter);
                if (!result.isEffectivelyActive()) {
                    transactionCache.put(transactionId, result);
                    //LOG.warn("cache PUT " + transactionId.getTransactionIdString());
                } else {
                    //LOG.warn("cache NOT " + transactionId.getTransactionIdString());
                }
                listener.loadTransaction();
                return result;
            }
        } finally {
            reader.close(transactionSTable);
        }
        throw new RuntimeException("transaction ID not found: " + transactionId);
    }

    private Long getLongField(Result resultTuple, Data commitQualifier) {
        final Data commitValue = dataLib.getResultValue(resultTuple, encodedSchema.siFamily, commitQualifier);
        Long commitTimestamp = null;
        if (commitValue != null) {
            commitTimestamp = (Long) dataLib.decode(commitValue, Long.class);
        }
        return commitTimestamp;
    }

    private TransactionStatus getTransactionStatusField(Result resultTuple, Data statusQualifier) {
        final Data statusValue = dataLib.getResultValue(resultTuple, encodedSchema.siFamily, statusQualifier);
        return (statusValue == null) ? null : TransactionStatus.values()[((Integer) dataLib.decode(statusValue, Integer.class))];
    }

    private Boolean getBooleanFieldFromResult(Result resultTuple, Data qualifier) {
        final Data value = dataLib.getResultValue(resultTuple, encodedSchema.siFamily, qualifier);
        Boolean result = null;
        if (value != null) {
            result = (Boolean) dataLib.decode(value, Boolean.class);
        }
        return result;
    }

    private Put makeStatusUpdateTuple(long transactionId, TransactionStatus newStatus) {
        Put put = makeBasePut(transactionId);
        addFieldToPut(put, encodedSchema.statusQualifier, newStatus.ordinal());
        return put;
    }

    private Put makeKeepAliveTuple(long transactionId) {
        Put put = makeBasePut(transactionId);
        addFieldToPut(put, encodedSchema.keepAliveQualifier, encodedSchema.siNull);
        return put;
    }

    private Put makeCreateTuple(long transactionId, TransactionParams params, TransactionStatus status,
                                   long beginTimestamp, long counter) {
        Put put = makeBasePut(transactionId);
        addFieldToPut(put, encodedSchema.dependentQualifier, params.dependent);
        addFieldToPut(put, encodedSchema.startQualifier, beginTimestamp);
        addFieldToPut(put, encodedSchema.counterQualifier, counter);
        addFieldToPut(put, encodedSchema.keepAliveQualifier, encodedSchema.siNull);
        if (params.parent != null && !params.parent.isRootTransaction()) {
            addFieldToPut(put, encodedSchema.parentQualifier, params.parent.getId());
        }
        addFieldToPut(put, encodedSchema.allowWritesQualifier, params.allowWrites);
        if (params.readUncommitted != null) {
            addFieldToPut(put, encodedSchema.readUncommittedQualifier, params.readUncommitted);
        }
        if (params.readCommitted != null) {
            addFieldToPut(put, encodedSchema.readCommittedQualifier, params.readCommitted);
        }
        if (status != null) {
            addFieldToPut(put, encodedSchema.statusQualifier, status.ordinal());
        }
        addFieldToPut(put, encodedSchema.idQualifier, transactionId);
        return put;
    }

    private Put makeCommitPut(long transactionId, long commitTimestamp, Long globalCommitTimestamp,
                                 TransactionStatus newStatus) {
        Put put = makeBasePut(transactionId);
        addFieldToPut(put, encodedSchema.commitQualifier, commitTimestamp);
        addFieldToPut(put, encodedSchema.statusQualifier, newStatus.ordinal());
        if (globalCommitTimestamp != null) {
            addFieldToPut(put, encodedSchema.globalCommitQualifier, globalCommitTimestamp);
        }
        return put;
    }

    private Put makeBasePut(long transactionId) {
        Data rowKey = dataLib.newRowKey(new Object[]{transactionIdToRowKey(transactionId)});
        return dataLib.newPut(rowKey);
    }

    private long transactionIdToRowKey(long id) {
        byte[] result = Bytes.toBytes(id);
        ArrayUtils.reverse(result);
        return Bytes.toLong(result);
    }

    private void addFieldToPut(Put put, Data qualifier, Object value) {
        dataLib.addKeyValueToPut(put, encodedSchema.siFamily, qualifier, null, dataLib.encode(value));
    }

    private void writePut(Put put) throws IOException {
        writePut(put, null);
    }

    private boolean writePut(Put put, Data expectedStatus) throws IOException {
        final Table transactionSTable = reader.open(transactionSchema.tableName);
        try {
            if (expectedStatus == null) {
                writer.write(transactionSTable, put);
                listener.writeTransaction();
                return true;
            } else {
                return writer.checkAndPut(transactionSTable, encodedSchema.siFamily, encodedSchema.statusQualifier,
                        expectedStatus, put);
            }
        } finally {
            reader.close(transactionSTable);
        }
    }

    public long getTimestamp(long transactionId) throws IOException {
        final Table transactionSTable = reader.open(transactionSchema.tableName);
        final Transaction transaction = loadTransactionDirect(transactionId);
        long current = transaction.counter;
        while (current - transaction.counter < 100) {
            final long next = current + 1;
            final Put put = makeBasePut(transactionId);
            addFieldToPut(put, encodedSchema.counterQualifier, next);
            if (writer.checkAndPut(transactionSTable, encodedSchema.siFamily, encodedSchema.counterQualifier,
                    dataLib.encode(transaction.counter), put)) {
                return next;
            } else {
                current = next;
            }
        }
        throw new RuntimeException("Unable to obtain timestamp");
    }

}
