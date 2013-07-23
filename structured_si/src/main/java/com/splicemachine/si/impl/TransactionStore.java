package com.splicemachine.si.impl;

import com.splicemachine.si.api.TransactorListener;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Library of functions used by the SI module when accessing the transaction table. Encapsulates low-level data access
 * calls so the other classes can be expressed at a higher level. The intent is to capture mechanisms here rather than
 * policy.
 */
public class TransactionStore<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Lock, Table, OperationStatus> {
    static final Logger LOG = Logger.getLogger(TransactionStore.class);

    // Plugins for creating gets/puts against the transaction table and for running the operations.
    private final SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus> dataLib;
    private final STableReader<Table, Result, Get, Scan> reader;
    private final STableWriter writer;
    private final TransactionSchema transactionSchema;
    private final EncodedTransactionSchema<Data> encodedSchema;

    // Configure how long to wait in the event of a commit race.
    private int waitForCommittingMS;

    // Callback for transaction status change events.
    private final TransactorListener listener;

    /**
     * The immutable parts (e.g. id, begin time, parent, transaction type) never change and can be cached.
     */
    private final Map<Long, ImmutableTransaction> immutableTransactionCache;

    /**
     * Cache for transactions that have not yet reached a final state. This can be used for satisfying transaction
     * lookups as long as the cached value is more recent than the perspective of the requester.
     */
    private final Map<Long, ActiveTransactionCacheEntry> activeTransactionCache;

    /**
     * Cache for transactions that have reached a final state. They are now fully immutable and can be cached aggressively.
     */
    private final Map<Long, Transaction> completedTransactionCache;

    /**
     * In some cases, all that we care about for committed/failed transactions is their global begin/end timestamps and
     * their status. These caches are for these "stub" transaction objects. These objects are immutable and can be cached.
     */
    private final Map<Long, Transaction> stubCommittedTransactionCache;
    private final Map<Long, Transaction> stubFailedTransactionCache;

    public TransactionStore(TransactionSchema transactionSchema, SDataLib dataLib,
                            STableReader reader, STableWriter writer,
                            Map<Long, ImmutableTransaction> immutableTransactionCache,
                            Map<Long, ActiveTransactionCacheEntry> activeTransactionCache,
                            Map<Long, Transaction> completedTransactionCache,
                            Map<Long, Transaction> stubCommittedTransactionCache,
                            Map<Long, Transaction> stubFailedTransactionCache,
                            int waitForCommittingMS, TransactorListener listener) {
        this.transactionSchema = transactionSchema;
        this.encodedSchema = transactionSchema.encodedSchema(dataLib);
        this.dataLib = dataLib;
        this.reader = reader;
        this.activeTransactionCache = activeTransactionCache;
        this.completedTransactionCache = completedTransactionCache;
        this.stubCommittedTransactionCache = stubCommittedTransactionCache;
        this.stubFailedTransactionCache = stubFailedTransactionCache;
        this.immutableTransactionCache = immutableTransactionCache;
        this.writer = writer;
        this.waitForCommittingMS = waitForCommittingMS;
        this.listener = listener;
    }

    // Write (i.e. "record") transaction information to the transaction table.

    public void recordNewTransaction(long startTransactionTimestamp, TransactionParams params,
                                     TransactionStatus status, long beginTimestamp, long counter) throws IOException {
        writePut(buildCreatePut(startTransactionTimestamp, params, status, beginTimestamp, counter));
    }

    public boolean recordTransactionCommit(long startTransactionTimestamp, long commitTimestamp,
                                           Long globalCommitTimestamp, TransactionStatus expectedStatus,
                                           TransactionStatus newStatus) throws IOException {
        Tracer.traceStatus(startTransactionTimestamp, newStatus, true);
        try {
            return writePut(buildCommitPut(startTransactionTimestamp, commitTimestamp, globalCommitTimestamp, newStatus),
                    (expectedStatus == null) ? null : encodeStatus(expectedStatus));
        } finally {
            Tracer.traceStatus(startTransactionTimestamp, newStatus, false);
        }
    }

    public boolean recordTransactionStatusChange(long startTransactionTimestamp, TransactionStatus expectedStatus,
                                                 TransactionStatus newStatus)
            throws IOException {
        Tracer.traceStatus(startTransactionTimestamp, newStatus, true);
        try {
            return writePut(buildStatusUpdatePut(startTransactionTimestamp, newStatus), encodeStatus(expectedStatus));
        } finally {
            Tracer.traceStatus(startTransactionTimestamp, newStatus, false);
        }
    }

    public void recordTransactionKeepAlive(long startTransactionTimestamp)
            throws IOException {
        writePut(buildKeepAlivePut(startTransactionTimestamp));
    }

    // Internal functions to construct operations to update the transaction table.

    private Put buildCreatePut(long transactionId, TransactionParams params, TransactionStatus status,
                               long beginTimestamp, long counter) {
        Put put = buildBasePut(transactionId);
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

    private Put buildStatusUpdatePut(long transactionId, TransactionStatus newStatus) {
        Put put = buildBasePut(transactionId);
        addFieldToPut(put, encodedSchema.statusQualifier, newStatus.ordinal());
        return put;
    }

    private Put buildCommitPut(long transactionId, long commitTimestamp, Long globalCommitTimestamp,
                               TransactionStatus newStatus) {
        Put put = buildBasePut(transactionId);
        addFieldToPut(put, encodedSchema.commitQualifier, commitTimestamp);
        addFieldToPut(put, encodedSchema.statusQualifier, newStatus.ordinal());
        if (globalCommitTimestamp != null) {
            addFieldToPut(put, encodedSchema.globalCommitQualifier, globalCommitTimestamp);
        }
        return put;
    }

    private Put buildKeepAlivePut(long transactionId) {
        Put put = buildBasePut(transactionId);
        addFieldToPut(put, encodedSchema.keepAliveQualifier, encodedSchema.siNull);
        return put;
    }

    private Put buildBasePut(long transactionId) {
        Data rowKey = dataLib.newRowKey(new Object[]{transactionIdToRowKey(transactionId)});
        return dataLib.newPut(rowKey);
    }

    private void addFieldToPut(Put put, Data qualifier, Object value) {
        dataLib.addKeyValueToPut(put, encodedSchema.siFamily, qualifier, null, dataLib.encode(value));
    }

    // Apply operations to the transaction table

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

    // Load transactions from the cache or the underlying transaction table.

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

    public Transaction getTransaction(TransactionId transactionId) throws IOException {
        return getTransaction(transactionId.getId());
    }

    public Transaction getTransaction(long transactionId) throws IOException {
        return getTransactionDirect(transactionId, false);
    }

    /**
     * Retrieve the transaction object for the given transactionId. Specifically retrieve a representation that is no
     * older than perspectiveTimestamp.
     * This function assumes that the time represented by the perspectiveTimestamp value is in the past. If this is
     * violated then this function will give incorrect results. In effect the perspectiveTimestamp is used as an
     * update regarding what time it is (i.e. the current time is something later than perspectiveTimestamp).
     *
     * @param transactionId
     * @param perspectiveTimestamp
     * @return
     * @throws IOException
     */
    public Transaction getTransactionAsOf(long transactionId, long perspectiveTimestamp) throws IOException {
        // If a transaction has completed then it won't change and we can use the cached value.
        final Transaction cachedTransaction = completedTransactionCache.get(transactionId);
        if (cachedTransaction != null) {
            return cachedTransaction;
        }
        final ActiveTransactionCacheEntry activeEntry = activeTransactionCache.get(transactionId);
        if (activeEntry != null && activeEntry.effectiveTimestamp >= perspectiveTimestamp) {
            return activeEntry.transaction;
        }
        final Transaction transaction = loadTransaction(transactionId, false);
        activeTransactionCache.put(transactionId, new ActiveTransactionCacheEntry(perspectiveTimestamp, transaction));
        return transaction;
    }

    // Internal helper functions for retrieving transactions from the caches or the transaction table.

    private ImmutableTransaction getImmutableTransactionFromCache(long transactionId) throws IOException {
        ImmutableTransaction immutableCachedTransaction = immutableTransactionCache.get(transactionId);
        if (immutableCachedTransaction != null) {
            return immutableCachedTransaction;
        }
        // Since the ImmutableTransaction part of a transaction never changes, if we have a Transaction object cached
        // anywhere, then we can use it.
        final Transaction cachedTransaction = completedTransactionCache.get(transactionId);
        if (cachedTransaction != null) {
            return cachedTransaction;
        }
        final ActiveTransactionCacheEntry activeCachedTransaction = activeTransactionCache.get(transactionId);
        if (activeCachedTransaction != null) {
            return activeCachedTransaction.transaction;
        }
        immutableCachedTransaction = getImmutableTransactionDirect(transactionId);
        immutableTransactionCache.put(transactionId, immutableCachedTransaction);
        return immutableCachedTransaction;
    }

    private Transaction getImmutableTransactionDirect(long transactionId) throws IOException {
        return getTransactionDirect(transactionId, true);
    }

    private Transaction getTransactionDirect(long transactionId, boolean immutableOnly) throws IOException {
        // If the transaction is completed then we will use the cached value, otherwise load it from the transaction table.
        final Transaction cachedTransaction = completedTransactionCache.get(transactionId);
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
            if (transaction.status.isCommitting()) {
                // It is important to avoid exposing the application to transactions that are in the intermediate
                // COMMITTING state. So wait here for the commit to complete.
                try {
                    Tracer.traceWaiting(transactionId);
                    Thread.sleep(waitForCommittingMS);
                } catch (InterruptedException e) {
                    // Ignore this
                }
                transaction = loadTransactionDirect(transactionId);
                if (transaction.status.isCommitting()) {
                    throw new DoNotRetryIOException("Transaction is committing: " + transactionId);
                }
            }
            return transaction;
        }
    }

    /**
     * Load a transaction from the underlying transaction table. All reads of the transaction table are expected to go
     * through here.
     *
     * @param transactionId
     * @return
     * @throws IOException
     */
    private Transaction loadTransactionDirect(long transactionId) throws IOException {
        if (transactionId == Transaction.ROOT_ID) {
            return Transaction.rootTransaction;
        }
        Data tupleKey = dataLib.newRowKey(new Object[]{transactionIdToRowKey(transactionId)});
        Table transactionTable = reader.open(transactionSchema.tableName);
        try {
            Get get = dataLib.newGet(tupleKey, null, null, null);
            Result resultTuple = reader.get(transactionTable, get);
            if (resultTuple != null) {
                listener.loadTransaction();
                final Transaction result = decodeTransactionResults(transactionId, resultTuple);
                cacheCompletedTransactions(transactionId, result);
                return result;
            }
        } finally {
            reader.close(transactionTable);
        }
        throw new RuntimeException("transaction ID not found: " + transactionId);
    }

    private void cacheCompletedTransactions(long transactionId, Transaction result) {
        if (result.getEffectiveStatus().isFinished()) {
            // If a transaction has reached a terminal status, then we can cache it for future reference.
            completedTransactionCache.put(transactionId, result);
            //LOG.warn("cache PUT " + transactionId.getTransactionIdString());
        } else {
            //LOG.warn("cache NOT " + transactionId.getTransactionIdString());
        }
    }

    // Decoding results from the transaction table.

    /**
     * Read the contents of a Result object (representing a row from the transaction table) and produce a Transaction
     * object.
     *
     * @param transactionId
     * @param resultTuple
     * @return
     * @throws IOException
     */
    private Transaction decodeTransactionResults(long transactionId, Result resultTuple) throws IOException {
        // TODO: create optimized versions of this code block that only load the data required by the caller, or make the loading lazy
        final Boolean dependent = decodeBoolean(resultTuple, encodedSchema.dependentQualifier);
        final TransactionBehavior transactionBehavior = dependent ?
                StubTransactionBehavior.instance :
                IndependentTransactionBehavior.instance;

        return new Transaction(transactionBehavior, transactionId,
                decodeLong(resultTuple, encodedSchema.startQualifier),
                decodeKeepAlive(resultTuple),
                decodeParent(resultTuple),
                dependent,
                decodeBoolean(resultTuple, encodedSchema.allowWritesQualifier),
                decodeBoolean(resultTuple, encodedSchema.readUncommittedQualifier),
                decodeBoolean(resultTuple, encodedSchema.readCommittedQualifier),
                decodeStatus(resultTuple, encodedSchema.statusQualifier),
                decodeLong(resultTuple, encodedSchema.commitQualifier),
                decodeLong(resultTuple, encodedSchema.globalCommitQualifier),
                decodeLong(resultTuple, encodedSchema.counterQualifier));
    }

    private long decodeKeepAlive(Result resultTuple) {
        final List<KeyValue> keepAliveValues = dataLib.getResultColumn(resultTuple, encodedSchema.siFamily, encodedSchema.keepAliveQualifier);
        final KeyValue keepAliveValue = keepAliveValues.get(0);
        return dataLib.getKeyValueTimestamp(keepAliveValue);
    }

    private Transaction decodeParent(Result resultTuple) throws IOException {
        Long parentId = decodeLong(resultTuple, encodedSchema.parentQualifier);
        if (parentId == null) {
            parentId = Transaction.ROOT_ID;
        }
        Transaction parent = null;
        if (parentId != null) {
            parent = getTransaction(parentId);
        }
        return parent;
    }

    private Long decodeLong(Result resultTuple, Data columnQualifier) {
        final Data columnValue = dataLib.getResultValue(resultTuple, encodedSchema.siFamily, columnQualifier);
        Long result = null;
        if (columnValue != null) {
            result = (Long) dataLib.decode(columnValue, Long.class);
        }
        return result;
    }

    private TransactionStatus decodeStatus(Result resultTuple, Data statusQualifier) {
        final Data statusValue = dataLib.getResultValue(resultTuple, encodedSchema.siFamily, statusQualifier);
        if (statusValue == null) {
            return null;
        } else {
            return TransactionStatus.values()[((Integer) dataLib.decode(statusValue, Integer.class))];
        }
    }

    private Boolean decodeBoolean(Result resultTuple, Data columnQualifier) {
        final Data columnValue = dataLib.getResultValue(resultTuple, encodedSchema.siFamily, columnQualifier);
        Boolean result = null;
        if (columnValue != null) {
            result = (Boolean) dataLib.decode(columnValue, Boolean.class);
        }
        return result;
    }

    // Misc

    /**
     * Generate a unique, monotonically increasing timestamp within the context of the given transaction ID. (i.e. it
     * is a "local" timestamp that is only unique and increasing for this transaction ID).
     *
     * @param transactionId
     * @return
     * @throws IOException
     */
    public long generateTimestamp(long transactionId) throws IOException {
        final Table transactionSTable = reader.open(transactionSchema.tableName);
        try {
            final Transaction transaction = loadTransactionDirect(transactionId);
            long current = transaction.counter;
            // TODO: more efficient mechanism for obtaining timestamp
            while (current - transaction.counter < 10000) {
                final long next = current + 1;
                final Put put = buildBasePut(transactionId);
                addFieldToPut(put, encodedSchema.counterQualifier, next);
                if (writer.checkAndPut(transactionSTable, encodedSchema.siFamily, encodedSchema.counterQualifier,
                        dataLib.encode(transaction.counter), put)) {
                    return next;
                } else {
                    current = next;
                }
            }
        } finally {
            reader.close(transactionSTable);
        }
        throw new RuntimeException("Unable to obtain timestamp");
    }

    // Pseudo Transaction constructors for transactions that are known to have committed or failed. These return "stub"
    // transaction records that can only be relied on to have correct begin/end timestamps and status.

    public Transaction makeStubCommittedTransaction(final long timestamp, final long globalCommitTimestamp) {
        // avoid using the cache get() that takes a loader to avoid the object creation cost of the anonymous inner class
        Transaction result = stubCommittedTransactionCache.get(timestamp);
        if (result == null) {
            result = new Transaction(StubTransactionBehavior.instance, timestamp,
                    timestamp, 0, Transaction.rootTransaction, true, false, false, false,
                    TransactionStatus.COMMITTED, globalCommitTimestamp, null, null);
            stubCommittedTransactionCache.put(timestamp, result);
        }
        return result;
    }

    public Transaction makeStubFailedTransaction(final long timestamp) {
        // avoid using the cache get() that takes a loader to avoid the object creation cost of the anonymous inner class
        Transaction result = stubFailedTransactionCache.get(timestamp);
        if (result == null) {
            result = new Transaction(StubTransactionBehavior.instance, timestamp,
                    timestamp, 0, Transaction.rootTransaction, true, false, false, false,
                    TransactionStatus.ERROR, null, null, null);
            stubFailedTransactionCache.put(timestamp, result);
        }
        return result;
    }

    // Internal utilities

    /**
     * Convert a transaction ID into the format/value used for the corresponding row key in the transaction table.
     * The row keys are non-sequential to avoid creating a hotspot in the table around a region that is hosting the
     * "current" transaction IDs.
     *
     * @param id
     * @return
     */
    private long transactionIdToRowKey(long id) {
        byte[] result = Bytes.toBytes(id);
        ArrayUtils.reverse(result);
        return Bytes.toLong(result);
    }

    /**
     * Convert a TransactionStatus into the representation used for it in the transaction table.
     *
     * @param status
     * @return
     */
    private Data encodeStatus(TransactionStatus status) {
        if (status == null) {
            return encodedSchema.siNull;
        } else {
            return dataLib.encode(status.ordinal());
        }
    }

}
