package com.splicemachine.si.impl;

import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.api.TransactorListener;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.impl.iterator.ContiguousIterator;
import com.splicemachine.si.impl.iterator.ContiguousIteratorFunctions;
import com.splicemachine.si.impl.iterator.DataIDDecoder;
import com.splicemachine.si.impl.iterator.OrderedMuxer;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Library of functions used by the SI module when accessing the transaction table. Encapsulates low-level data access
 * calls so the other classes can be expressed at a higher level. The intent is to capture mechanisms here rather than
 * policy.
 */
public class TransactionStore<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Lock, Table, OperationStatus, Scanner> {
    static final Logger LOG = Logger.getLogger(TransactionStore.class);

    // Plugins for creating gets/puts against the transaction table and for running the operations.
    private final SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus> dataLib;
    private final STableReader<Table, Result, Get, Scan, KeyValue, Scanner, Data> reader;
    private final STableWriter writer;
    private final TransactionSchema transactionSchema;
    private final EncodedTransactionSchema<Data> encodedSchema;
    private final List<Data> siFamilyList = new ArrayList<Data>(1);
    private final List<Data> permissionFamilyList = new ArrayList<Data>(1);

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

    private final Map<PermissionArgs, Byte> permissionCache;

    public TransactionStore(TransactionSchema transactionSchema, SDataLib dataLib,
                            STableReader reader, STableWriter writer,
                            Map<Long, ImmutableTransaction> immutableTransactionCache,
                            Map<Long, ActiveTransactionCacheEntry> activeTransactionCache,
                            Map<Long, Transaction> completedTransactionCache,
                            Map<Long, Transaction> stubCommittedTransactionCache,
                            Map<Long, Transaction> stubFailedTransactionCache,
                            Map<PermissionArgs, Byte> permissionCache,
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
        this.permissionCache = permissionCache;
        this.writer = writer;
        this.waitForCommittingMS = waitForCommittingMS;
        this.listener = listener;
        setupFamilyLists();
    }

    private void setupFamilyLists() {
        this.siFamilyList.add(this.encodedSchema.siFamily);
        this.permissionFamilyList.add(this.encodedSchema.permissionFamily);
    }

    // Write (i.e. "record") transaction information to the transaction table.

    public void recordNewTransaction(final long startTransactionTimestamp, final TransactionParams params,
                                     final TransactionStatus status, final long beginTimestamp, final long counter) throws IOException {
        withTransactionTable(new TransactionStoreCallback<Void, Table>() {
            @Override
            public Void withTable(Table transactionTable) throws IOException {
                if (!recordNewTransactionDirect(transactionTable, startTransactionTimestamp, params, status, beginTimestamp, counter)) {
                    throw new RuntimeException("create transaction failed");
                }
                return null;
            }
        });
    }

    public boolean recordNewTransactionDirect(Table transactionTable, long startTransactionTimestamp, TransactionParams params,
                                              TransactionStatus status, long beginTimestamp, long counter) throws IOException {
        return writePut(transactionTable, buildCreatePut(startTransactionTimestamp, params, status, beginTimestamp, counter));
    }

    public boolean recordTransactionCommit(final long startTransactionTimestamp, final long commitTimestamp,
                                           final Long globalCommitTimestamp, final TransactionStatus expectedStatus,
                                           final TransactionStatus newStatus) throws IOException {
        Tracer.traceStatus(startTransactionTimestamp, newStatus, true);
        try {
            return withTransactionTable(new TransactionStoreCallback<Boolean, Table>() {
                @Override
                public Boolean withTable(Table transactionTable) throws IOException {
                    return writePut(transactionTable, buildCommitPut(startTransactionTimestamp, commitTimestamp, globalCommitTimestamp, newStatus),
                            (expectedStatus == null) ? null : encodeStatus(expectedStatus));
                }
            });
        } finally {
            Tracer.traceStatus(startTransactionTimestamp, newStatus, false);
        }
    }

    public boolean recordTransactionStatusChange(final long startTransactionTimestamp, final TransactionStatus expectedStatus,
                                                 final TransactionStatus newStatus)
            throws IOException {
        Tracer.traceStatus(startTransactionTimestamp, newStatus, true);
        try {
            return withTransactionTable(new TransactionStoreCallback<Boolean, Table>() {
                @Override
                public Boolean withTable(Table transactionTable) throws IOException {
                    return writePut(transactionTable, buildStatusUpdatePut(startTransactionTimestamp, newStatus), encodeStatus(expectedStatus));
                }
            });
        } finally {
            Tracer.traceStatus(startTransactionTimestamp, newStatus, false);
        }
    }

    public void recordTransactionKeepAlive(final long startTransactionTimestamp)
            throws IOException {
        withTransactionTable(new TransactionStoreCallback<Void, Table>() {
            @Override
            public Void withTable(Table transactionTable) throws IOException {
                writePut(transactionTable, buildKeepAlivePut(startTransactionTimestamp), encodeStatus(TransactionStatus.ACTIVE));
                return null;
            }
        });
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
        return dataLib.newPut(transactionIdToRowKeyObject(transactionId));
    }

    private Data transactionIdToRowKeyObject(long transactionId) {
        return dataLib.newRowKey(transactionIdToRowKey(transactionId));
    }

    private void addFieldToPut(Put put, Data qualifier, Object value) {
        dataLib.addKeyValueToPut(put, encodedSchema.siFamily, qualifier, null, dataLib.encode(value));
    }

    // Apply operations to the transaction table

    private boolean writePut(Table transactionTable, Put put) throws IOException {
        return writePut(transactionTable, put, null);
    }

    private <T> T withTransactionTable(TransactionStoreCallback<T, Table> transactionStoreCallback) throws IOException {
        final Table transactionTable = reader.open(transactionSchema.tableName);
        try {
            return transactionStoreCallback.withTable(transactionTable);
        } finally {
            reader.close(transactionTable);
        }
    }

    private boolean writePut(Table transactionTable, Put put, Data expectedStatus) throws IOException {
        final boolean result = writer.checkAndPut(transactionTable, encodedSchema.siFamily, encodedSchema.statusQualifier,
                expectedStatus, put);
        if (expectedStatus == null && result) {
            listener.writeTransaction();
        }
        return result;
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
        Table transactionTable = reader.open(transactionSchema.tableName);
        try {
            final Result rawResult = readTransaction(transactionTable, transactionId);
            if (rawResult != null) {
                Transaction result = decodeResults(transactionId, rawResult);
                if (result != null) {
                    return result;
                }
            }
        } finally {
            reader.close(transactionTable);
        }
        throw new RuntimeException("transaction ID not found: " + transactionId);
    }

    private Result readTransaction(Table transactionTable, long transactionId) throws IOException {
        Data tupleKey = transactionIdToRowKeyObject(transactionId);
        Get get = dataLib.newGet(tupleKey, siFamilyList, null, null);
        return reader.get(transactionTable, get);
    }

    private Transaction decodeResults(long transactionId, Result resultTuple) throws IOException {
        Transaction result = null;
        if (resultTuple != null) {
            listener.loadTransaction();
            result = decodeTransactionResults(transactionId, resultTuple);
            cacheCompletedTransactions(transactionId, result);
        }
        return result;
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

    private Byte decodeByte(Result resultTuple, Data family, Data columnQualifier) {
        final Data columnValue = dataLib.getResultValue(resultTuple, family, columnQualifier);
        Byte result = null;
        if (columnValue != null) {
            result = (Byte) dataLib.decode(columnValue, Byte.class);
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
        final Table transactionTable = reader.open(transactionSchema.tableName);
        try {
            final Transaction transaction = loadTransactionDirect(transactionId);
            long current = transaction.counter;
            // TODO: more efficient mechanism for obtaining timestamp
            while (current - transaction.counter < 10000) {
                final long next = current + 1;
                final Put put = buildBasePut(transactionId);
                addFieldToPut(put, encodedSchema.counterQualifier, next);
                if (writer.checkAndPut(transactionTable, encodedSchema.siFamily, encodedSchema.counterQualifier,
                        dataLib.encode(current), put)) {
                    return next;
                } else {
                    current = next;
                }
            }
        } finally {
            reader.close(transactionTable);
        }
        throw new IOException("Unable to obtain timestamp");
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
    private Object[] transactionIdToRowKey(long id) {
        return new Object[]{(byte) (id % 16), id};
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

    final DataIDDecoder<Long, Result> decoder = new DataIDDecoder<Long, Result>() {
        @Override
        public Long getID(Result result) {
            return decodeLong(result, encodedSchema.idQualifier);
        }
    };

    public List<Transaction> getOldestActiveTransactions(final long startTransactionId, final long maxTransactionId,
                                                         final int maxCount, final TransactionParams missingParams,
                                                         final TransactionStatus missingStatus)
            throws IOException {
        return withTransactionTable(new TransactionStoreCallback<List<Transaction>, Table>() {
            @Override
            public List<Transaction> withTable(final Table transactionTable) throws IOException {
                final ContiguousIterator<Long, Result> contiguousIterator = makeContiguousIterator(transactionTable, startTransactionId, missingParams, missingStatus);
                final List<Transaction> results = new ArrayList<Transaction>();
                long transactionId = startTransactionId;
                while (results.size() < maxCount && contiguousIterator.hasNext() && transactionId < maxTransactionId) {
                    final Result next = contiguousIterator.next();
                    transactionId = decoder.getID(next);
                    final Transaction transaction = decodeResults(transactionId, next);
                    if (transaction.getEffectiveStatus().isActive()) {
                        results.add(transaction);
                    }
                }
                return results;
            }
        });
    }

    private ContiguousIterator<Long, Result> makeContiguousIterator(Table transactionTable, long startTransactionId, TransactionParams missingParams, TransactionStatus missingStatus) throws IOException {
        List<Iterator<Result>> scanners = makeScanners(transactionTable, startTransactionId);
        return new ContiguousIterator<Long, Result>(startTransactionId,
                new OrderedMuxer<Long, Result>(scanners, decoder), decoder, makeCallbacks(transactionTable, missingParams, missingStatus));
    }

    private ContiguousIteratorFunctions<Long, Result> makeCallbacks(final Table transactionTable, final TransactionParams missingParams, final TransactionStatus missingStatus) {
        return new ContiguousIteratorFunctions<Long, Result>() {
            @Override
            public Long increment(Long transactionId) {
                return transactionId + 1;
            }

            @Override
            public Result missing(Long transactionId) throws IOException {
                recordNewTransactionDirect(transactionTable, transactionId, missingParams, missingStatus, transactionId, 0);
                return readTransaction(transactionTable, transactionId);
            }
        };
    }

    private List<Iterator<Result>> makeScanners(Table transactionTable, long startTransactionId) throws IOException {
        List<Iterator<Result>> scanners = new ArrayList<Iterator<Result>>();
        for (byte i = 0; i < 16; i++) {
            final Data rowKey = dataLib.newRowKey(new Object[]{i, startTransactionId});
            final Data endKey = i == 15 ? null : dataLib.newRowKey(new Object[]{(byte) (i + 1)});
            final Scan scan = dataLib.newScan(rowKey, endKey, null, null, null);
            scanners.add(reader.scan(transactionTable, scan));
        }
        return scanners;
    }

    public void confirmPermission(TransactionId transactionId, String tableName) throws IOException {
        Byte p = readPermission(transactionId, tableName);
        if (p == null) {
            if (writePermission(transactionId, tableName, (byte) 1)) {
            } else {
                throw new PermissionFailure("permission fail " + transactionId + " " + tableName);
            }
        } else if (p == 1) {
        } else if (p == 0) {
            throw new PermissionFailure("permission fail " + transactionId + " " + tableName);
        }
    }

    private Byte readPermission(final TransactionId transactionId, final String tableName) throws IOException {
        final PermissionArgs key = new PermissionArgs(transactionId, tableName);
        Byte result = permissionCache.get(key);
        if (result == null) {
            result = readPermissionDirect(transactionId, tableName);
            if (result != null) {
                permissionCache.put(key, result);
            }
        }
        return result;
    }

    private Byte readPermissionDirect(final TransactionId transactionId, final String tableName) throws IOException {
        return withTransactionTable(new TransactionStoreCallback<Byte, Table>() {
            @Override
            public Byte withTable(Table transactionTable) throws IOException {
                return readPermissionBody(transactionTable, transactionId, tableName);
            }
        });
    }

    private Byte readPermissionBody(Table transactionTable, TransactionId transactionId, String tableName) throws IOException {
        Data tupleKey = transactionIdToRowKeyObject(transactionId.getId());
        final Data qualifier = dataLib.encode(tableName);
        final List<List<Data>> columnList = Arrays.asList(
                Arrays.asList(encodedSchema.permissionFamily, qualifier));
        Get get = dataLib.newGet(tupleKey, permissionFamilyList, columnList, null);
        final Result result = reader.get(transactionTable, get);
        if (result == null) {
            return null;
        } else {
            return decodeByte(result, encodedSchema.permissionFamily, qualifier);
        }
    }

    private boolean writePermission(final TransactionId transactionId, final String tableName, final byte permissionValue) throws IOException {
        return withTransactionTable(new TransactionStoreCallback<Boolean, Table>() {
            @Override
            public Boolean withTable(Table transactionTable) throws IOException {
                Data tupleKey = transactionIdToRowKeyObject(transactionId.getId());
                final Put put = dataLib.newPut(tupleKey);
                final Data qualifier = dataLib.encode(tableName);
                dataLib.addKeyValueToPut(put, encodedSchema.permissionFamily, qualifier, null, dataLib.encode(permissionValue));
                if (writer.checkAndPut(transactionTable, encodedSchema.permissionFamily, qualifier, null, put)) {
                    return true;
                } else {
                    return (readPermissionBody(transactionTable, transactionId, tableName) == permissionValue);
                }
            }
        });
    }

    public boolean forbidPermission(final String tableName, final TransactionId transactionId) throws IOException {
        return writePermission(transactionId, tableName, (byte) 0);
    }
}
