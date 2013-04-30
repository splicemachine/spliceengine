package com.splicemachine.si.impl;

import com.google.common.cache.Cache;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.SGet;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.api.TransactionId;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransactionStore {
    static final Logger LOG = Logger.getLogger(TransactionStore.class);

    private final SDataLib dataLib;
    private final STableReader reader;
    private final Cache<Long, ImmutableTransaction> immutableTransactionCache;
    private final Cache<Long, Transaction> transactionCache;
    private final STableWriter writer;

    private final TransactionSchema transactionSchema;
    private final TransactionSchema encodedSchema;

    public TransactionStore(TransactionSchema transactionSchema, SDataLib dataLib,
                            STableReader reader, STableWriter writer, Cache<Long, Transaction> transactionCache,
                            Cache<Long, ImmutableTransaction> immutableTransactionCache) {
        this.transactionSchema = transactionSchema;
        this.encodedSchema = transactionSchema.encodedSchema(dataLib);
        this.dataLib = dataLib;
        this.reader = reader;
        this.transactionCache = transactionCache;
        this.immutableTransactionCache = immutableTransactionCache;
        this.writer = writer;
    }

    public void recordNewTransaction(TransactionId startTransactionTimestamp, TransactionParams params,
                                     TransactionStatus status) throws IOException {
        writePut(makeCreateTuple(startTransactionTimestamp, params, status));
    }

    public void addChildToTransaction(TransactionId transactionId, TransactionId childTransactionId) throws IOException {
        Object put = makeBasePut(transactionId);
        dataLib.addKeyValueToPut(put, encodedSchema.siChildrenFamily, dataLib.encode(childTransactionId.getTransactionIdString()), null,
                dataLib.encode(true));
        writePut(put);
    }

    public boolean recordTransactionEnd(TransactionId startTransactionTimestamp, long commitTransactionTimestamp,
                                        TransactionStatus expectedStatus, TransactionStatus newStatus) throws IOException {
        Tracer.traceStatus(startTransactionTimestamp.getId(), newStatus, true);
        try {
            return writePut(makeCommitPut(startTransactionTimestamp, commitTransactionTimestamp, newStatus), encodedStatus(expectedStatus));
        } finally {
            Tracer.traceStatus(startTransactionTimestamp.getId(), newStatus, false);
        }
    }

    private Object encodedStatus(TransactionStatus status) {
        if (status == null) {
            return encodedSchema.siNull;
        } else {
            return dataLib.encode(status.ordinal());
        }
    }

    public boolean recordTransactionStatusChange(TransactionId startTransactionTimestamp, TransactionStatus expectedStatus,
                                                 TransactionStatus newStatus)
            throws IOException {
        Tracer.traceStatus(startTransactionTimestamp.getId(), newStatus, true);
        try {
            return writePut(makeStatusUpdateTuple(startTransactionTimestamp, newStatus), encodedStatus(expectedStatus));
        } finally {
            Tracer.traceStatus(startTransactionTimestamp.getId(), newStatus, false);
        }
    }

    public void recordKeepAlive(TransactionId startTransactionTimestamp)
            throws IOException {
        writePut(makeKeepAliveTuple(startTransactionTimestamp));
    }

    public ImmutableTransaction getImmutableTransaction(TransactionId transactionId) throws IOException {
        final Transaction cachedTransaction = transactionCache.getIfPresent(transactionId.getId());
        if (cachedTransaction != null) {
            return cachedTransaction;
        }
        ImmutableTransaction immutableCachedTransaction = immutableTransactionCache.getIfPresent(transactionId.getId());
        if (immutableCachedTransaction != null) {
            return immutableCachedTransaction;
        }
        immutableCachedTransaction = getTransaction(transactionId.getId());
        immutableTransactionCache.put(transactionId.getId(), immutableCachedTransaction);
        return immutableCachedTransaction;
    }

    public Transaction getTransaction(long beginTimestamp) throws IOException {
        return getTransaction(new SiTransactionId(beginTimestamp));
    }

    public Transaction getTransaction(TransactionId transactionId) throws IOException {
        final Transaction cachedTransaction = transactionCache.getIfPresent(transactionId.getId());
        if (cachedTransaction != null) {
            //LOG.warn("cache HIT " + transactionId.getTransactionIdString());
            return cachedTransaction;
        }
        Object tupleKey = dataLib.newRowKey(new Object[]{transactionIdToRowKey(transactionId)});

        STable transactionSTable = reader.open(transactionSchema.tableName);
        try {
            SGet get = dataLib.newGet(tupleKey, null, null, null);
            Object resultTuple = reader.get(transactionSTable, get);
            if (resultTuple != null) {
                final Object value = dataLib.getResultValue(resultTuple, encodedSchema.siFamily, encodedSchema.statusQualifier);

                final List keepAliveValues = dataLib.getResultColumn(resultTuple, encodedSchema.siFamily, encodedSchema.keepAliveQualifier);
                final Object keepAliveValue = keepAliveValues.get(0);
                final long keepAlive = dataLib.getKeyValueTimestamp(keepAliveValue);

                TransactionStatus status = (value == null) ? null : TransactionStatus.values()[((Integer) dataLib.decode(value, Integer.class))];
                Long parentId = getLongFieldFromResult(resultTuple, encodedSchema.parentQualifier);
                Transaction parent = null;
                if (parentId != null) {
                    parent = getTransaction(parentId);
                }
                final Object commitValue = dataLib.getResultValue(resultTuple, encodedSchema.siFamily, encodedSchema.commitQualifier);
                Long commitTimestamp = null;
                if (commitValue != null) {
                    commitTimestamp = (Long) dataLib.decode(commitValue, Long.class);
                }
                Map childrenMap = dataLib.getResultFamilyMap(resultTuple, encodedSchema.siChildrenFamily);
                Set<Long> children = new HashSet<Long>();
                for (Object child : childrenMap.keySet()) {
                    children.add(Long.valueOf((String) dataLib.decode(child, String.class)));
                }

                final Transaction result = new Transaction(transactionId.getId(), keepAlive, parent, children,
                        getBooleanFieldFromResult(resultTuple, encodedSchema.dependentQualifier),
                        getBooleanFieldFromResult(resultTuple, encodedSchema.allowWritesQualifier),
                        getBooleanFieldFromResult(resultTuple, encodedSchema.readUncommittedQualifier),
                        getBooleanFieldFromResult(resultTuple, encodedSchema.readCommittedQualifier),
                        status, commitTimestamp);
                if (!result.isActive()) {
                    transactionCache.put(transactionId.getId(), result);
                    //LOG.warn("cache PUT " + transactionId.getTransactionIdString());
                } else {
                    //LOG.warn("cache NOT " + transactionId.getTransactionIdString());
                }
                return result;
            }
        } finally {
            reader.close(transactionSTable);
        }
        throw new RuntimeException("transaction ID not found");
    }

    private Long getLongFieldFromResult(Object resultTuple, Object qualifier) {
        final Object value = dataLib.getResultValue(resultTuple, encodedSchema.siFamily, qualifier);
        Long result = null;
        if (value != null) {
            result = (Long) dataLib.decode(value, Long.class);
        }
        return result;
    }

    private Boolean getBooleanFieldFromResult(Object resultTuple, Object qualifier) {
        final Object value = dataLib.getResultValue(resultTuple, encodedSchema.siFamily, qualifier);
        Boolean result = null;
        if (value != null) {
            result = (Boolean) dataLib.decode(value, Boolean.class);
        }
        return result;
    }

    private Object makeStatusUpdateTuple(TransactionId transactionId, TransactionStatus newStatus) {
        Object put = makeBasePut(transactionId);
        addFieldToPut(put, encodedSchema.statusQualifier, newStatus.ordinal());
        return put;
    }

    private Object makeKeepAliveTuple(TransactionId transactionId) {
        Object put = makeBasePut(transactionId);
        addFieldToPut(put, encodedSchema.keepAliveQualifier, encodedSchema.siNull);
        return put;
    }

    private Object makeCreateTuple(TransactionId transactionId, TransactionParams params, TransactionStatus status) {
        Object put = makeBasePut(transactionId);
        addFieldToPut(put, encodedSchema.startQualifier, transactionId.getId());
        addFieldToPut(put, encodedSchema.keepAliveQualifier, encodedSchema.siNull);
        if (params.parent != null) {
            addFieldToPut(put, encodedSchema.parentQualifier, params.parent.getId());
        }
        if (params.dependent != null) {
            addFieldToPut(put, encodedSchema.dependentQualifier, params.dependent);
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
        return put;
    }

    private Object makeCommitPut(TransactionId transactionId, long commitTransactionTimestamp, TransactionStatus newStatus) {
        Object put = makeBasePut(transactionId);
        addFieldToPut(put, encodedSchema.commitQualifier, commitTransactionTimestamp);
        addFieldToPut(put, encodedSchema.statusQualifier, newStatus.ordinal());
        return put;
    }

    private Object makeBasePut(TransactionId transactionId) {
        Object rowKey = dataLib.newRowKey(new Object[]{transactionIdToRowKey(transactionId)});
        return dataLib.newPut(rowKey);
    }

    private long transactionIdToRowKey(TransactionId transactionId) {
        byte[] result = Bytes.toBytes(transactionId.getId());
        ArrayUtils.reverse(result);
        return Bytes.toLong(result);
    }

    private void addFieldToPut(Object put, Object qualifier, Object value) {
        dataLib.addKeyValueToPut(put, encodedSchema.siFamily, qualifier, null, dataLib.encode(value));
    }

    private void writePut(Object put) throws IOException {
        writePut(put, null);
    }

    private boolean writePut(Object put, Object expectedStatus) throws IOException {
        final STable transactionSTable = reader.open(transactionSchema.tableName);
        try {
            if (expectedStatus == null) {
                writer.write(transactionSTable, put);
                return true;
            } else {
                return writer.checkAndPut(transactionSTable, encodedSchema.siFamily, encodedSchema.statusQualifier,
                        expectedStatus, put);
            }
        } finally {
            reader.close(transactionSTable);
        }
    }
}
