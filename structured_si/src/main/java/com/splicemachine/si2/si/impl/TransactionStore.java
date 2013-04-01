package com.splicemachine.si2.si.impl;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.data.api.STableWriter;
import com.splicemachine.si2.si.api.TransactionId;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

public class TransactionStore {
    static final Logger LOG = Logger.getLogger(TransactionStore.class);

    private final SDataLib dataLib;
    private final STableReader reader;
    private final STableWriter writer;

    private final TransactionSchema transactionSchema;
    private final TransactionSchema encodedSchema;

    public TransactionStore(TransactionSchema transactionSchema, SDataLib dataLib,
                            STableReader reader, STableWriter writer) {
        this.transactionSchema = transactionSchema;
        this.encodedSchema = transactionSchema.encodedSchema(dataLib);
        this.dataLib = dataLib;
        this.reader = reader;
        this.writer = writer;
    }

    public void recordNewTransaction(TransactionId startTransactionTimestamp, TransactionId parent, Boolean dependent,
                                     boolean allowWrites, Boolean readUncommitted, Boolean readCommitted,
                                     TransactionStatus status)
            throws IOException {
        writePut(makeCreateTuple(startTransactionTimestamp, parent, dependent, allowWrites, readUncommitted,
                readCommitted, status));
    }

    public void recordTransactionCommit(TransactionId startTransactionTimestamp, long commitTransactionTimestamp,
                                        TransactionStatus newStatus) throws IOException {
        writePut(makeCommitPut(startTransactionTimestamp, commitTransactionTimestamp, newStatus));
    }

    public void recordTransactionStatusChange(TransactionId startTransactionTimestamp, TransactionStatus newStatus)
            throws IOException {
        writePut(makeStatusUpdateTuple(startTransactionTimestamp, newStatus));
    }

    public TransactionStruct getTransactionStatus(long beginTimestamp) throws IOException {
        return getTransactionStatus(new SiTransactionId(beginTimestamp));
    }

    public TransactionStruct getTransactionStatus(TransactionId transactionId) throws IOException {
        Object tupleKey = dataLib.newRowKey(new Object[]{transactionIdToRowKey(transactionId)});

        STable transactionSTable = reader.open(transactionSchema.tableName);
        try {
            SGet get = dataLib.newGet(tupleKey, null, null, null);
            Object resultTuple = reader.get(transactionSTable, get);
            if (resultTuple != null) {
                final Object value = dataLib.getResultValue(resultTuple, encodedSchema.siFamily, encodedSchema.statusQualifier);
                TransactionStatus status = (value == null) ? null : TransactionStatus.values()[((Integer) dataLib.decode(value, Integer.class))];
                Long parentId = getLongFieldFromResult(resultTuple, encodedSchema.parentQualifier);
                TransactionStruct parent = null;
                if (parentId != null) {
                    parent = getTransactionStatus(parentId);
                }
                final Object commitValue = dataLib.getResultValue(resultTuple, encodedSchema.siFamily, encodedSchema.commitQualifier);
                Long commitTimestamp = null;
                if (commitValue != null) {
                    commitTimestamp = (Long) dataLib.decode(commitValue, Long.class);
                }
                return new TransactionStruct(transactionId.getId(),
                        parent,
                        getBooleanFieldFromResult(resultTuple, encodedSchema.dependentQualifier),
                        getBooleanFieldFromResult(resultTuple, encodedSchema.allowWritesQualifier),
                        getBooleanFieldFromResult(resultTuple, encodedSchema.readUncommittedQualifier),
                        getBooleanFieldFromResult(resultTuple, encodedSchema.readCommittedQualifier),
                        status, commitTimestamp);
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

    private Object makeCreateTuple(TransactionId transactionId, TransactionId parent, Boolean dependent,
                                   boolean allowWrites, Boolean readUncommitted,
                                   Boolean readCommitted, TransactionStatus status) {
        Object put = makeBasePut(transactionId);
        addFieldToPut(put, encodedSchema.startQualifier, transactionId.getId());
        if (parent != null) {
            addFieldToPut(put, encodedSchema.parentQualifier, parent.getId());
        }
        if (dependent != null) {
            addFieldToPut(put, encodedSchema.dependentQualifier, dependent);
        }
        addFieldToPut(put, encodedSchema.allowWritesQualifier, allowWrites);
        if (readUncommitted != null) {
            addFieldToPut(put, encodedSchema.readUncommittedQualifier, readUncommitted);
        }
        if (readCommitted != null) {
            addFieldToPut(put, encodedSchema.readCommittedQualifier, readCommitted);
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
        final STable transactionSTable = reader.open(transactionSchema.tableName);
        try {
            writer.write(transactionSTable, put);
        } finally {
            reader.close(transactionSTable);
        }
    }
}
