package com.splicemachine.si2.si.impl;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.data.api.STableWriter;
import com.splicemachine.si2.si.api.TransactionId;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.Iterator;

public class TransactionStore {
    private final SDataLib handler;
    private final STableReader reader;
    private final STableWriter writer;

    private final TransactionSchema transactionSchema;
    private final TransactionSchema encodedSchema;

    public TransactionStore(TransactionSchema transactionSchema, SDataLib handler,
                            STableReader reader, STableWriter writer) {
        this.transactionSchema = transactionSchema;
        this.encodedSchema = transactionSchema.encodedSchema(handler);
        this.handler = handler;
        this.reader = reader;
        this.writer = writer;
    }

    public void recordNewTransaction(TransactionId startTransactionTimestamp, TransactionStatus status) {
        writeTuple(makeCreateTuple(startTransactionTimestamp, status));
    }

    public void recordTransactionCommit(TransactionId startTransactionTimestamp, long commitTransactionTimestamp, TransactionStatus newStatus) {
        writeTuple(makeCommitTuple(startTransactionTimestamp, commitTransactionTimestamp, newStatus));
    }

    public void recordTransactionStatusChange(TransactionId startTransactionTimestamp, TransactionStatus newStatus) {
        writeTuple(makeStatusUpdateTuple(startTransactionTimestamp, newStatus));
    }

    public TransactionStruct getTransactionStatus(TransactionId transactionId) {
        Object tupleKey = handler.newRowKey(new Object[]{transactionIdToRowKey(transactionId)});

        STable transactionSTable = reader.open(transactionSchema.relationIdentifier);
        try {
            SGet get = handler.newGet(tupleKey, null, null, null);
            Object resultTuple = reader.get(transactionSTable, get);
            if (resultTuple != null) {
                final Object value = handler.getResultValue(resultTuple, encodedSchema.siFamily, encodedSchema.statusQualifier);
                TransactionStatus status = TransactionStatus.values()[((Integer) handler.decode(value, Integer.class))];
                final Object commitValue = handler.getResultValue(resultTuple, encodedSchema.siFamily, encodedSchema.commitQualifier);
                Long commitTimestamp = null;
                if (commitValue != null) {
                    commitTimestamp = (Long) handler.decode(commitValue, Long.class);
                }
                return new TransactionStruct(transactionId.getId(), status, commitTimestamp);
            }
        } finally {
            reader.close(transactionSTable);
        }
        throw new RuntimeException("transaction ID not found");
    }

    private Object makeStatusUpdateTuple(TransactionId transactionId, TransactionStatus newStatus) {
        Object tuple = makeBaseTuple(transactionId);
        addFieldToTuple(tuple, encodedSchema.statusQualifier, newStatus.ordinal());
        return tuple;
    }

    private Object makeCreateTuple(TransactionId transactionId, TransactionStatus status) {
        Object tuple = makeBaseTuple(transactionId);
        addFieldToTuple(tuple, encodedSchema.startQualifier, transactionId.getId());
        addFieldToTuple(tuple, encodedSchema.statusQualifier, status.ordinal());
        return tuple;
    }

    private Object makeCommitTuple(TransactionId transactionId, long commitTransactionTimestamp, TransactionStatus newStatus) {
        Object tuple = makeBaseTuple(transactionId);
        addFieldToTuple(tuple, encodedSchema.commitQualifier, commitTransactionTimestamp);
        addFieldToTuple(tuple, encodedSchema.statusQualifier, newStatus.ordinal());
        return tuple;
    }

    private Object makeBaseTuple(TransactionId transactionId) {
        Object tupleKey = handler.newRowKey(new Object[]{transactionIdToRowKey(transactionId)});
        return handler.newPut(tupleKey);
    }

    private long transactionIdToRowKey(TransactionId transactionId) {
        byte[] result = Bytes.toBytes(transactionId.getId());
        ArrayUtils.reverse(result);
        return Bytes.toLong(result);
    }

    private void addFieldToTuple(Object tuple, Object qualifier, Object value) {
        handler.addKeyValueToPut(tuple, encodedSchema.siFamily, qualifier, null, handler.encode(value));
    }

    private void writeTuple(Object tuple) {
        final STable transactionSTable = reader.open(transactionSchema.relationIdentifier);
        try {
            writer.write(transactionSTable, Arrays.asList(tuple));
        } finally {
            reader.close(transactionSTable);
        }
    }

}
