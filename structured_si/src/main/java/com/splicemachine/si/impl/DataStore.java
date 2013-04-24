package com.splicemachine.si.impl;

import com.splicemachine.si.api.PutLog;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.SGet;
import com.splicemachine.si.data.api.SRead;
import com.splicemachine.si.data.api.SRowLock;
import com.splicemachine.si.data.api.SScan;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.api.TransactionId;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.splicemachine.constants.TransactionConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME;
import static com.splicemachine.constants.TransactionConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE;

public class DataStore {
    private final SDataLib dataLib;
    private final STableReader reader;
    private final STableWriter writer;

    private final String siNeededAttribute;
    private final String transactionIdAttribute;
    private final String deletePutAttribute;

    private final Object siFamily;
    private final Object commitTimestampQualifier;
    private final Object tombstoneQualifier;
    private final Object siNull;

    private final Object userColumnFamily;

    private final int maxLogRows;

    public DataStore(SDataLib dataLib, STableReader reader, STableWriter writer, String siNeededAttribute,
                     String transactionIdAttribute, String deletePutAttribute,
                     String siMetaFamily, Object siCommitQualifier, Object siTombstoneQualifier, Object siMetaNull,
                     Object userColumnFamily, int maxLogRows) {
        this.dataLib = dataLib;
        this.reader = reader;
        this.writer = writer;
        this.siNeededAttribute = siNeededAttribute;
        this.transactionIdAttribute = transactionIdAttribute;
        this.deletePutAttribute = deletePutAttribute;
        this.siFamily = dataLib.encode(siMetaFamily);

        this.commitTimestampQualifier = dataLib.encode(siCommitQualifier);
        this.tombstoneQualifier = dataLib.encode(siTombstoneQualifier);
        this.siNull = dataLib.encode(siMetaNull);
        this.userColumnFamily = dataLib.encode(userColumnFamily);

        this.maxLogRows = maxLogRows;
    }

    void setSiNeededAttribute(Object put) {
        dataLib.addAttribute(put, siNeededAttribute, dataLib.encode(true));
    }

    Boolean getSiNeededAttribute(Object put) {
        Object neededValue = dataLib.getAttribute(put, siNeededAttribute);
        return (Boolean) dataLib.decode(neededValue, Boolean.class);
    }

    void setDeletePutAttribute(Object put) {
        dataLib.addAttribute(put, deletePutAttribute, dataLib.encode(true));
    }

    Boolean getDeletePutAttribute(Object put) {
        Object neededValue = dataLib.getAttribute(put, deletePutAttribute);
        return (Boolean) dataLib.decode(neededValue, Boolean.class);
    }

    void addTransactionIdToPut(Object put, TransactionId transactionId) {
        dataLib.addKeyValueToPut(put, siFamily, commitTimestampQualifier, transactionId.getId(), siNull);
    }

    void setTransactionId(SiTransactionId transactionId, Object operation) {
        dataLib.addAttribute(operation, transactionIdAttribute, dataLib.encode(transactionId.getTransactionIdString()));
    }

    SiTransactionId getTransactionIdFromOperation(Object put) {
        Object value = dataLib.getAttribute(put, transactionIdAttribute);
        String transactionId = (String) dataLib.decode(value, String.class);
        if (transactionId != null) {
            return new SiTransactionId(transactionId);
        }
        return null;
    }

    void copyPutKeyValues(Object put, Object newPut, long timestamp) {
        for (Object keyValue : dataLib.listPut(put)) {
            dataLib.addKeyValueToPut(newPut, dataLib.getKeyValueFamily(keyValue),
                    dataLib.getKeyValueQualifier(keyValue),
                    timestamp,
                    dataLib.getKeyValueValue(keyValue));
        }
    }

    List getCommitTimestamp(STable table, Object rowKey) throws IOException {
        final List<List<Object>> columns = Arrays.asList(Arrays.asList(siFamily, commitTimestampQualifier));
        SGet get = dataLib.newGet(rowKey, null, columns, null);
        Object result = reader.get(table, get);
        if (result != null) {
            return dataLib.getResultColumn(result, siFamily, commitTimestampQualifier);
        }
        return null;
    }

    public KeyValueType getKeyValueType(Object family, Object qualifier) {
        if (dataLib.valuesEqual(family, siFamily) && dataLib.valuesEqual(qualifier, commitTimestampQualifier)) {
            return KeyValueType.COMMIT_TIMESTAMP;
        } else if (dataLib.valuesEqual(family, siFamily) && dataLib.valuesEqual(qualifier, tombstoneQualifier)) {
            return KeyValueType.TOMBSTONE;
        } else if (dataLib.valuesEqual(family, userColumnFamily)) {
            return KeyValueType.USER_DATA;
        } else {
            return KeyValueType.OTHER;
        }
    }

    public boolean isSiNull(Object value) {
        return dataLib.valuesEqual(value, siNull);
    }

    public void addToPutLog(ImmutableTransaction transaction, Object rowKey, PutLog putLog) {
        final long transactionId = transaction.getTransactionId().getId();
        Set rows = putLog.getRows(transactionId);
        if (rows == null) {
            rows = new HashSet();
            putLog.setRows(transactionId, rows);
        }
        if (rows.size() < maxLogRows) {
            rows.add(rowKey);
        }
    }

    public void rollForward(STable table, Object rowKey, Transaction transaction) throws IOException {
        //setCommitTimestamp(table, rowKey, transaction.beginTimestamp, transaction.commitTimestamp);
    }

    public void setCommitTimestamp(STable table, Object rowKey, long beginTimestamp, long commitTimestamp) throws IOException {
        Object put = dataLib.newPut(rowKey);
        suppressIndexing(put);
        dataLib.addKeyValueToPut(put, siFamily, commitTimestampQualifier, beginTimestamp, dataLib.encode(commitTimestamp));
        writer.write(table, put, false);
    }

    /**
     * When this new operation goes through the co-processor stack it should not be indexed (because it already has been
     * when the original operation went through or because it is strictly modifying our metadata).
     */
    public void suppressIndexing(Object newPut) {
        dataLib.addAttribute(newPut, SUPPRESS_INDEXING_ATTRIBUTE_NAME, SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
    }


    public void setTombstoneOnPut(Object put, SiTransactionId transactionId) {
        dataLib.addKeyValueToPut(put, siFamily, tombstoneQualifier, transactionId.getId(), siNull);
    }

    public void addSiFamilyToReadIfNeeded(SRead get) {
        dataLib.addFamilyToReadIfNeeded(get, siFamily);
    }
}
