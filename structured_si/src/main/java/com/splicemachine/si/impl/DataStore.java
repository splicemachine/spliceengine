package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.splicemachine.constants.SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME;
import static com.splicemachine.constants.SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE;

/**
 * Library of functions used by the SI module when accessing rows from data tables (data tables as opposed to the
 * transaction table).
 */
public class DataStore {
    final SDataLib dataLib;
    private final STableReader reader;
    private final STableWriter writer;

    private final String siNeededAttribute;
    private final Object siNeededValue;
    private final Object includeSIColumnValue;
    private final String includeUncommittedAsOfStartAttribute;
    private final Object includeUncommittedAsOfStartValue;
    private final String transactionIdAttribute;
    private final String deletePutAttribute;

    private final Object siFamily;
    private final Object commitTimestampQualifier;
    private final Object tombstoneQualifier;
    private final Object placeHolderQualifier;
    private final Object siNull;
    final Object siFail;

    private final Object userColumnFamily;

    public DataStore(SDataLib dataLib, STableReader reader, STableWriter writer, String siNeededAttribute,
                     Object siNeededValue, Object includeSIColumnValue, String includeUncommittedAsOfStartAttribute,
                     Object includeUncommittedAsOfStartValue, String transactionIdAttribute, String deletePutAttribute,
                     String siMetaFamily, Object siCommitQualifier, Object siTombstoneQualifier,
                     Object placeHolderQualifier, Object siMetaNull, Object siFail, Object userColumnFamily) {
        this.dataLib = dataLib;
        this.reader = reader;
        this.writer = writer;
        this.siNeededAttribute = siNeededAttribute;
        this.siNeededValue = dataLib.encode(siNeededValue);
        this.includeSIColumnValue = dataLib.encode(includeSIColumnValue);
        this.includeUncommittedAsOfStartAttribute = includeUncommittedAsOfStartAttribute;
        this.includeUncommittedAsOfStartValue = dataLib.encode(includeUncommittedAsOfStartValue);
        this.transactionIdAttribute = transactionIdAttribute;
        this.deletePutAttribute = deletePutAttribute;
        this.siFamily = dataLib.encode(siMetaFamily);
        this.commitTimestampQualifier = dataLib.encode(siCommitQualifier);
        this.tombstoneQualifier = dataLib.encode(siTombstoneQualifier);
        this.placeHolderQualifier = dataLib.encode(placeHolderQualifier);
        this.siNull = dataLib.encode(siMetaNull);
        this.siFail = dataLib.encode(siFail);
        this.userColumnFamily = dataLib.encode(userColumnFamily);
    }

    void setSINeededAttribute(Object operation, boolean includeSIColumn) {
        dataLib.addAttribute(operation, siNeededAttribute, dataLib.encode(includeSIColumn ? includeSIColumnValue : siNeededValue));
    }

    Object getSINeededAttribute(Object operation) {
        return dataLib.getAttribute(operation, siNeededAttribute);
    }

    boolean isIncludeSIColumn(Object operation) {
        return dataLib.valuesEqual(dataLib.getAttribute(operation, siNeededAttribute), includeSIColumnValue);
    }

    void setIncludeUncommittedAsOfStart(Object operation) {
        dataLib.addAttribute(operation, includeUncommittedAsOfStartAttribute, includeUncommittedAsOfStartValue);
    }

    boolean isScanIncludeUncommittedAsOfStart(Object operation) {
        return dataLib.valuesEqual(dataLib.getAttribute(operation, includeUncommittedAsOfStartAttribute), includeUncommittedAsOfStartValue);
    }

    void setDeletePutAttribute(Object operation) {
        dataLib.addAttribute(operation, deletePutAttribute, dataLib.encode(true));
    }

    Boolean getDeletePutAttribute(Object operation) {
        Object neededValue = dataLib.getAttribute(operation, deletePutAttribute);
        return (Boolean) dataLib.decode(neededValue, Boolean.class);
    }

    void addTransactionIdToPut(Object put, long transactionId) {
        dataLib.addKeyValueToPut(put, siFamily, commitTimestampQualifier, transactionId, siNull);
    }

    void setTransactionId(long transactionId, Object operation) {
        dataLib.addAttribute(operation, transactionIdAttribute, dataLib.encode(String.valueOf(transactionId)));
    }

    TransactionId getTransactionIdFromOperation(Object put) {
        Object value = dataLib.getAttribute(put, transactionIdAttribute);
        String transactionId = (String) dataLib.decode(value, String.class);
        if (transactionId != null) {
            return new TransactionId(transactionId);
        }
        return null;
    }

    void copyPutKeyValues(Object put, boolean skipPlaceHolder, Object newPut, long timestamp) {
        for (Object keyValue : dataLib.listPut(put)) {
            final Object qualifier = dataLib.getKeyValueQualifier(keyValue);
            if (!skipPlaceHolder || !dataLib.valuesEqual(placeHolderQualifier, qualifier)) {
                dataLib.addKeyValueToPut(newPut, dataLib.getKeyValueFamily(keyValue),
                        qualifier,
                        timestamp,
                        dataLib.getKeyValueValue(keyValue));
            }
        }
    }

    public Object copyPutToDelete(Object put, Set<Long> transactionIdsToDelete) {
        Object delete = dataLib.newDelete(dataLib.getPutKey(put));
        for (Long transactionId : transactionIdsToDelete) {
            for (Object keyValue : dataLib.listPut(put)) {
                dataLib.addKeyValueToDelete(delete, dataLib.getKeyValueFamily(keyValue),
                        dataLib.getKeyValueQualifier(keyValue), transactionId);
            }
            dataLib.addKeyValueToDelete(delete, siFamily, tombstoneQualifier, transactionId);
            dataLib.addKeyValueToDelete(delete, siFamily, commitTimestampQualifier, transactionId);
        }
        return delete;
    }

    List getCommitTimestamp(STable table, Object rowKey) throws IOException {
        final List<List<Object>> columns = Arrays.asList(Arrays.asList(siFamily, commitTimestampQualifier));
        Object get = dataLib.newGet(rowKey, null, columns, null);
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

    public boolean isSINull(Object value) {
        return dataLib.valuesEqual(value, siNull);
    }

    public boolean isSIFail(Object value) {
        return dataLib.valuesEqual(value, siFail);
    }

    public void recordRollForward(RollForwardQueue rollForwardQueue, ImmutableTransaction transaction, Object row) {
        recordRollForward(rollForwardQueue, transaction.getLongTransactionId(), row);
    }

    public void recordRollForward(RollForwardQueue rollForwardQueue, long transactionId, Object row) {
        if (rollForwardQueue != null) {
            rollForwardQueue.recordRow(transactionId, row);
        }
    }

    public void setCommitTimestamp(STable table, Object rowKey, long beginTimestamp, long transactionId) throws IOException {
        setCommitTimestampDirect(table, rowKey, beginTimestamp, dataLib.encode(transactionId));
    }

    public void setCommitTimestampToFail(STable table, Object rowKey, long transactionId) throws IOException {
        setCommitTimestampDirect(table, rowKey, transactionId, siFail);
    }

    private void setCommitTimestampDirect(STable table, Object rowKey, long transactionId, Object timestampValue) throws IOException {
        Object put = dataLib.newPut(rowKey);
        suppressIndexing(put);
        dataLib.addKeyValueToPut(put, siFamily, commitTimestampQualifier, transactionId, timestampValue);
        writer.write(table, put, false);
    }

    /**
     * When this new operation goes through the co-processor stack it should not be indexed (because it already has been
     * when the original operation went through).
     */
    public void suppressIndexing(Object newPut) {
        dataLib.addAttribute(newPut, SUPPRESS_INDEXING_ATTRIBUTE_NAME, SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
    }

    public void setTombstoneOnPut(Object put, long transactionId) {
        dataLib.addKeyValueToPut(put, siFamily, tombstoneQualifier, transactionId, siNull);
    }

    public void setTombstonesOnColumns(STable table, long timestamp, Object put) throws IOException {
        final Map userData = getUserData(table, dataLib.getPutKey(put));
        if (userData != null) {
            for (Object qualifier : userData.keySet()) {
                dataLib.addKeyValueToPut(put, userColumnFamily, qualifier, timestamp, siNull);
            }
        }
    }

    private Map getUserData(STable table, Object rowKey) throws IOException {
        final List<Object> families = Arrays.asList(userColumnFamily);
        Object get = dataLib.newGet(rowKey, families, null, null);
        dataLib.setGetMaxVersions(get, 1);
        Object result = reader.get(table, get);
        if (result != null) {
            return dataLib.getResultFamilyMap(result, userColumnFamily);
        }
        return null;
    }

    public void addSIFamilyToGet(Object read) {
        dataLib.addFamilyToGet(read, siFamily);
    }

    public void addSIFamilyToGetIfNeeded(Object read) {
        dataLib.addFamilyToGetIfNeeded(read, siFamily);
    }

    public void addSIFamilyToScan(Object read) {
        dataLib.addFamilyToScan(read, siFamily);
    }

    public void addSIFamilyToScanIfNeeded(Object read) {
        dataLib.addFamilyToScanIfNeeded(read, siFamily);
    }

    public void addPlaceHolderColumnToEmptyPut(Object put) {
        final List keyValues = dataLib.listPut(put);
        if (keyValues.isEmpty()) {
            dataLib.addKeyValueToPut(put, siFamily, placeHolderQualifier, 0L, siNull);
        }
    }
}
