package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;
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
public class DataStore<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, IHTable, Lock> {
    final SDataLib<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Lock> dataLib;
    private final STableReader<IHTable, Result, Get, Scan> reader;
    private final STableWriter<IHTable, Put, Delete, Data, Lock> writer;

    private final String siNeededAttribute;
    private final Data siNeededValue;
    private final Data includeSIColumnValue;
    private final String includeUncommittedAsOfStartAttribute;
    private final Data includeUncommittedAsOfStartValue;
    private final String transactionIdAttribute;
    private final String deletePutAttribute;

    private final Data siFamily;
    private final Data commitTimestampQualifier;
    private final Data tombstoneQualifier;
    private final Data placeHolderQualifier;
    private final Data siNull;
    final Data siFail;

    private final Data userColumnFamily;

    public DataStore(SDataLib<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Lock>
                     dataLib, STableReader reader, STableWriter writer, String siNeededAttribute,
                     Data siNeededValue, Data includeSIColumnValue, String includeUncommittedAsOfStartAttribute,
                     Data includeUncommittedAsOfStartValue, String transactionIdAttribute, String deletePutAttribute,
                     String siMetaFamily, Data siCommitQualifier, Data siTombstoneQualifier,
                     Data placeHolderQualifier, Data siMetaNull, Data siFail, Data userColumnFamily) {
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

    void setSINeededAttribute(OperationWithAttributes operation, boolean includeSIColumn) {
        dataLib.addAttribute(operation, siNeededAttribute, dataLib.encode(includeSIColumn ? includeSIColumnValue : siNeededValue));
    }

    Object getSINeededAttribute(OperationWithAttributes operation) {
        return dataLib.getAttribute(operation, siNeededAttribute);
    }

    boolean isIncludeSIColumn(OperationWithAttributes operation) {
        return dataLib.valuesEqual(dataLib.getAttribute(operation, siNeededAttribute), includeSIColumnValue);
    }

    void setIncludeUncommittedAsOfStart(OperationWithAttributes operation) {
        dataLib.addAttribute(operation, includeUncommittedAsOfStartAttribute, includeUncommittedAsOfStartValue);
    }

    boolean isScanIncludeUncommittedAsOfStart(OperationWithAttributes operation) {
        return dataLib.valuesEqual(dataLib.getAttribute(operation, includeUncommittedAsOfStartAttribute), includeUncommittedAsOfStartValue);
    }

    void setDeletePutAttribute(Put operation) {
        dataLib.addAttribute((OperationWithAttributes) operation, deletePutAttribute, dataLib.encode(true));
    }

    Boolean getDeletePutAttribute(OperationWithAttributes operation) {
        Data neededValue = dataLib.getAttribute(operation, deletePutAttribute);
        return (Boolean) dataLib.decode(neededValue, Boolean.class);
    }

    void addTransactionIdToPut(Put put, long transactionId) {
        dataLib.addKeyValueToPut(put, siFamily, commitTimestampQualifier, transactionId, siNull);
    }

    void setTransactionId(long transactionId, OperationWithAttributes operation) {
        dataLib.addAttribute(operation, transactionIdAttribute, dataLib.encode(String.valueOf(transactionId)));
    }

    TransactionId getTransactionIdFromOperation(OperationWithAttributes put) {
        Data value = dataLib.getAttribute(put, transactionIdAttribute);
        String transactionId = (String) dataLib.decode(value, String.class);
        if (transactionId != null) {
            return new TransactionId(transactionId);
        }
        return null;
    }

    void copyPutKeyValues(Put put, boolean skipPlaceHolder, Put newPut, long timestamp) {
        for (KeyValue keyValue : dataLib.listPut(put)) {
            final Data qualifier = dataLib.getKeyValueQualifier(keyValue);
            if (!skipPlaceHolder || !dataLib.valuesEqual(placeHolderQualifier, qualifier)) {
                dataLib.addKeyValueToPut(newPut, dataLib.getKeyValueFamily(keyValue),
                        qualifier,
                        timestamp,
                        dataLib.getKeyValueValue(keyValue));
            }
        }
    }

    public Delete copyPutToDelete(Put put, Set<Long> transactionIdsToDelete) {
        Delete delete = dataLib.newDelete(dataLib.getPutKey(put));
        for (Long transactionId : transactionIdsToDelete) {
            for (KeyValue keyValue : dataLib.listPut(put)) {
                dataLib.addKeyValueToDelete(delete, dataLib.getKeyValueFamily(keyValue),
                        dataLib.getKeyValueQualifier(keyValue), transactionId);
            }
            dataLib.addKeyValueToDelete(delete, siFamily, tombstoneQualifier, transactionId);
            dataLib.addKeyValueToDelete(delete, siFamily, commitTimestampQualifier, transactionId);
        }
        return delete;
    }

    List<KeyValue> getCommitTimestamp(IHTable table, Data rowKey) throws IOException {
        final List<List<Data>> columns = Arrays.asList(Arrays.asList(siFamily, commitTimestampQualifier));
        Get get = dataLib.newGet(rowKey, null, columns, null);
        Result result = reader.get(table, get);
        if (result != null) {
            return dataLib.getResultColumn(result, siFamily, commitTimestampQualifier);
        }
        return null;
    }

    public KeyValueType getKeyValueType(Data family, Data qualifier) {
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

    public boolean isSINull(Data value) {
        return dataLib.valuesEqual(value, siNull);
    }

    public boolean isSIFail(Data value) {
        return dataLib.valuesEqual(value, siFail);
    }

    public void recordRollForward(RollForwardQueue<Data> rollForwardQueue, ImmutableTransaction transaction, Data row) {
        recordRollForward(rollForwardQueue, transaction.getLongTransactionId(), row);
    }

    public void recordRollForward(RollForwardQueue<Data> rollForwardQueue, long transactionId, Data row) {
        if (rollForwardQueue != null) {
            rollForwardQueue.recordRow(transactionId, row);
        }
    }

    public void setCommitTimestamp(IHTable table, Data rowKey, long beginTimestamp, long transactionId) throws IOException {
        setCommitTimestampDirect(table, rowKey, beginTimestamp, dataLib.encode(transactionId));
    }

    public void setCommitTimestampToFail(IHTable table, Data rowKey, long transactionId) throws IOException {
        setCommitTimestampDirect(table, rowKey, transactionId, siFail);
    }

    private void setCommitTimestampDirect(IHTable table, Data rowKey, long transactionId, Data timestampValue) throws IOException {
        Put put = dataLib.newPut(rowKey);
        suppressIndexing((OperationWithAttributes) put);
        dataLib.addKeyValueToPut(put, siFamily, commitTimestampQualifier, transactionId, timestampValue);
        writer.write(table, put, false);
    }

    /**
     * When this new operation goes through the co-processor stack it should not be indexed (because it already has been
     * when the original operation went through).
     */
    public void suppressIndexing(OperationWithAttributes newPut) {
        dataLib.addAttribute(newPut, SUPPRESS_INDEXING_ATTRIBUTE_NAME, (Data) SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
    }

    public void setTombstoneOnPut(Put put, long transactionId) {
        dataLib.addKeyValueToPut(put, siFamily, tombstoneQualifier, transactionId, siNull);
    }

    public void setTombstonesOnColumns(IHTable table, long timestamp, Put put) throws IOException {
        final Map<Data, Data> userData = getUserData(table, dataLib.getPutKey(put));
        if (userData != null) {
            for (Data qualifier : userData.keySet()) {
                dataLib.addKeyValueToPut(put, userColumnFamily, qualifier, timestamp, siNull);
            }
        }
    }

    private Map<Data, Data> getUserData(IHTable table, Data rowKey) throws IOException {
        final List<Data> families = Arrays.asList(userColumnFamily);
        Get get = dataLib.newGet(rowKey, families, null, null);
        dataLib.setGetMaxVersions(get, 1);
        Result result = reader.get(table, get);
        if (result != null) {
            return dataLib.getResultFamilyMap(result, userColumnFamily);
        }
        return null;
    }

    public void addSIFamilyToGet(Get read) {
        dataLib.addFamilyToGet(read, siFamily);
    }

    public void addSIFamilyToGetIfNeeded(Get read) {
        dataLib.addFamilyToGetIfNeeded(read, siFamily);
    }

    public void addSIFamilyToScan(Scan read) {
        dataLib.addFamilyToScan(read, siFamily);
    }

    public void addSIFamilyToScanIfNeeded(Scan read) {
        dataLib.addFamilyToScanIfNeeded(read, siFamily);
    }

    public void addPlaceHolderColumnToEmptyPut(Put put) {
        final List<KeyValue> keyValues = dataLib.listPut(put);
        if (keyValues.isEmpty()) {
            dataLib.addKeyValueToPut(put, siFamily, placeHolderQualifier, 0L, siNull);
        }
    }
}
