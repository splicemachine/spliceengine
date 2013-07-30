package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
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
public class DataStore<Data, Hashable extends Comparable, Result, KeyValue, OperationWithAttributes, Mutation, Put extends OperationWithAttributes,
        Delete, Get extends OperationWithAttributes, Scan, IHTable, Lock, OperationStatus, Scanner> {
    final SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus> dataLib;
    private final STableReader<IHTable, Result, Get, Scan, KeyValue, Scanner, Data> reader;
    private final STableWriter<IHTable, Mutation, Put, Delete, Data, Lock, OperationStatus> writer;
    private final Hasher<Data, Hashable> hasher;

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
    private final Data siNull;
    private final Data siAntiTombstoneValue;
    final Data siFail;

    private final Data userColumnFamily;

    public DataStore(SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus>
                             dataLib, STableReader reader, STableWriter writer, String siNeededAttribute,
                     Object siNeededValue, Object includeSIColumnValue, String includeUncommittedAsOfStartAttribute,
                     Object includeUncommittedAsOfStartValue, String transactionIdAttribute, String deletePutAttribute,
                     String siMetaFamily, Object siCommitQualifier, Object siTombstoneQualifier,
                     Object siNull, Object siAntiTombstoneValue, Object siFail, Object userColumnFamily, Hasher<Data, Hashable> hasher) {
        this.dataLib = dataLib;
        this.reader = reader;
        this.writer = writer;
        this.hasher = hasher;

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
        this.siNull = dataLib.encode(siNull);
        this.siAntiTombstoneValue = dataLib.encode(siAntiTombstoneValue);
        this.siFail = dataLib.encode(siFail);
        this.userColumnFamily = dataLib.encode(userColumnFamily);
    }

    void setSINeededAttribute(OperationWithAttributes operation, boolean includeSIColumn) {
        dataLib.addAttribute(operation, siNeededAttribute, dataLib.encode(includeSIColumn ? includeSIColumnValue : siNeededValue));
    }

    Data getSINeededAttribute(OperationWithAttributes operation) {
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
        dataLib.addAttribute(operation, deletePutAttribute, dataLib.encode(true));
    }

    Boolean getDeletePutAttribute(OperationWithAttributes operation) {
        Data neededValue = dataLib.getAttribute(operation, deletePutAttribute);
        return (Boolean) dataLib.decode(neededValue, Boolean.class);
    }

    void addTransactionIdToPutKeyValues(Put put, long transactionId) {
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

    void copyPutKeyValues(Put put, Put newPut, long timestamp) {
        for (KeyValue keyValue : dataLib.listPut(put)) {
            final Data qualifier = dataLib.getKeyValueQualifier(keyValue);
            dataLib.addKeyValueToPut(newPut, dataLib.getKeyValueFamily(keyValue),
                    qualifier,
                    timestamp,
                    dataLib.getKeyValueValue(keyValue));
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

    public Scan getCommitTimestampsAndTombstonesScan(ScanBounds<Data> scanBounds) throws IOException {
        final List<List<Data>> columns = Arrays.asList(
                Arrays.asList(siFamily, tombstoneQualifier),
                Arrays.asList(siFamily, commitTimestampQualifier));
        return dataLib.newScan(scanBounds.minKey, dataLib.increment(scanBounds.maxKey), null, columns, null);
    }

    public List<KeyValue>[] splitCommitTimestampsAndTombstones(List<KeyValue> result) {
        if (result == null) {
            return new List[]{null, null};
        } else {
            List<KeyValue> tombstones = new ArrayList<KeyValue>();
            List<KeyValue> commitTimestamps = new ArrayList<KeyValue>();
            for (KeyValue kv : result) {
                if (dataLib.valuesEqual(dataLib.getKeyValueQualifier(kv), tombstoneQualifier)) {
                    tombstones.add(kv);
                } else {
                    if (!(dataLib.valuesEqual(dataLib.getKeyValueQualifier(kv), commitTimestampQualifier))) {
                        throw new RuntimeException("unexpected qualifier");
                    }
                    commitTimestamps.add(kv);
                }
            }
            return new List[]{tombstones, commitTimestamps};
        }
    }

    List<KeyValue>[] getCommitTimestampsAndTombstonesSingle(IHTable table, Data rowKey) throws IOException {
        final List<List<Data>> columns = Arrays.asList(
                Arrays.asList(siFamily, tombstoneQualifier),
                Arrays.asList(siFamily, commitTimestampQualifier));
        Get get = dataLib.newGet(rowKey, null, columns, null);
        suppressIndexing(get);
        Result result = reader.get(table, get);
        if (result != null) {
            return new List[]{dataLib.getResultColumn(result, siFamily, tombstoneQualifier),
                    dataLib.getResultColumn(result, siFamily, commitTimestampQualifier)};
        }
        return new List[]{null, null};
    }

    boolean isAntiTombstone(KeyValue keyValue) {
        return dataLib.valuesEqual(siAntiTombstoneValue, dataLib.getKeyValueValue(keyValue));
    }

    public KeyValueType getKeyValueType(Data family, Data qualifier, Data value) {
        final boolean isSIFamily = dataLib.valuesEqual(family, siFamily);
        if (isSIFamily && dataLib.valuesEqual(qualifier, commitTimestampQualifier)) {
            return KeyValueType.COMMIT_TIMESTAMP;
        } else if (isSIFamily && dataLib.valuesEqual(qualifier, tombstoneQualifier)) {
            if (dataLib.valuesEqual(value, siNull)) {
                return KeyValueType.TOMBSTONE;
            } else if (dataLib.valuesEqual(value, siAntiTombstoneValue)) {
                return KeyValueType.ANTI_TOMBSTONE;
            } else {
                return KeyValueType.OTHER;
            }
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

    public void recordRollForward(RollForwardQueue<Data, Hashable> rollForwardQueue, long transactionId, Data row) {
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
        suppressIndexing(put);
        dataLib.addKeyValueToPut(put, siFamily, commitTimestampQualifier, transactionId, timestampValue);
        writer.write(table, put, false);
    }

    /**
     * When this new operation goes through the co-processor stack it should not be indexed (because it already has been
     * when the original operation went through).
     */
    public void suppressIndexing(OperationWithAttributes operation) {
        dataLib.addAttribute(operation, SUPPRESS_INDEXING_ATTRIBUTE_NAME, (Data) SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
    }

    public boolean isSuppressIndexing(OperationWithAttributes operation) {
        return dataLib.getAttribute(operation, SUPPRESS_INDEXING_ATTRIBUTE_NAME) != null;
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

    public void setAntiTombstoneOnPut(Put put, long transactionId) throws IOException {
        dataLib.addKeyValueToPut(put, siFamily, tombstoneQualifier, transactionId, siAntiTombstoneValue);
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
        final Iterable<KeyValue> keyValues = dataLib.listPut(put);
        if (!keyValues.iterator().hasNext()) {
            dataLib.addKeyValueToPut(put, siFamily, commitTimestampQualifier, 0L, siNull);
        }
    }

    public OperationStatus[] writeBatch(IHTable table, Pair<Mutation, Lock>[] mutationsAndLocks) throws IOException {
        return writer.writeBatch(table, mutationsAndLocks);
    }

    public void closeLowLevelScan(IHTable table) {
        reader.closeRegionScanner(table);
    }

    public Scanner startLowLevelScan(IHTable table, Scan resultsScan) throws IOException {
        return reader.openRegionScanner(table, resultsScan);
    }

    public SeekScanner newSeekScanner(Scanner scanner) {
        return new SeekScanner(reader, dataLib, hasher, scanner);
    }
}
