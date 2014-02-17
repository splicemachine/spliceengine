package com.splicemachine.si.impl;

import com.splicemachine.hbase.KeyValueUtils;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.splicemachine.constants.SpliceConstants.CHECK_BLOOM_ATTRIBUTE_NAME;
import static com.splicemachine.constants.SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME;
import static com.splicemachine.constants.SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE;

/**
 * Library of functions used by the SI module when accessing rows from data tables (data tables as opposed to the
 * transaction table).
 */
public class DataStore<Mutation, Put extends OperationWithAttributes,
        Delete, Get extends OperationWithAttributes, Scan, IHTable> {
    final SDataLib<Put, Delete, Get, Scan> dataLib;
    private final STableReader<IHTable, Get, Scan> reader;
    private final STableWriter<IHTable, Mutation, Put, Delete> writer;

    private final String siNeededAttribute;
    private final byte[] siNeededValue;
    private final byte[] includeSIColumnValue;
    private final String transactionIdAttribute;
    private final String deletePutAttribute;

    private final byte[] siFamily;
    private final byte[] commitTimestampQualifier;
    private final byte[] tombstoneQualifier;
    private final byte[] siNull;
    private final byte[] siAntiTombstoneValue;
    final byte[] siFail;

    private final byte[] userColumnFamily;

    public DataStore(SDataLib<Put, Delete, Get, Scan>
                             dataLib,
										 STableReader reader,
										 STableWriter writer,
										 String siNeededAttribute,
                     Object siNeededValue,
										 Object includeSIColumnValue,
										 String transactionIdAttribute,
										 String deletePutAttribute,
                     byte[] siMetaFamily,
										 byte[] siCommitQualifier,
										 byte[] siTombstoneQualifier,
                     Object siNull,
										 Object siAntiTombstoneValue,
										 Object siFail,
										 Object userColumnFamily) {
        this.dataLib = dataLib;
        this.reader = reader;
        this.writer = writer;

        this.siNeededAttribute = siNeededAttribute;
        this.siNeededValue = dataLib.encode(siNeededValue);
        this.includeSIColumnValue = dataLib.encode(includeSIColumnValue);
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

    public void setSINeededAttribute(OperationWithAttributes operation, boolean includeSIColumn) {
				operation.setAttribute(siNeededAttribute, dataLib.encode(includeSIColumn ? includeSIColumnValue : siNeededValue));
    }

    public byte[] getSINeededAttribute(OperationWithAttributes operation) {
        return operation.getAttribute(siNeededAttribute);
    }

    boolean isIncludeSIColumn(OperationWithAttributes operation) {
				byte[] attribute = operation.getAttribute(siNeededAttribute);
				return attribute != null && Bytes.equals(attribute, includeSIColumnValue);
    }

    public void setDeletePutAttribute(Put operation) {
				operation.setAttribute(deletePutAttribute, dataLib.encode(true)); //TODO -sf- cache this encoding
    }

    public Boolean getDeletePutAttribute(OperationWithAttributes operation) {
        byte[] neededValue = operation.getAttribute(deletePutAttribute);
				if(neededValue==null) return false;
        return dataLib.decode(neededValue, Boolean.class);
    }

    void addTransactionIdToPutKeyValues(Put put, long transactionId) {
        dataLib.addKeyValueToPut(put, siFamily, commitTimestampQualifier, transactionId, siNull);
    }

    public void setTransactionId(long transactionId, OperationWithAttributes operation) {
				operation.setAttribute(transactionIdAttribute, dataLib.encode(String.valueOf(transactionId)));
    }

    public TransactionId getTransactionIdFromOperation(OperationWithAttributes put) {
        byte[] value = put.getAttribute(transactionIdAttribute);
				if(value==null) return null;
				return new TransactionId(Bytes.toString(value));
    }

		public String getTransactionid(OperationWithAttributes owa){
				byte[] value = owa.getAttribute(transactionIdAttribute);
				if(value==null) return null;
				return Bytes.toString(value);
		}

    void copyPutKeyValues(Put put, Put newPut, long timestamp) {
        for (KeyValue keyValue : dataLib.listPut(put)) {
            final byte[] qualifier = keyValue.getQualifier();
            dataLib.addKeyValueToPut(newPut, keyValue.getFamily(),
                    qualifier,
                    timestamp,
                    keyValue.getValue());
        }
    }

    public Delete copyPutToDelete(Put put, Set<Long> transactionIdsToDelete) {
        Delete delete = dataLib.newDelete(dataLib.getPutKey(put));
        for (Long transactionId : transactionIdsToDelete) {
            for (KeyValue keyValue : dataLib.listPut(put)) {
                dataLib.addKeyValueToDelete(delete, keyValue.getFamily(),
                        keyValue.getQualifier(), transactionId);
            }
            dataLib.addKeyValueToDelete(delete, siFamily, tombstoneQualifier, transactionId);
            dataLib.addKeyValueToDelete(delete, siFamily, commitTimestampQualifier, transactionId);
        }
        return delete;
    }
//
//    public Scan getCommitTimestampsAndTombstonesScan(ScanBounds<Data> scanBounds) throws IOException {
//        final List<List<byte[]>> columns = Arrays.asList(
//                Arrays.asList(siFamily, tombstoneQualifier),
//                Arrays.asList(siFamily, commitTimestampQualifier));
//        return dataLib.newScan(scanBounds.minKey, dataLib.increment(scanBounds.maxKey), null, columns, null);
//    }
//
//    public List<KV>[] splitCommitTimestampsAndTombstones(List<KV> result) {
//        if (result == null) {
//            return new List[]{null, null};
//        } else {
//            List<KV> tombstones = new ArrayList<KV>();
//            List<KV> commitTimestamps = new ArrayList<KV>();
//            for (KV kv : result) {
//                if (dataLib.valuesEqual(dataLib.getKeyValueQualifier(kv), tombstoneQualifier)) {
//                    tombstones.add(kv);
//                } else {
//                    if (!(dataLib.valuesEqual(dataLib.getKeyValueQualifier(kv), commitTimestampQualifier))) {
//                        throw new RuntimeException("unexpected qualifier");
//                    }
//                    commitTimestamps.add(kv);
//                }
//            }
//            return new List[]{tombstones, commitTimestamps};
//        }
//    }

    List<KeyValue>[] getCommitTimestampsAndTombstonesSingle(IHTable table, byte[] rowKey) throws IOException {
        @SuppressWarnings("unchecked") final List<List<byte[]>> columns = Arrays.asList(
								Arrays.asList(siFamily, tombstoneQualifier),
								Arrays.asList(siFamily, commitTimestampQualifier));
        Get get = dataLib.newGet(rowKey, null, columns, null);
        suppressIndexing(get);
        checkBloom(get);
        Result result = reader.get(table, get);
        if (result != null) {
						//noinspection unchecked
						return new List[]{
                    result.getColumn(siFamily, tombstoneQualifier),
                    result.getColumn(siFamily, commitTimestampQualifier)};
        }
				//noinspection unchecked
				return new List[]{null, null};
    }

    public void checkBloom(OperationWithAttributes operation) {
				operation.setAttribute(CHECK_BLOOM_ATTRIBUTE_NAME, siFamily);
    }

    boolean isAntiTombstone(KeyValue keyValue) {
				byte[] buffer = keyValue.getBuffer();
				int valueOffset = keyValue.getValueOffset();
				int valueLength = keyValue.getValueLength();
				return Bytes.equals(siAntiTombstoneValue,0,siAntiTombstoneValue.length,buffer,valueOffset,valueLength);
//				byte[] keyValueValue = dataLib.getKeyValueValue(keyValue);
//				return keyValueValue != null && Bytes.equals(siAntiTombstoneValue, keyValueValue);
		}

    public KeyValueType getKeyValueType(KeyValue keyValue) {
				if(keyValue.matchingFamily(siFamily)){
						if(keyValue.matchingQualifier(commitTimestampQualifier)){
	            return KeyValueType.COMMIT_TIMESTAMP;
	        } else { // Took out the check...
	            if (KeyValueUtils.matchingValue(keyValue, siNull)) {
	                return KeyValueType.TOMBSTONE;
	            } else if (KeyValueUtils.matchingValue(keyValue, siAntiTombstoneValue)) {
	                return KeyValueType.ANTI_TOMBSTONE;
	            } else {
	                return KeyValueType.OTHER;
	            }
	        }
	   }else if(keyValue.matchingFamily(userColumnFamily)){
		   return KeyValueType.USER_DATA;
	   } else {
	       return KeyValueType.OTHER;
	   }
    }

    public boolean isSINull(KeyValue keyValue) {
        return KeyValueUtils.matchingValue(keyValue, siNull);
    }

    public boolean isSIFail(KeyValue keyValue) {
        return KeyValueUtils.matchingValue(keyValue, siFail);
    }

    public void recordRollForward(RollForwardQueue rollForwardQueue, long transactionId, byte[] row, Boolean knownToBeCommitted) {
        if (rollForwardQueue != null) {
            rollForwardQueue.recordRow(transactionId, row, knownToBeCommitted);
        }
    }

    public void setCommitTimestamp(IHTable table, byte[] rowKey, long beginTimestamp, long transactionId) throws IOException {
        setCommitTimestampDirect(table, rowKey, beginTimestamp, dataLib.encode(transactionId));
    }

    public void setCommitTimestampToFail(IHTable table, byte[] rowKey, long transactionId) throws IOException {
        setCommitTimestampDirect(table, rowKey, transactionId, siFail);
    }

    private void setCommitTimestampDirect(IHTable table, byte[] rowKey, long transactionId, byte[] timestampValue) throws IOException {
        Put put = dataLib.newPut(rowKey);
        suppressIndexing(put);
        dataLib.addKeyValueToPut(put, siFamily, commitTimestampQualifier, transactionId, timestampValue);
        writer.write(table, put, false);
    }

    /**
     * When this new operation goes through the co-processor stack it should not be indexed (because it already has been
     * when the original operation went through).
		 * @param operation
		 */
    public void suppressIndexing(OperationWithAttributes operation) {
				operation.setAttribute(SUPPRESS_INDEXING_ATTRIBUTE_NAME, SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
    }

    public boolean isSuppressIndexing(OperationWithAttributes operation) {
        return operation.getAttribute(SUPPRESS_INDEXING_ATTRIBUTE_NAME) != null;
    }

    public void setTombstoneOnPut(Put put, long transactionId) {
        dataLib.addKeyValueToPut(put, siFamily, tombstoneQualifier, transactionId, siNull);
    }

    public void setTombstonesOnColumns(IHTable table, long timestamp, Put put) throws IOException {
        final Map<byte[],byte[]> userData = getUserData(table, dataLib.getPutKey(put));
        if (userData != null) {
            for (byte[] qualifier : userData.keySet()) {
                dataLib.addKeyValueToPut(put, userColumnFamily, qualifier, timestamp, siNull);
            }
        }
    }

    public void setAntiTombstoneOnPut(Put put, long transactionId) throws IOException {
        dataLib.addKeyValueToPut(put, siFamily, tombstoneQualifier, transactionId, siAntiTombstoneValue);
    }

    private Map<byte[], byte[]> getUserData(IHTable table, byte[] rowKey) throws IOException {
        final List<byte[]> families = Arrays.asList(userColumnFamily);
        Get get = dataLib.newGet(rowKey, families, null, null);
        dataLib.setGetMaxVersions(get, 1);
        Result result = reader.get(table, get);
        if (result != null) {
            return result.getFamilyMap(userColumnFamily);
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

    public OperationStatus[] writeBatch(IHTable table, Pair<Mutation, Integer>[] mutationsAndLocks) throws IOException {
        return writer.writeBatch(table, mutationsAndLocks);
    }

    public void closeLowLevelOperation(IHTable table) throws IOException {
        reader.closeOperation(table);
    }

    public void startLowLevelOperation(IHTable table) throws IOException {
        reader.openOperation(table);
    }

    public String getTableName(IHTable table) {
        return reader.getTableName(table);
    }
}
