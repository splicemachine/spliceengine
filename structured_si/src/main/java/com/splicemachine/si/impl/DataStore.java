package com.splicemachine.si.impl;

import static com.splicemachine.constants.SpliceConstants.CHECK_BLOOM_ATTRIBUTE_NAME;
import static com.splicemachine.constants.SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME;
import static com.splicemachine.constants.SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;

/**
 * Library of functions used by the SI module when accessing rows from data tables (data tables as opposed to the
 * transaction table).
 */
public class DataStore<Mutation, Put extends OperationWithAttributes, Delete, Get extends OperationWithAttributes, Scan, IHTable> {
	private static Logger LOG = Logger.getLogger(DataStore.class);
    final SDataLib<Put, Delete, Get, Scan> dataLib;
    private final STableReader<IHTable, Get, Scan> reader;
    private final STableWriter<IHTable, Mutation, Put, Delete> writer;
    private final String siNeededAttribute;
    private final byte[] siNeededValue;
    private final String transactionIdAttribute;
    private final String deletePutAttribute;
    private final byte[] commitTimestampQualifier;
    private final byte[] tombstoneQualifier;
    private final byte[] siNull;
    private final byte[] siAntiTombstoneValue;
    public final byte[] siFail;
    private final byte[] userColumnFamily;
    
    public DataStore(SDataLib<Put, Delete, Get, Scan> dataLib,
					STableReader reader,
					STableWriter writer,
					String siNeededAttribute,
                    byte[] siNeededValue,
					String transactionIdAttribute,
					String deletePutAttribute,
					byte[] siCommitQualifier,
					byte[] siTombstoneQualifier,
                    byte[] siNull,
                    byte[] siAntiTombstoneValue,
                    byte[] siFail,
                    byte[] userColumnFamily) {
        this.dataLib = dataLib;
        this.reader = reader;
        this.writer = writer;
        this.siNeededAttribute = siNeededAttribute;
        this.siNeededValue = dataLib.encode(siNeededValue);
        this.transactionIdAttribute = transactionIdAttribute;
        this.deletePutAttribute = deletePutAttribute;
        this.commitTimestampQualifier = dataLib.encode(siCommitQualifier);
        this.tombstoneQualifier = dataLib.encode(siTombstoneQualifier);
        this.siNull = dataLib.encode(siNull);
        this.siAntiTombstoneValue = dataLib.encode(siAntiTombstoneValue);
        this.siFail = dataLib.encode(siFail);
        this.userColumnFamily = dataLib.encode(userColumnFamily);
    }





	public void setSINeededAttribute(OperationWithAttributes operation) {
		operation.setAttribute(siNeededAttribute, siNeededValue);
    }

    public byte[] getSINeededAttribute(OperationWithAttributes operation) {
        return operation.getAttribute(siNeededAttribute);
    }

    public void setDeletePutAttribute(Put operation) {
		operation.setAttribute(deletePutAttribute, SIConstants.TRUE_BYTES);
    }

    public Boolean getDeletePutAttribute(OperationWithAttributes operation) {
        byte[] neededValue = operation.getAttribute(deletePutAttribute);
				if(neededValue==null) return false;
        return dataLib.decode(neededValue, Boolean.class);
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
        for (Cell keyValue : dataLib.listPut(put)) {
            final byte[] qualifier = CellUtil.cloneQualifier(keyValue);
            dataLib.addKeyValueToPut(newPut, keyValue.getFamilyArray(),
                    qualifier,
                    timestamp,
                    keyValue.getValueArray());
        }
    }

    public Delete copyPutToDelete(Put put, Set<Long> transactionIdsToDelete) {
        Delete delete = dataLib.newDelete(dataLib.getPutKey(put));
        for (Long transactionId : transactionIdsToDelete) {
            for (Cell keyValue : dataLib.listPut(put)) {
                dataLib.addKeyValueToDelete(delete,CellUtil.cloneFamily(keyValue),
                        CellUtil.cloneQualifier(keyValue), transactionId);
            }
            dataLib.addKeyValueToDelete(delete, userColumnFamily, tombstoneQualifier, transactionId);
            dataLib.addKeyValueToDelete(delete, userColumnFamily, commitTimestampQualifier, transactionId);
        }
        return delete;
    }


    Result getCommitTimestampsAndTombstonesSingle(IHTable table, byte[] rowKey) throws IOException {
    	// XXX TODO JLeach.
    	@SuppressWarnings("unchecked") final List<List<byte[]>> columns = Arrays.asList(
								Arrays.asList(userColumnFamily, tombstoneQualifier),
								Arrays.asList(userColumnFamily, commitTimestampQualifier),
								Arrays.asList(userColumnFamily, SIConstants.PACKED_COLUMN_BYTES)); // This needs to be static : why create this each time?
        Get get = dataLib.newGet(rowKey, null, columns, null,1); // Just Retrieve one per...
        suppressIndexing(get);
        checkBloom(get);
        return reader.get(table, get);
    }

    public void checkBloom(OperationWithAttributes operation) {
				operation.setAttribute(CHECK_BLOOM_ATTRIBUTE_NAME, userColumnFamily);
    }

    boolean isAntiTombstone(Cell keyValue) {
				byte[] buffer = ((KeyValue)keyValue).getBuffer();
				int valueOffset = keyValue.getValueOffset();
				int valueLength = keyValue.getValueLength();
				return Bytes.equals(siAntiTombstoneValue,0,siAntiTombstoneValue.length,buffer,valueOffset,valueLength);
		}

    public CellType getKeyValueType(Cell keyValue) {
				if(CellUtils.singleMatchingQualifier(keyValue, commitTimestampQualifier)){
					return CellType.COMMIT_TIMESTAMP;
				} else if(CellUtils.singleMatchingQualifier(keyValue, SIConstants.PACKED_COLUMN_BYTES)){
					   return CellType.USER_DATA;
				} else { // Took out the check...
		            if (CellUtils.matchingValue(keyValue, siNull)) {
		                return CellType.TOMBSTONE;
		            } else if (CellUtils.matchingValue(keyValue, siAntiTombstoneValue)) {
		                return CellType.ANTI_TOMBSTONE;
		            } else {
		                return CellType.OTHER;
		            }
	        }
    }

    public boolean isSINull(Cell keyValue) {
        return CellUtils.matchingValue(keyValue, siNull);
    }

    public boolean isSIFail(Cell keyValue) {
        return CellUtils.matchingValue(keyValue, siFail);
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
        dataLib.addKeyValueToPut(put, userColumnFamily, commitTimestampQualifier, transactionId, timestampValue);
        writer.write(table, put);
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
        dataLib.addKeyValueToPut(put, userColumnFamily, tombstoneQualifier, transactionId, siNull);
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
        dataLib.addKeyValueToPut(put, userColumnFamily, tombstoneQualifier, transactionId, siAntiTombstoneValue);
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

    public OperationStatus[] writeBatch(IHTable table, Mutation[] mutations) throws IOException {
        if (mutations == null || mutations.length == 0)
            return new OperationStatus[] {};
        return writer.writeBatch(table, mutations);
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
