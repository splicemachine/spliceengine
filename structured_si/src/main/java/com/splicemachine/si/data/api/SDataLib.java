package com.splicemachine.si.data.api;

import java.util.Comparator;
import java.util.List;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.utils.ByteSlice;

import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

/**
 * Defines an abstraction over the construction and manipulate of HBase operations. Having this abstraction allows an
 * alternate lightweight store to be used instead of HBase (e.g. for rapid testing).
 */
public interface SDataLib<Data,
				Put extends OperationWithAttributes,
				Delete,
				Get extends OperationWithAttributes, Scan> {
    byte[] newRowKey(Object[] args);
	byte[] encode(Object value);
    <T> T decode(byte[] value, Class<T> type);
	<T> T decode(byte[] value, int offset,int length, Class<T> type);
	List<Data> listResult(Result result);
	Put newPut(byte[] key);
    Put newPut(byte[] key, Integer lock);
    void addKeyValueToPut(Put put, byte[] family, byte[] qualifier, long timestamp, byte[] value);
    Iterable<Data> listPut(Put put);
    byte[] getPutKey(Put put);
	Get newGet(byte[] rowKey, List<byte[]> families, List<List<byte[]>> columns, Long effectiveTimestamp);
	Get newGet(byte[] rowKey, List<byte[]> families, List<List<byte[]>> columns, Long effectiveTimestamp, int maxVersions);	
    byte[] getGetRow(Get get);
    void setGetTimeRange(Get get, long minTimestamp, long maxTimestamp);
    void setGetMaxVersions(Get get);
    void setGetMaxVersions(Get get, int max);
    void addFamilyToGet(Get read, byte[] family);
    void addFamilyToGetIfNeeded(Get get, byte[] family);
    Scan newScan(byte[] startRowKey, byte[] endRowKey, List<byte[]> families, List<List<byte[]>> columns, Long effectiveTimestamp);
    void setScanTimeRange(Scan get, long minTimestamp, long maxTimestamp);
    void setScanMaxVersions(Scan get);
	void addFamilyToScan(Scan read, byte[] family);
    void addFamilyToScanIfNeeded(Scan get, byte[] family);
    Delete newDelete(byte[] rowKey);
    void addFamilyQualifierToDelete(Delete delete, byte[] family, byte[] qualifier, long timestamp);
    void addDataToDelete(Delete delete, Data data, long timestamp);
	KVPair toKVPair(Put put);
	Put toPut(KVPair kvPair, byte[] family, byte[] column, long longTransactionId);
	void setWriteToWAL(Put put, boolean writeToWAL);
	public boolean singleMatchingColumn(Data element, byte[] family, byte[] qualifier);
	public boolean singleMatchingFamily(Data element, byte[] family);
	public boolean singleMatchingQualifier(Data element, byte[] qualifier);
	public boolean matchingValue(Data element, byte[] value);
	public boolean matchingFamilyKeyValue(Data element, Data other);
	public boolean matchingQualifierKeyValue(Data element, Data other);
	public boolean matchingRowKeyValue(Data element, Data other);
	public Data newValue(Data element, byte[] value);
	public Data newValue(byte[] rowKey, byte[] family, byte[] qualifier, Long timestamp, byte[] value);
	public boolean isAntiTombstone(Data element, byte[] antiTombstone);
	public Comparator getComparator();
	public long getTimestamp(Data element);
	public String getFamilyAsString(Data element);
	public String getQualifierAsString(Data element);
	public void setRowInSlice(Data element, ByteSlice slice);
	public boolean isFailedCommitTimestamp(Data element);
	public Data newTransactionTimeStampKeyValue(Data element, byte[] value);
	public long getValueLength(Data element);
	public long getValueToLong(Data element);
	public byte[] getDataFamily(Data element);
	public byte[] getDataQualifier(Data element);
	public byte[] getDataValue(Data element);	
	public byte[] getDataRow(Data element);
	public byte[] getDataValueBuffer(Data element);
	public int getDataValueOffset(Data element);
	public int getDataValuelength(Data element);
	public int getLength(Data element);
	public Result newResult(List<Data> element);
	public Data[] getDataFromResult(Result result);
	public Data getColumnLatest(Result result, byte[] family, byte[] qualifier);
}
