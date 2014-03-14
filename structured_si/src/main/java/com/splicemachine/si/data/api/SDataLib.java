package com.splicemachine.si.data.api;

import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;

import com.splicemachine.hbase.KVPair;

/**
 * Defines an abstraction over the construction and manipulate of HBase operations. Having this abstraction allows an
 * alternate lightweight store to be used instead of HBase (e.g. for rapid testing).
 */
public interface SDataLib<
				Put extends OperationWithAttributes,
				Delete,
				Get extends OperationWithAttributes, Scan> {
    byte[] newRowKey(Object[] args);

		byte[] encode(Object value);
    <T> T decode(byte[] value, Class<T> type);

		List<Cell> listResult(Result result);

		Put newPut(byte[] key);
    Put newPut(byte[] key, HRegion.RowLock lock);
    void addKeyValueToPut(Put put, byte[] family, byte[] qualifier, long timestamp, byte[] value);
    Iterable<Cell> listPut(Put put);
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
    void addKeyValueToDelete(Delete delete, byte[] family, byte[] qualifier, long timestamp);

		KVPair toKVPair(Put put);

		Put toPut(KVPair kvPair, byte[] family, byte[] column, long longTransactionId);
}
