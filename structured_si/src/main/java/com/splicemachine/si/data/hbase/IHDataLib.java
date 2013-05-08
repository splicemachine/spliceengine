package com.splicemachine.si.data.hbase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;

import java.util.List;
import java.util.Map;

public interface IHDataLib {
    byte[] newRowKey(Object[] args);

    byte[] encode(Object value);
    Object decode(byte[] value, Class type);
    boolean valuesEqual(byte[] value1, byte[] value2);

    void addAttribute(OperationWithAttributes operation, String attributeName, byte[] value);
    byte[] getAttribute(OperationWithAttributes operation, String attributeName);

    Result newResult(byte[] key, List keyValues);
    byte[] getResultKey(Result result);
    List listResult(Result result);
    List getResultColumn(Result result, byte[] family, byte[] qualifier);
    byte[] getResultValue(Result result, byte[] family, byte[] qualifier);
    Map getResultFamilyMap(Result result, byte[] family);

    KeyValue newKeyValue(byte[] rowKey, byte[] family, byte[] qualifier, Long timestamp, byte[] value);
    byte[] getKeyValueRow(KeyValue keyValue);
    byte[] getKeyValueFamily(KeyValue keyValue);
    byte[] getKeyValueQualifier(KeyValue keyValue);
    byte[] getKeyValueValue(KeyValue keyValue);
    long getKeyValueTimestamp(KeyValue keyValue);

    Put newPut(byte[] key);
    Put newPut(byte[] key, RowLock lock);
    void addKeyValueToPut(Put put, byte[] family, byte[] qualifier, Long timestamp, byte[] value);
    List listPut(Put put);
    byte[] getPutKey(Put put);

    Get newGet(byte[] rowKey, List families, List<List> columns, Long effectiveTimestamp);
    Scan newScan(byte[] startRowKey, byte[] endRowKey, List families, List<List> columns,
                 Long effectiveTimestamp);
    void setReadTimeRange(Get get, long minTimestamp, long maxTimestamp);
    void setReadTimeRange(Scan get, long minTimestamp, long maxTimestamp);
    void setReadMaxVersions(Get get);
    void setReadMaxVersions(Get get, int max);
    void setReadMaxVersions(Scan scan);
    void setReadMaxVersions(Scan scan, int max);
    void addFamilyToReadIfNeeded(Get get, byte[] family);
    void addFamilyToReadIfNeeded(Scan scan, byte[] family);
}
