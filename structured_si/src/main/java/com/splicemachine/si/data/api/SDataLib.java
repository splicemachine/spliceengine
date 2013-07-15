package com.splicemachine.si.data.api;

import java.util.List;
import java.util.Map;

/**
 * Defines an abstraction over the construction and manipulate of HBase operations. Having this abstraction allows an
 * alternate lightweight store to be used instead of HBase (e.g. for rapid testing).
 */
public interface SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus> {
    Data newRowKey(Object[] args);

    Data encode(Object value);
    Object decode(Data value, Class type);
    boolean valuesEqual(Data value1, Data value2);

    void addAttribute(OperationWithAttributes operation, String attributeName, Data value);
    Data getAttribute(OperationWithAttributes operation, String attributeName);

    Result newResult(Data key, List<KeyValue> keyValues);
    Data getResultKey(Result result);
    List<KeyValue> listResult(Result result);
    List<KeyValue> getResultColumn(Result result, Data family, Data qualifier);
    Data getResultValue(Result result, Data family, Data qualifier);
    Map<Data, Data> getResultFamilyMap(Result result, Data family);

    Put newPut(Data key);
    Put newPut(Data key, Lock lock);
    void addKeyValueToPut(Put put, Data family, Data qualifier, Long timestamp, Data value);
    Iterable<KeyValue> listPut(Put put);
    Data getPutKey(Put put);

    OperationStatus newFailStatus();

    KeyValue newKeyValue(Data rowKey, Data family, Data qualifier, Long timestamp, Data value);
    Data getKeyValueRow(KeyValue keyValue);
    Data getKeyValueFamily(KeyValue keyValue);
    Data getKeyValueQualifier(KeyValue keyValue);
    Data getKeyValueValue(KeyValue keyValue);
    long getKeyValueTimestamp(KeyValue keyValue);

    Get newGet(Data rowKey, List<Data> families, List<List<Data>> columns, Long effectiveTimestamp);
    void setGetTimeRange(Get get, long minTimestamp, long maxTimestamp);
    void setGetMaxVersions(Get get);
    void setGetMaxVersions(Get get, int max);
    void addFamilyToGet(Get read, Data family);
    void addFamilyToGetIfNeeded(Get get, Data family);

    Scan newScan(Data startRowKey, Data endRowKey, List<Data> families, List<List<Data>> columns, Long effectiveTimestamp);
    void setScanTimeRange(Scan get, long minTimestamp, long maxTimestamp);
    void setScanMaxVersions(Scan get);
    void setScanMaxVersions(Scan get, int max);
    void addFamilyToScan(Scan read, Data family);
    void addFamilyToScanIfNeeded(Scan get, Data family);

    Delete newDelete(Data rowKey);
    void addKeyValueToDelete(Delete delete, Data family, Data qualifier, long timestamp);
}
