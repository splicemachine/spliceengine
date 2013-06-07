package com.splicemachine.si.data.api;

import java.util.List;
import java.util.Map;

public interface SDataLib<Data, Result, KeyValue, Put, Delete, Get extends SGet, Scan extends SScan, Read extends SRead> {
    Data newRowKey(Object[] args);

    Data encode(Object value);
    Object decode(Data value, Class type);
    boolean valuesEqual(Data value1, Data value2);

    void addAttribute(Object operation, String attributeName, Data value);
    Data getAttribute(Object operation, String attributeName);

    Result newResult(Object key, List<KeyValue> keyValues);
    Data getResultKey(Result result);
    List<KeyValue> listResult(Result result);
    List<KeyValue> getResultColumn(Result result, Data family, Data qualifier);
    Data getResultValue(Result result, Data family, Data qualifier);
    Map<Data, Data> getResultFamilyMap(Result result, Data family);

    Put newPut(Data key);
    Put newPut(Data key, SRowLock lock);
    void addKeyValueToPut(Put put, Data family, Data qualifier, Long timestamp, Data value);
    List<KeyValue> listPut(Put put);
    Data getPutKey(Put put);

    KeyValue newKeyValue(Data rowKey, Data family, Data qualifier, Long timestamp, Data value);
    Data getKeyValueRow(KeyValue keyValue);
    Data getKeyValueFamily(KeyValue keyValue);
    Data getKeyValueQualifier(KeyValue keyValue);
    Data getKeyValueValue(KeyValue keyValue);
    long getKeyValueTimestamp(KeyValue keyValue);

    Get newGet(Data rowKey, List<Data> families, List<List<Data>> columns, Long effectiveTimestamp);
    Scan newScan(Data startRowKey, Data endRowKey, List<Data> families, List<List<Data>> columns, Long effectiveTimestamp);
    void setReadTimeRange(Read get, long minTimestamp, long maxTimestamp);
    void setReadMaxVersions(Read get);
    void setReadMaxVersions(Read get, int max);
    void addFamilyToRead(Read read, Data siFamily);
    void addFamilyToReadIfNeeded(Read get, Data family);

    Delete newDelete(Data rowKey);
    void addKeyValueToDelete(Delete delete, Data family, Data qualifier, long timestamp);
}
