package com.splicemachine.si.data.api;

import java.util.List;
import java.util.Map;

public interface SDataLib {
    Object newRowKey(Object[] args);

    Object encode(Object value);
    Object decode(Object value, Class type);
    boolean valuesEqual(Object value1, Object value2);

    void addAttribute(Object operation, String attributeName, Object value);
    Object getAttribute(Object operation, String attributeName);

    Object newResult(Object key, List keyValues);
    Object getResultKey(Object result);
    List listResult(Object result);
    List getResultColumn(Object result, Object family, Object qualifier);
    Object getResultValue(Object result, Object family, Object qualifier);
    Map getResultFamilyMap(Object result, Object family);

    Object newPut(Object key);
    Object newPut(Object key, SRowLock lock);
    void addKeyValueToPut(Object put, Object family, Object qualifier, Long timestamp, Object value);
    List listPut(Object put);
    Object getPutKey(Object put);

    Object newKeyValue(Object rowKey, Object family, Object qualifier, Long timestamp, Object value);
    Object getKeyValueRow(Object keyValue);
    Object getKeyValueFamily(Object keyValue);
    Object getKeyValueQualifier(Object keyValue);
    Object getKeyValueValue(Object keyValue);
    long getKeyValueTimestamp(Object keyValue);

    SGet newGet(Object rowKey, List families, List columns, Long effectiveTimestamp);
    SScan newScan(Object startRowKey, Object endRowKey, List families, List columns, Long effectiveTimestamp);
    void setReadTimeRange(SRead get, long minTimestamp, long maxTimestamp);
    void setReadMaxVersions(SRead get);
    void setReadMaxVersions(SRead get, int max);
    void addFamilyToRead(SRead read, Object siFamily);
    void addFamilyToReadIfNeeded(SRead get, Object family);

    Object newDelete(Object rowKey);
    void addKeyValueToDelete(Object delete, Object family, Object qualifier, long timestamp);
}
