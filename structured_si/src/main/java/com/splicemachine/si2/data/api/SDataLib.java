package com.splicemachine.si2.data.api;

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

    Object getKeyValueRow(Object keyValue);
    Object getKeyValueFamily(Object keyValue);
    Object getKeyValueQualifier(Object keyValue);
    Object getKeyValueValue(Object keyValue);
    long getKeyValueTimestamp(Object keyValue);

    SGet newGet(Object rowKey, List families, List columns, Long effectiveTimestamp);
    void setGetTimeRange(SGet get, long minTimestamp, long maxTimestamp);
    void setGetMaxVersions(SGet get);
    void ensureFamilyOnGet(SGet get, Object family);

    SScan newScan(Object startRowKey, Object endRowKey, List families, List columns, Long effectiveTimestamp);
    void setScanTimeRange(SScan get, long minTimestamp, long maxTimestamp);
    void setScanMaxVersions(SScan scan);
    void ensureFamilyOnScan(SScan scan, Object family);
}
