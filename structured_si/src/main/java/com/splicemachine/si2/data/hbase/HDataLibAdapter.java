package com.splicemachine.si2.data.hbase;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.SRowLock;
import com.splicemachine.si2.data.api.SScan;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;

public class HDataLibAdapter implements SDataLib {
    private final IHDataLib dataLib;

    public HDataLibAdapter(IHDataLib dataLib) {
        this.dataLib = dataLib;
    }

    @Override
    public Object newResult(Object key, List keyValues) {
        return dataLib.newResult((byte[]) key, keyValues);
    }

    @Override
    public Object newRowKey(Object... args) {
        return dataLib.newRowKey(args);
    }

    @Override
    public Object getResultKey(Object result) {
        return dataLib.getResultKey((Result) result);
    }

    @Override
    public Object getPutKey(Object put) {
        return dataLib.getPutKey((Put) put);
    }

    @Override
    public List getResultColumn(Object result, Object family, Object qualifier) {
        return dataLib.getResultColumn((Result) result, (byte[]) family, (byte[]) qualifier);
    }

    @Override
    public Object getResultValue(Object result, Object family, Object qualifier) {
        return dataLib.getResultValue((Result) result, (byte[]) family, (byte[]) qualifier);
    }

    @Override
    public List listResult(Object result) {
        return dataLib.listResult((Result) result);
    }

    @Override
    public List listPut(Object put) {
        return dataLib.listPut((Put) put);
    }

    @Override
    public Object getKeyValueRow(Object keyValue) {
        return dataLib.getKeyValueRow((KeyValue) keyValue);
    }

    @Override
    public Object getKeyValueFamily(Object keyValue) {
        return dataLib.getKeyValueFamily((KeyValue) keyValue);
    }

    @Override
    public Object getKeyValueQualifier(Object keyValue) {
        return dataLib.getKeyValueQualifier((KeyValue) keyValue);
    }

    @Override
    public Object getKeyValueValue(Object keyValue) {
        return dataLib.getKeyValueValue((KeyValue) keyValue);
    }

    @Override
    public long getKeyValueTimestamp(Object keyValue) {
        return dataLib.getKeyValueTimestamp((KeyValue) keyValue);
    }

    @Override
    public Object encode(Object value) {
        return dataLib.encode(value);
    }

    @Override
    public Object decode(Object value, Class type) {
        return dataLib.decode((byte[]) value, type);
    }

    @Override
    public boolean valuesEqual(Object value1, Object value2) {
        return dataLib.valuesEqual((byte[]) value1, (byte[]) value2);
    }

    @Override
    public void addKeyValueToPut(Object put, Object family, Object qualifier, Long timestamp, Object value) {
        dataLib.addKeyValueToPut((Put) put, (byte[]) family, (byte[]) qualifier, timestamp, (byte[]) value);
    }

    @Override
    public void addAttribute(Object operation, String attributeName, Object value) {
        dataLib.addAttribute((OperationWithAttributes) operation, attributeName, (byte[]) value);
    }

    @Override
    public Object getAttribute(Object operation, String attributeName) {
        return dataLib.getAttribute((OperationWithAttributes) operation, attributeName);
    }

    @Override
    public Object newPut(Object key) {
        return dataLib.newPut((byte[]) key);
    }

    @Override
    public Object newPut(Object key, SRowLock lock) {
        return dataLib.newPut((byte[]) key, ((HRowLock) lock).lock);
    }

    @Override
    public SGet newGet(Object rowKey, List families, List columns, Long effectiveTimestamp) {
        return new HGet(dataLib.newGet((byte[]) rowKey, families, columns, effectiveTimestamp));
    }

    @Override
    public SScan newScan(Object startRowKey, Object endRowKey, List families, List columns, Long effectiveTimestamp) {
        return new HScan(dataLib.newScan((byte[]) startRowKey, (byte[]) endRowKey, families, columns, effectiveTimestamp));
    }

    public static byte[] convertToBytes(Object value) {
        return HDataLib.convertToBytes(value);
    }
}
