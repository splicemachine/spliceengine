package com.splicemachine.si.data.hbase;

import com.splicemachine.constants.bytes.BytesUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class HDataLib implements IHDataLib {

    @Override
    public byte[] newRowKey(Object... args) {
        List<byte[]> bytes = new ArrayList<byte[]>();
        for (Object a : args) {
            bytes.add(HDataLib.convertToBytes(a));
        }
        return BytesUtil.concat(bytes);
    }

    @Override
    public byte[] getResultKey(Result result) {
        return result.getRow();
    }

    @Override
    public byte[] getPutKey(Put put) {
        return put.getRow();
    }

    @Override
    public List getResultColumn(Result result, byte[] family, byte[] qualifier) {
        return result.getColumn(family, qualifier);
    }

    @Override
    public byte[] getResultValue(Result result, byte[] family, byte[] qualifier) {
        return result.getValue(family, qualifier);
    }

    @Override
    public Map getResultFamilyMap(Result result, byte[] family) {
        return result.getFamilyMap(family);
    }

    @Override
    public List listResult(Result result) {
        return result.list();
    }

    @Override
    public List listPut(Put put) {
        final Map<byte[], List<KeyValue>> familyMap = put.getFamilyMap();
        List result = new ArrayList();
        for (List<KeyValue> subList : familyMap.values()) {
            result.addAll(subList);
        }
        return result;
    }

    @Override
    public KeyValue newKeyValue(byte[] rowKey, byte[] family, byte[] qualifier, Long timestamp, byte[] value) {
        return new KeyValue(rowKey, family, qualifier, timestamp, value);
    }

    @Override
    public byte[] getKeyValueRow(KeyValue keyValue) {
        return keyValue.getRow();
    }

    @Override
    public byte[] getKeyValueFamily(KeyValue keyValue) {
        return keyValue.getFamily();
    }

    @Override
    public byte[] getKeyValueQualifier(KeyValue keyValue) {
        return keyValue.getQualifier();
    }

    @Override
    public byte[] getKeyValueValue(KeyValue keyValue) {
        return keyValue.getValue();
    }

    @Override
    public long getKeyValueTimestamp(KeyValue keyValue) {
        return keyValue.getTimestamp();
    }

    @Override
    public byte[] encode(Object value) {
        return convertToBytes(value);
    }

    @Override
    public Object decode(byte[] value, Class type) {
        if (value == null) {
            return null;
        }
        final byte[] bytes = value;
        if (type.equals(Boolean.class)) {
            return Bytes.toBoolean(bytes);
        } else if (type.equals(Short.class)) {
            return Bytes.toShort(bytes);
        } else if (type.equals(Integer.class)) {
            return Bytes.toInt(bytes);
        } else if (type.equals(Long.class)) {
            return Bytes.toLong(bytes);
        } else if (type.equals(String.class)) {
            return Bytes.toString(bytes);
        }
        throw new RuntimeException("unsupported type conversion: " + type.getName());
    }

    @Override
    public boolean valuesEqual(byte[] value1, byte[] value2) {
        return Arrays.equals(value1, value2);
    }

    @Override
    public void addKeyValueToPut(Put put, byte[] family, byte[] qualifier, Long timestamp, byte[] value) {
        if (timestamp == null) {
            put.add(family, qualifier, value);
        } else {
            put.add(family, qualifier, timestamp, value);
        }
    }

    @Override
    public void addAttribute(OperationWithAttributes operation, String attributeName, byte[] value) {
        operation.setAttribute(attributeName, value);
    }

    @Override
    public byte[] getAttribute(OperationWithAttributes operation, String attributeName) {
        return operation.getAttribute(attributeName);
    }

    @Override
    public Result newResult(byte[] key, List keyValues) {
        return new Result(keyValues);
    }

    @Override
    public Put newPut(byte[] key) {
        return new Put(key);
    }

    @Override
    public Put newPut(byte[] key, RowLock lock) {
        return new Put(key, lock);
    }

    @Override
    public Get newGet(byte[] rowKey, List families, List columns, Long effectiveTimestamp) {
        Get get = new Get(rowKey);
        if (families != null) {
            for (Object f : families) {
                get.addFamily((byte[]) f);
            }
        }
        if (columns != null) {
            for (List c : (List<List>) columns) {
                get.addColumn((byte[]) c.get(0), (byte[]) c.get(1));
            }
        }
        if (effectiveTimestamp != null) {
            try {
                get.setTimeRange(effectiveTimestamp, Long.MAX_VALUE);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return get;
    }

    @Override
    public void setReadTimeRange(Get get, long minTimestamp, long maxTimestamp) {
        try {
            get.setTimeRange(minTimestamp, maxTimestamp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Scan newScan(byte[] startRowKey, byte[] endRowKey, List families, List columns, Long effectiveTimestamp) {
        Scan scan = new Scan();
        scan.setStartRow(startRowKey);
        scan.setStopRow(endRowKey);
        if (families != null) {
            for (Object f : families) {
                scan.addFamily((byte[]) f);
            }
        }
        if (columns != null) {
            for (List c : (List<List>) columns) {
                scan.addColumn((byte[]) c.get(0), (byte[]) c.get(1));
            }
        }
        if (effectiveTimestamp != null) {
            try {
                scan.setTimeRange(effectiveTimestamp, Long.MAX_VALUE);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return scan;
    }

    @Override
    public void setReadTimeRange(Scan scan, long minTimestamp, long maxTimestamp) {
        try {
            scan.setTimeRange(minTimestamp, maxTimestamp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setReadMaxVersions(Scan scan) {
        scan.setMaxVersions();
    }

    @Override
    public void setReadMaxVersions(Get get, int max) {
        try {
            get.setMaxVersions(max);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setReadMaxVersions(Get get) {
        get.setMaxVersions();
    }

    @Override
    public void setReadMaxVersions(Scan scan, int max) {
        scan.setMaxVersions(max);
    }

    @Override
    public void addFamilyToRead(Get get, byte[] family) {
        get.addFamily(family);
    }

    @Override
    public void addFamilyToRead(Scan scan, byte[] family) {
        scan.addFamily(family);
    }

    @Override
    public void addFamilyToReadIfNeeded(Get get, byte[] family) {
        if (get.hasFamilies()) {
            get.addFamily(family);
        }
    }

    @Override
    public void addFamilyToReadIfNeeded(Scan scan, byte[] family) {
        if(scan.hasFamilies()) {
            scan.addFamily(family);
        }
    }

    static byte[] convertToBytes(Object value) {
        if (value instanceof String) {
            return Bytes.toBytes((String) value);
        } else if (value instanceof Integer) {
            return Bytes.toBytes((Integer) value);
        } else if (value instanceof Short) {
            return Bytes.toBytes((Short) value);
        } else if (value instanceof Long) {
            return Bytes.toBytes((Long) value);
        } else if (value instanceof Boolean) {
            return Bytes.toBytes((Boolean) value);
        } else if (value instanceof byte[]) {
            return (byte[]) value;
        }
        throw new RuntimeException("Unsupported class " + value.getClass().getName() + " for " + value);
    }

    @Override
    public Delete newDelete(byte[] rowKey) {
        return new Delete(rowKey);
    }

    @Override
    public void addKeyValueToDelete(Delete delete, byte[] family, byte[] qualifier, long timestamp) {
        delete.deleteColumn(family, qualifier, timestamp);
    }
}
