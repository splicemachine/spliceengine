package com.splicemachine.si.data.hbase;

import com.google.common.collect.Iterables;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.hbase.ByteBufferArrayUtils;
import com.splicemachine.si.data.api.SDataLib;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.util.ArrayUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Implementation of SDataLib that is specific to the HBase operation and result types.
 */
public class HDataLib implements SDataLib<byte[], Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Integer, OperationStatus> {

    @Override
    public Result newResult(byte[] key, List<KeyValue> keyValues) {
        return new Result(keyValues);
    }

    @Override
    public byte[] newRowKey(Object... args) {
        List<byte[]> bytes = new ArrayList<byte[]>();
        for (Object a : args) {
            bytes.add(convertToBytes(a, a.getClass()));
        }
        return BytesUtil.concat(bytes);
    }

    @Override
    public byte[] increment(byte[] key) {
        if (key.length == 0) {
            return key;
        }
        return BytesUtil.unsignedCopyAndIncrement(key);
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
    public List<KeyValue> getResultColumn(Result result, byte[] family, byte[] qualifier) {
        return result.getColumn(family, qualifier);
    }

    @Override
    public byte[] getResultValue(Result result, byte[] family, byte[] qualifier) {
        return result.getValue(family, qualifier);
    }

    @Override
    public Map<byte[], byte[]> getResultFamilyMap(Result result, byte[] family) {
        return result.getFamilyMap(family);
    }

    @Override
    public List<KeyValue> listResult(Result result) {
        return result.list();
    }

    @Override
    public Iterable<KeyValue> listPut(Put put) {
        return Iterables.concat(put.getFamilyMap().values());
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
        return convertToBytes(value, value.getClass());
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
        } else if (type.equals(Byte.class)) {
            if (value.length > 0) {
                return value[0];
            } else {
                return null;
            }
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
    public Put newPut(byte[] key) {
        return new Put(key);
    }

    @Override
    public Put newPut(byte[] key, Integer lock) {
        return new Put(key, lock);
    }

    @Override
    public OperationStatus newFailStatus() {
        return new OperationStatus(HConstants.OperationStatusCode.FAILURE);
    }

    @Override
    public Get newGet(byte[] rowKey, List<byte[]> families, List<List<byte[]>> columns, Long effectiveTimestamp) {
        Get get = new Get(rowKey);
        if (families != null) {
            for (byte[] f : families) {
                get.addFamily(f);
            }
        }
        if (columns != null) {
            for (List<byte[]> c : columns) {
                get.addColumn(c.get(0), c.get(1));
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
    public byte[] getGetRow(Get get) {
        return get.getRow();
    }

    @Override
    public void setGetTimeRange(Get get, long minTimestamp, long maxTimestamp) {
        try {
            get.setTimeRange(minTimestamp, maxTimestamp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setGetMaxVersions(Get get) {
        get.setMaxVersions();
    }

    @Override
    public void setGetMaxVersions(Get get, int max) {
        try {
            get.setMaxVersions(max);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addFamilyToGet(Get get, byte[] family) {
        get.addFamily(family);
    }

    @Override
    public void addFamilyToGetIfNeeded(Get get, byte[] family) {
        if (get.hasFamilies()) {
            get.addFamily(family);
        }
    }

    @Override
    public Scan newScan(byte[] startRowKey, byte[] endRowKey, List<byte[]> families, List<List<byte[]>> columns, Long effectiveTimestamp) {
        Scan scan = new Scan();
        scan.setStartRow(startRowKey);
        scan.setStopRow(endRowKey);
        if (families != null) {
            for (byte[] f : families) {
                scan.addFamily(f);
            }
        }
        if (columns != null) {
            for (List<byte[]> c : columns) {
                scan.addColumn(c.get(0), c.get(1));
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
    public void setScanTimeRange(Scan scan, long minTimestamp, long maxTimestamp) {
        try {
            scan.setTimeRange(minTimestamp, maxTimestamp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setScanMaxVersions(Scan scan) {
        scan.setMaxVersions();
    }

    @Override
    public void setScanMaxVersions(Scan scan, int max) {
        scan.setMaxVersions(max);
    }

    @Override
    public void addFamilyToScan(Scan scan, byte[] family) {
        scan.addFamily(family);
    }

    @Override
    public void addFamilyToScanIfNeeded(Scan scan, byte[] family) {
        if(scan.hasFamilies()) {
            scan.addFamily(family);
        }
    }

    @Override
    public Delete newDelete(byte[] rowKey) {
        return new Delete(rowKey);
    }

    @Override
    public void addKeyValueToDelete(Delete delete, byte[] family, byte[] qualifier, long timestamp) {
        delete.deleteColumn(family, qualifier, timestamp);
    }

    static byte[] convertToBytes(Object value, Class clazz) {
        if (clazz == String.class) {
            return Bytes.toBytes((String) value);
        } else if (clazz == Integer.class) {
            return Bytes.toBytes((Integer) value);
        } else if (clazz == Short.class) {
            return Bytes.toBytes((Short) value);
        } else if (clazz == Long.class) {
            return Bytes.toBytes((Long) value);
        } else if (clazz == Boolean.class) {
            return Bytes.toBytes((Boolean) value);
        } else if (clazz == byte[].class) {
            return (byte[]) value;
        } else if (clazz == Byte.class) {
            return new byte[] {(Byte) value};
        }
        throw new RuntimeException("Unsupported class " + clazz.getName() + " for " + value);
    }
    
    @Override
    public boolean matchingColumn(KeyValue keyValue, byte[] family, byte[] qualifier) {
    	return ByteBufferArrayUtils.matchingColumn(keyValue, family, qualifier);
    }

    @Override
    public boolean matchingFamily(KeyValue keyValue, byte[] family) {
    	return ByteBufferArrayUtils.matchingFamily(keyValue, family);
    }
    
    @Override
    public boolean matchingQualifier(KeyValue keyValue, byte[] qualifier) {
    	return ByteBufferArrayUtils.matchingQualifier(keyValue, qualifier);
    }

    @Override
    public boolean matchingValue(KeyValue keyValue, byte[] value) {
    	return ByteBufferArrayUtils.matchingValue(keyValue, value);
    }

	@Override
	public boolean matchingFamilyKeyValue(KeyValue keyValue, KeyValue other) {
		return ByteBufferArrayUtils.matchingFamilyKeyValue(keyValue, other);
	}
	@Override
	public boolean matchingQualifierKeyValue(KeyValue keyValue, KeyValue other) {
		return ByteBufferArrayUtils.matchingQualifierKeyValue(keyValue, other);
	}
	@Override
	public boolean matchingRowKeyValue(KeyValue keyValue, KeyValue other) {
		return ByteBufferArrayUtils.matchingRowKeyValue(keyValue, other);
	}

	@Override
    public boolean matchingValueKeyValue(KeyValue keyValue, KeyValue other) {
		return ByteBufferArrayUtils.matchingValueKeyValue(keyValue, other);
    }

	@Override
	public KeyValue newKeyValue(KeyValue keyValue, byte[] value) {		
			return new KeyValue(keyValue.getBuffer(),keyValue.getRowOffset(),keyValue.getRowLength(),keyValue.getBuffer(),keyValue.getFamilyOffset(),keyValue.getFamilyLength(),keyValue.getBuffer(),keyValue.getQualifierOffset(),keyValue.getQualifierLength(),keyValue.getTimestamp(),Type.Put,value,0,value==null ? 0 : value.length);		
	}
	
    @Override
    public KeyValue newKeyValue(byte[] rowKey, byte[] family, byte[] qualifier, Long timestamp, byte[] value) {
        return new KeyValue(rowKey, family, qualifier, timestamp, value);
    }

}
