package com.splicemachine.si.impl;

import org.apache.hadoop.hbase.KeyValue;

/**
 * Lazily read individual elements out of a KV object and cache them for subsequent calls.
 */
public class DecodedKeyValue {
    private KeyValue keyValue;
    private byte[] row;
    private byte[] value;
    private long timestamp;

    public DecodedKeyValue() {

    }

    public void setKeyValue(KeyValue keyValue) {
        this.row = null;
        this.value = null;
        this.timestamp = -1;
        this.keyValue = keyValue;
    }

    public KeyValue keyValue() {
        return keyValue;
    }

    public byte[] row() {
        if (row == null) {
            row = keyValue.getRow();
        }
        return row;
    }
    public byte[] value() {
        if (value == null) {
            value = keyValue.getValue();
        }
        return value;
    }

    public long timestamp() {
        if (timestamp == -1) {
            timestamp = keyValue.getTimestamp();
        }
        return timestamp;
    }

	@Override
	public String toString() {
		return String.format("DecodedKeyValue { keyValue=%s}",keyValue);
	}
    
}
