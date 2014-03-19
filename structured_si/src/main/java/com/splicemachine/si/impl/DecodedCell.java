package com.splicemachine.si.impl;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

/**
 * Lazily read individual elements out of a Cell object and cache them for subsequent calls.
 */
public class DecodedCell {
    private Cell keyValue;
    private byte[] row;
    private byte[] value;
    private long timestamp;

    public DecodedCell() {

    }

    public void setKeyValue(Cell keyValue) {
        this.row = null;
        this.value = null;
        this.timestamp = -1;
        this.keyValue = keyValue;
    }

    public Cell keyValue() {
        return keyValue;
    }

    public byte[] row() {
        if (row == null) {
            row = CellUtil.cloneValue(keyValue);
        }
        return row;
    }
    public byte[] value() {
        if (value == null) {
            value = CellUtil.cloneValue(keyValue);
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
