package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;

/**
 * Lazily read individual elements out of a KeyValue object and cache them for subsequent calls.
 */
public class DecodedKeyValue<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Lock, OperationStatus> {
    private final SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus> dataLib;
    private KeyValue keyValue;
    private Data row;
    private Data value;
    private long timestamp;

    public DecodedKeyValue(SDataLib dataLib) {
        this.dataLib = dataLib;
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

    public Data row() {
        if (row == null) {
            row = dataLib.getKeyValueRow(keyValue);
        }
        return row;
    }
    public Data value() {
        if (value == null) {
            value = dataLib.getKeyValueValue(keyValue);
        }
        return value;
    }

    public long timestamp() {
        if (timestamp == -1) {
            timestamp = dataLib.getKeyValueTimestamp(keyValue);
        }
        return timestamp;
    }
}
