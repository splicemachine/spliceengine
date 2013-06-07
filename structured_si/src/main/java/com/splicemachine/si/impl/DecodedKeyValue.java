package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;

public class DecodedKeyValue {
    private final SDataLib dataLib;

    private Object keyValue;

    private Object row;
    private Object family;
    private Object qualifier;
    private Object value;
    private Long timestamp;

    public DecodedKeyValue(SDataLib dataLib) {
        this.dataLib = dataLib;
    }

    public void setKeyValue(Object keyValue) {
        if (keyValue.getClass().equals(DecodedKeyValue.class)) {
            throw new RuntimeException("fail");
        }
        this.row = null;
        this.family = null;
        this.qualifier = null;
        this.value = null;
        this.timestamp = null;
        this.keyValue = keyValue;
    }

    public Object keyValue() {
        return keyValue;
    }

    public Object row() {
        if (row == null) {
            row = dataLib.getKeyValueRow(keyValue);
        }
        return row;
    }

    public Object family() {
        if (family == null) {
            family = dataLib.getKeyValueFamily(keyValue);
        }
        return family;
    }

    public Object qualifier() {
        if (qualifier == null) {
            qualifier = dataLib.getKeyValueQualifier(keyValue);
        }
        return qualifier;
    }

    public Object value() {
        if (value == null) {
            value = dataLib.getKeyValueValue(keyValue);
        }
        return value;
    }

    public long timestamp() {
        if (timestamp == null) {
            timestamp = dataLib.getKeyValueTimestamp(keyValue);
        }
        return timestamp;
    }
}
