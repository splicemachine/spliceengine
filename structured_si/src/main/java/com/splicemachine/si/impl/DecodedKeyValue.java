package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;

public class DecodedKeyValue {
    public final Object keyValue;
    public final Object row;
    public final Object family;
    public final Object qualifier;
    public final long timestamp;
    public final Object value;

    public DecodedKeyValue(SDataLib dataLib, Object keyValue) {
        this.keyValue = keyValue;
        row = dataLib.getKeyValueRow(keyValue);
        family = dataLib.getKeyValueFamily(keyValue);
        qualifier = dataLib.getKeyValueQualifier(keyValue);
        timestamp = dataLib.getKeyValueTimestamp(keyValue);
        value = dataLib.getKeyValueValue(keyValue);
    }
}
