package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;

public class DecodedKeyValue {
    final Object row;
    final Object family;
    final Object qualifier;
    final long timestamp;
    final Object value;

    public DecodedKeyValue(SDataLib dataLib, Object keyValue) {
        row = dataLib.getKeyValueRow(keyValue);
        family = dataLib.getKeyValueFamily(keyValue);
        qualifier = dataLib.getKeyValueQualifier(keyValue);
        timestamp = dataLib.getKeyValueTimestamp(keyValue);
        value = dataLib.getKeyValueValue(keyValue);
    }
}
