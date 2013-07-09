package com.splicemachine.si.data.light;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LTuple {
    final String key;
    final List<LKeyValue> values;
    final Map<String, Object> attributes;
    final LRowLock lock;

    public LTuple(String key, List<LKeyValue> values) {
        this.key = key;
        this.values = values;
        this.attributes = new HashMap<String, Object>();
        this.lock = null;
    }

    public LTuple(String key, List<LKeyValue> values, Map<String, Object> attributes) {
        this.key = key;
        this.values = values;
        this.attributes = attributes;
        this.lock = null;
    }

    public LTuple(String key, List<LKeyValue> values, LRowLock lock) {
        this.key = key;
        this.values = values;
        this.attributes = new HashMap<String, Object>();
        this.lock = lock;
    }

    public LTuple pack(String familyToPack, String packedQualifier) {
        List<LKeyValue> newValues = new ArrayList<LKeyValue>();
        String newRowKey = null;
        Long newTimestamp = null;
        boolean nothingPacked = true;
        Map<String, Object> packedRow = new HashMap<String, Object>();
        for (LKeyValue keyValue : values) {
            if (keyValue.family.equals(familyToPack)) {
                packedRow.put(keyValue.qualifier, keyValue.value);
                if (nothingPacked) {
                    newRowKey = keyValue.rowKey;
                } else {
                    if (newRowKey != keyValue.rowKey) {
                        throw new RuntimeException("rowkey mis-match");
                    }
                }
                if (nothingPacked) {
                    newTimestamp = keyValue.timestamp;
                } else {
                    if (newTimestamp != keyValue.timestamp) {
                        throw new RuntimeException("timestamp mis-match");
                    }
                }
                nothingPacked = false;
            } else {
                newValues.add(keyValue);
            }
        }
        if (!nothingPacked) {
            newValues.add(new LKeyValue(newRowKey, familyToPack, packedQualifier, newTimestamp, packedRow));
        }
        return new LTuple(key, newValues, new HashMap<String, Object>(attributes));
    }

    public LTuple unpack(String familyToUnpack) {
        List<LKeyValue> newValues = new ArrayList<LKeyValue>();
        for (LKeyValue keyValue : values) {
            if (keyValue.family.equals(familyToUnpack)) {
                Map<String, Object> packedRow = (Map<String, Object>) keyValue.value;
                for (String k : packedRow.keySet()) {
                    final LKeyValue newKeyValue = new LKeyValue(keyValue.rowKey, keyValue.family, k, keyValue.timestamp, packedRow.get(k));
                    newValues.add(newKeyValue);
                }
            } else {
                newValues.add(keyValue);
            }
        }
        return new LTuple(key, newValues, new HashMap<String, Object>(attributes));
    }

    @Override
    public String toString() {
        return "<" + key + " " + values + ">";
    }

}
