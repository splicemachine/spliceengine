package com.splicemachine.si.data.light;

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

    @Override
    public String toString() {
        return "<" + key + " " + values + ">";
    }
}
