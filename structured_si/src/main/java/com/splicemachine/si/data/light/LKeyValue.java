package com.splicemachine.si.data.light;

public class LKeyValue {
    final String rowKey;
    final String family;
    final String qualifier;
    final Object value;
    final Long timestamp;

    public LKeyValue(String rowKey, String family, String qualifier, Long timestamp, Object value) {
        this.rowKey = rowKey;
        this.family = family;
        this.qualifier = qualifier;
        this.value = value;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return family + "." + qualifier + "@" + timestamp + "=" + value;
    }
}
