package com.splicemachine.si.data.light;

import com.splicemachine.si.data.api.SGet;
import com.splicemachine.si.data.api.SScan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LGet implements SGet, SScan {
    final Object startTupleKey;
    final Object endTupleKey;
    final java.util.List families;
    final List<List<Object>> columns;
    Long effectiveTimestamp;
    final Map<String, Object> attributes;

    public LGet(Object startTupleKey, Object endTupleKey, List families, List<List<Object>> columns,
                Long effectiveTimestamp) {
        this.startTupleKey = startTupleKey;
        this.endTupleKey = endTupleKey;
        this.families = families;
        this.columns = columns;
        this.effectiveTimestamp = effectiveTimestamp;
        this.attributes = new HashMap<String, Object>();
    }
}
