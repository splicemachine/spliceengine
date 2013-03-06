package com.splicemachine.si2.data.light;

import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.SScan;

import java.util.List;

public class LGet implements SGet, SScan {
    final Object startTupleKey;
    final Object endTupleKey;
    final java.util.List families;
    final List<List> columns;
    final Long effectiveTimestamp;

    public LGet(Object startTupleKey, Object endTupleKey, List families, List<List> columns,
                Long effectiveTimestamp) {
        this.startTupleKey = startTupleKey;
        this.endTupleKey = endTupleKey;
        this.families = families;
        this.columns = columns;
        this.effectiveTimestamp = effectiveTimestamp;
    }
}
