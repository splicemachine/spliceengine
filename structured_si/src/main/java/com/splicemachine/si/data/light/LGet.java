package com.splicemachine.si.data.light;

import org.apache.hadoop.hbase.client.OperationWithAttributes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LGet extends OperationWithAttributes{
    final byte[] startTupleKey;
    final byte[] endTupleKey;
    final List families;
    final List<List<byte[]>> columns;
    Long effectiveTimestamp;

    public LGet(byte[] startTupleKey, byte[] endTupleKey, List families, List<List<byte[]>> columns,
                Long effectiveTimestamp) {
        this.startTupleKey = startTupleKey;
        this.endTupleKey = endTupleKey;
        this.families = families;
        this.columns = columns;
        this.effectiveTimestamp = effectiveTimestamp;
    }

		@Override public Map<String, Object> getFingerprint() { throw new UnsupportedOperationException(); }
		@Override public Map<String, Object> toMap(int maxCols) { throw new UnsupportedOperationException(); }
}
