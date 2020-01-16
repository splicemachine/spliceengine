package com.splicemachine.si.impl.server;

import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

public class SITestUtils {
    private static byte[] row = Bytes.toBytes("commit");

    private static KeyValue generateKV(byte[] row, byte[] qualifier, long timestamp, byte[] value) {
        return new KeyValue(row, SIConstants.DEFAULT_FAMILY_BYTES, qualifier, timestamp, value);
    }

    public static KeyValue getMockCommitCell(long timestamp) {
        return generateKV(row, SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES, timestamp, Bytes.toBytes(""));
    }

    public static KeyValue getMockTombstoneCell(long timestamp) {
        return generateKV(row, SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES, timestamp, Bytes.toBytes(""));
    }

    public static KeyValue getMockAntiTombstoneCell(long timestamp) {
        return generateKV(row, SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES, timestamp, SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES);
    }

    public static KeyValue getMockValueCell(long timestamp) {
        return generateKV(row, SIConstants.PACKED_COLUMN_BYTES, timestamp, Bytes.toBytes("value"));
    }
}
