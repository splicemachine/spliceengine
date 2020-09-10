package com.splicemachine.si.impl.server;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class SITestUtils {
    private static final KryoPool kp = new KryoPool(1);

    private static byte[] row = Bytes.toBytes("commit");

    private static KeyValue generateKV(byte[] row, byte[] qualifier, long timestamp, byte[] value) {
        return new KeyValue(row, SIConstants.DEFAULT_FAMILY_BYTES, qualifier, timestamp, value);
    }

    public static KeyValue getMockCommitCell(long timestamp, long commitTimestamp) {
        return generateKV(row, SIConstants.COMMIT_TIMESTAMP_COLUMN_BYTES, timestamp, Bytes.toBytes(commitTimestamp));
    }

    public static KeyValue getMockTombstoneCell(long timestamp) {
        return generateKV(row, SIConstants.TOMBSTONE_COLUMN_BYTES, timestamp, SIConstants.EMPTY_BYTE_ARRAY);
    }

    public static KeyValue getMockAntiTombstoneCell(long timestamp) {
        return generateKV(row, SIConstants.TOMBSTONE_COLUMN_BYTES, timestamp, SIConstants.ANTI_TOMBSTONE_VALUE_BYTES);
    }

    public static KeyValue getMockValueCell(long timestamp) throws IOException {
        return getMockValueCell(timestamp, new boolean[]{true, true, true});
    }

    public static KeyValue getMockValueCell(long timestamp, boolean[] setColumns) throws IOException {
        return getMockValueCell(timestamp, getMockPackedBytes(setColumns));
    }

    public static KeyValue getMockValueCell(long timestamp, byte[] value) {
        return generateKV(row, SIConstants.PACKED_COLUMN_BYTES, timestamp, value);
    }

    public static byte[] getMockPackedBytes(boolean[] setColumns) throws IOException {
        BitSet setCols = new BitSet(setColumns.length);
        BitSet scalarCols = new BitSet(setColumns.length);
        BitSet empty = new BitSet();
        for (int i = 0; i < setColumns.length; ++i) {
            if (setColumns[i]) {
                setCols.set(i);
                scalarCols.set(i);
            }
        }

        EntryEncoder ee = EntryEncoder.create(kp,3, setCols, scalarCols, empty, empty);
        MultiFieldEncoder entryEncoder=ee.getEntryEncoder();
        for (int i = 0; i < setColumns.length; ++i) {
            if (setColumns[i]) {
                entryEncoder.encodeNext(i);
            }
        }

        return ee.encode();
    }

    public static KeyValue getMockFirstWriteCell(long timestamp) {
        return generateKV(row, SIConstants.FIRST_OCCURRENCE_TOKEN_COLUMN_BYTES, timestamp, SIConstants.EMPTY_BYTE_ARRAY);
    }

    public static KeyValue getMockDeleteRightAfterFirstWriteCell(long timestamp) {
        return generateKV(row, SIConstants.FIRST_OCCURRENCE_TOKEN_COLUMN_BYTES, timestamp, SIConstants.DELETE_RIGHT_AFTER_FIRST_WRITE_VALUE_BYTES);
    }

    public static long getSize(List<Cell> keyValueList) {
        return keyValueList.stream().mapToLong(KeyValueUtil::length).sum();
    }
}
