package com.splicemachine.io.hbase.encoding;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SettableSequenceId;
import org.apache.hadoop.hbase.io.HeapSize;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * PAX Active Cell implementation...
 *
 */
public class PAXActiveCell implements Cell, SettableSequenceId, HeapSize  {
    private byte[] extendedRowKey;
    private int seekerPosition;
    private byte[] value;
    private LazyColumnarSeeker lazyColumnarSeeker;
    private long timestamp;

    PAXActiveCell(int seekerPosition, byte[] extendedRowKey, LazyColumnarSeeker lazyColumnarSeeker, long timestamp) {
        this.extendedRowKey = extendedRowKey;
        this.seekerPosition = seekerPosition;
        this.lazyColumnarSeeker = lazyColumnarSeeker;
        this.timestamp = timestamp;
    }

    @Override
    public byte[] getRowArray() {
        return extendedRowKey;
    }

    @Override
    public int getRowOffset() {
        return 0;
    }

    @Override
    public short getRowLength() {
        return (short) (extendedRowKey.length-1);
    }

    @Override
    public byte[] getFamilyArray() {
        return SIConstants.DEFAULT_FAMILY_ACTIVE_BYTES;
    }

    @Override
    public int getFamilyOffset() {
        return 0;
    }

    @Override
    public byte getFamilyLength() {return 1;}

    @Override
    public byte[] getQualifierArray() {
        return SIConstants.PACKED_COLUMN_BYTES;
    }

    @Override
    public int getQualifierOffset() {
        return 0;
    }

    @Override
    public int getQualifierLength() {
        return 1;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public byte getTypeByte() {
        return KeyValue.Type.Put.getCode();
    }

    @Override
    public long getMvccVersion() {
        return 0;
    }

    @Override
    public long getSequenceId() {
        try {
            if (value == null)
                getValue();
            return lazyColumnarSeeker.writtenExecRow.getColumn(3).getLong();
        } catch (StandardException se) {
            throw new RuntimeException(se);
        }
    }

    @Override
    public byte[] getValueArray() {
        if (value == null)
            getValue();
        return value;
    }

    @Override
    public int getValueOffset() {
        return 0;
    }

    @Override
    public int getValueLength() {
        if (value== null)
            return 100; // Hack for gets
        return value.length;
    }

    @Override
    public byte[] getTagsArray() {
        return SIConstants.EMPTY_BYTE_ARRAY;
    }

    @Override
    public int getTagsOffset() {
        return 0;
    }

    @Override
    public int getTagsLength() {
        return 0;
    }

    @Override
    public byte[] getValue() {
        if (value != null)
            return value;
        try {
            value = lazyColumnarSeeker.lazyFetchPosition(seekerPosition, extendedRowKey, 0, extendedRowKey.length);
            return value;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] getFamily() {
        return SIConstants.DEFAULT_FAMILY_ACTIVE_BYTES;
    }

    @Override
    public byte[] getQualifier() {
        return SIConstants.PACKED_COLUMN_BYTES;
    }

    @Override
    public byte[] getRow() {
        return ByteBuffer.wrap(extendedRowKey,0,extendedRowKey.length - 1).array();
    }

    @Override
    public long heapSize() {
        return 100; // ?
    }

    @Override
    public void setSequenceId(long seqId) throws IOException {

    }
}
