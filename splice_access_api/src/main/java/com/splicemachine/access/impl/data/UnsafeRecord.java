package com.splicemachine.access.impl.data;

import com.splicemachine.storage.Record;
import com.splicemachine.storage.RecordType;

import java.util.Iterator;

/**
 * Created by jleach on 1/3/17.
 */
public class UnsafeRecord implements Record<byte[],Object> {
    byte[] key;
    long version;
    Object baseObject;
    long baseOffset;

    public UnsafeRecord(byte[] key, long version, Object baseObject,long baseOffset) {
        assert key != null:"key cannot be null";
        assert version > 0L:"version cannot be negative";
        this.key = key;
        this.version = version;
        this.baseObject = baseObject;
        this.baseOffset = baseOffset;
    }

    @Override
    public long getTxnId1() {
        return 0;
    }

    @Override
    public void setTxnId1(long transactionId1) {

    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public boolean hasTombstone() {
        return false;
    }

    @Override
    public void setHasTombstone(boolean hasTombstone) {

    }

    @Override
    public long getTxnId2() {
        return 0;
    }

    @Override
    public void setTxnId2(long transactionId2) {

    }

    @Override
    public long getEffectiveTimestamp() {
        return 0;
    }

    @Override
    public void setEffectiveTimestamp(long effectiveTimestamp) {

    }

    @Override
    public int numberOfColumns() {
        return 0;
    }

    @Override
    public void setNumberOfColumns(int numberOfColumns) {

    }

    @Override
    public Object getData() {
        return null;
    }

    @Override
    public void setData(Object data) {

    }

    @Override
    public byte[] getKey() {
        return null;
    }

    @Override
    public void setKey(byte[] key) {
        this.key = key;
    }

    @Override
    public RecordType getRecordType() {
        return null;
    }

    @Override
    public void setRecordType(RecordType recordType) {

    }

    @Override
    public void setResolved() {

    }

    @Override
    public boolean isResolved() {
        return false;
    }

    @Override
    public void setActive() {

    }

    @Override
    public boolean isActive() {
        return false;
    }

    @Override
    public Record applyRollback(Iterator iterator) {
        return null;
    }

    @Override
    public Record[] updateRecord(Record updatedRecord) {
        return new Record[0];
    }
}
