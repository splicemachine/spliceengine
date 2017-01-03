package com.splicemachine.si.impl.data;

import com.splicemachine.storage.Record;
import com.splicemachine.storage.RecordType;

import java.util.Iterator;

/**
 * Created by jleach on 1/3/17.
 */
public class UnsafeRecord implements Record {
    byte[] key;
    byte[] version;
    Object baseObject;
    long baseOffset;

    @Override
    public long getTxnId1() {
        return 0;
    }

    @Override
    public void setTxnId1(long transactionId1) {

    }

    @Override
    public long getVersion() {
        return 0;
    }

    @Override
    public void setVersion(long version) {

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
    public Object getKey() {
        return null;
    }

    @Override
    public void setKey(Object key) {

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
