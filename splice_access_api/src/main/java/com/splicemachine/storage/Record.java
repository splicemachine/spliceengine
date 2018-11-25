package com.splicemachine.storage;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.utils.ByteSlice;

/**
 *
 * Record Implementation
 *
 */
public interface Record<K> {
    /**
     *
     * Txn ID 1
     *
     * @return`
     */
    long getTxnId1();

    /**
     *
     * Txn ID 1
     *
     * @return
     */
    void setTxnId1(long transactionId1);

    /**
     *
     * Version Number of Updates, required due to
     * read committed nature of select for update functionality.
     *
     * @return
     */
    long getVersion();

    /**
     *
     * Version Number of Updates, required due to
     * read committed nature of select for update functionality.
     *
     * @return
     */
    void setVersion(long version);

    /**
     *
     * Tombstone Marker For Deletes
     *
     */
    boolean hasTombstone();

    /**
     *
     * Tombstone Marker For Deletes
     *
     */
    void setHasTombstone(boolean hasTombstone);


    /**
     *
     * The effective commit timestamp of the data, -1 if rolledback
     *
     * @return
     */
    long getEffectiveTimestamp();

    /**
     *
     * The effective commit timestamp of the data, -1 if rolledback
     *
     * @return
     */
    void setEffectiveTimestamp(long effectiveTimestamp);


    /**
     *
     * Number of Columns
     *
     * @return
     */
    int numberOfColumns();

    /**
     *
     * Number of Columns
     *
     * @return
     */
    void setNumberOfColumns(int numberOfColumns);


    /**
     *
     * Get Actual Data
     *
     * @return
     */
    void getData(int[] columns, ExecRow row) throws StandardException;

    /**
     *
     * Get Actual Data
     *
     * @return
     */
    void getData(int[] columns, DataValueDescriptor[] rows) throws StandardException;

    /**
     *
     * Get Actual Data
     *
     * @return
     */
    void getData(FormatableBitSet accessedColumns, ExecRow row) throws StandardException;

    /**
     *
     * Get Actual Data
     *
     * @return
     */
    void getData(FormatableBitSet accessedColumns, DataValueDescriptor[] data) throws StandardException;

    /**
     *
     * Set Actual Data
     *
     * @return
     */
    void setData(int[] columns, ExecRow row) throws StandardException;

    /**
     *
     * Set Actual Data
     *
     * @return
     */
    void setData(int[] columns, DataValueDescriptor[] dvds) throws StandardException;

    /**
     *
     * Perform Updates....
     *
     *
     */
    void setData(int[] colPosMap, FormatableBitSet heapList, DataValueDescriptor[] dvds) throws StandardException;


    void mergeWithExistingData(int[] columns, DataValueDescriptor[] rows, int[] indexToMainColumnMap) throws StandardException;

    void mergeWithExistingData(BitSet accessedColumns, DataValueDescriptor[] rows, int[] indexToMainColumnMap) throws StandardException;

    /**
     *
     * Set Actual Data
     *
     * @return
     */
    void setData(DataValueDescriptor[] dvds) throws StandardException;

    void setData(FormatableBitSet formatableBitSet, ExecRow data) throws StandardException;

    void getData(BitSet accessedColumns, DataValueDescriptor[] dvds) throws StandardException;

    void getData(int[] columns, DataValueDescriptor[] rows, int[] mainColToIndexPosMap) throws StandardException;

    void getData(BitSet accessedColumns, DataValueDescriptor[] rows, int[] mainColToIndexPosMap) throws StandardException;

    void setData(FormatableBitSet formatableBitSet, DataValueDescriptor[] dvds) throws StandardException;
    /**
     *
     * Set Actual Data
     *
     * @return
     */
    byte[] getKey();

    /**
     *
     * Set Actual Data
     *
     * @return
     */
    byte[] getValue();

    /**
     *
     * Set Actual Data
     *
     * @return
     */
    void setKey(K key);

    public void setKey(byte[] key, long keyOffset, int length);

    RecordType getRecordType();

    void setRecordType(RecordType recordType);

    void setResolved(boolean resolved);

    boolean isResolved();

    void setActive(boolean active);

    boolean isActive();
                                                   /* msirek-temp->
    Record applyRedo(Iterator<Record<K>> recordIterator, ExecRow rowDefinition) throws StandardException;

    Record applyRedo(Iterator<Record<K>> recordIterator, FormatableBitSet variableLength) throws StandardException;
                                     <- msirek-temp */
    /**
     * Position 0 active Record, Position 1 redo record
     *
     * @param updatedRecord
     * @return
     */
                                                   /* msirek-temp->
    Record applyRedo(Record<K> updatedRecord, FormatableBitSet variableLength, FormatableBitSet accessedColumns) throws StandardException;

    Record applyRedo(Record<K> updatedRecord, ExecRow recordDefinition) throws StandardException;

    Record applyRedo(Record<K> updatedRecord, FormatableBitSet variableLength) throws StandardException;
                                     <- msirek-temp */

    /**
     * Position 0 active Record, Position 1 redo record
     *
     * @param updatedRecord
     * @return
     */
                                                   /* msirek-temp->
    Record[] updateRecord(Record<K> updatedRecord, ExecRow recordDefinition) throws StandardException;

    Record[] updateRecord(Record<K> updatedRecord, FormatableBitSet variableLength) throws StandardException;
                                     <- msirek-temp */
    Record createIndexDelete(int[] mainColToIndexPosMap, boolean uniqueWithDuplicateNulls, boolean[] descending, ExecRow indexRow) throws StandardException;

    Record createIndexInsert(int[] mainColToIndexPosMap, boolean uniqueWithDuplicateNulls, boolean[] descending, ExecRow indexRow) throws StandardException;

    int getSize();

    ByteSlice rowKeySlice();

    Record cancelToDelete();

    public KVPair getKVPair();

}