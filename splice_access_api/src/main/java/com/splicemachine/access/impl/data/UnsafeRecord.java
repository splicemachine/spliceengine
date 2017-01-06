package com.splicemachine.access.impl.data;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.storage.Record;
import com.splicemachine.storage.RecordType;
import org.apache.commons.codec.binary.Hex;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.unsafe.Platform;
import java.util.Iterator;

/**
 *
 * []
 *
 */
public class UnsafeRecord implements Record<byte[],ExecRow> {
    protected byte[] key;
    protected long version;
    protected Object baseObject;
    protected long baseOffset;
    protected boolean active;
    protected boolean resolved;
    protected boolean isActiveRecord;
    protected RecordType recordType;
    protected static int TOTAL_BYTES = 0;
    protected static int TOMB_INC = TOTAL_BYTES;//+8;
    protected static int TXN_ID1_INC = TOMB_INC+1;
    protected static int TXN_ID2_INC = TXN_ID1_INC+8;
    protected static int EFF_TS_INC = TXN_ID2_INC+8;
    protected static int NUM_COLS_INC = EFF_TS_INC+8;
    protected static int COLS_BS_INC = NUM_COLS_INC+4;
    protected static int UNSAFE_INC = COLS_BS_INC+4;




    public UnsafeRecord(byte[] key, long version, Object baseObject,long baseOffset, boolean isActiveRecord) {
        assert key != null:"key cannot be null";
        assert version > 0L:"version cannot be negative";
        this.key = key;
        this.version = version;
        this.baseObject = baseObject;
        this.baseOffset = baseOffset;
        this.isActiveRecord = isActiveRecord;
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
        return Platform.getBoolean(baseObject,baseOffset+TOMB_INC);
    }

    @Override
    public void setHasTombstone(boolean hasTombstone) {
        Platform.putBoolean(baseObject, baseOffset+TOMB_INC, hasTombstone);
    }

    @Override
    public long getTxnId1() {
        return Platform.getLong(baseObject,baseOffset+TXN_ID1_INC);
    }

    @Override
    public void setTxnId1(long transactionId1) {
        Platform.putLong(baseObject,baseOffset+TXN_ID1_INC,transactionId1);
    }

    @Override
    public long getTxnId2() {
        return Platform.getLong(baseObject,baseOffset+TXN_ID2_INC);
    }

    @Override
    public void setTxnId2(long transactionId2) {
        Platform.putLong(baseObject,baseOffset+TXN_ID2_INC,transactionId2);
    }

    @Override
    public long getEffectiveTimestamp() {
        return Platform.getLong(baseObject,baseOffset+EFF_TS_INC);
    }

    @Override
    public void setEffectiveTimestamp(long effectiveTimestamp) {
        Platform.putLong(baseObject,baseOffset+EFF_TS_INC,effectiveTimestamp);
    }

    @Override
    public int numberOfColumns() {
        return Platform.getInt(baseObject,baseOffset+NUM_COLS_INC);
    }

    @Override
    public void setNumberOfColumns(int numberOfColumns) {
        Platform.putInt(baseObject,baseOffset+NUM_COLS_INC,numberOfColumns);
    }

    @Override
    public ExecRow getData(int[] columns, ExecRow execRow) throws StandardException {
        assert columns.length == execRow.nColumns():"Columns Passed have mismatch with data passed";
        int numberOfColumns = numberOfColumns();
        int bitSetWidth = UnsafeRecordUtils.calculateBitSetWidthInBytes(numberOfColumns);
        UnsafeRow row = new UnsafeRow(UnsafeRecordUtils.cardinality(baseObject,baseOffset+COLS_BS_INC,bitSetWidth/8));
        row.pointTo(baseObject,baseOffset+UNSAFE_INC+bitSetWidth,100);
        int fromIndex = UnsafeRecordUtils.nextSetBit(baseObject,baseOffset+ COLS_BS_INC,0,8);
        int ordinal = 0;
        for (int i = 0; i< columns.length; i++) {
            do {
                if (fromIndex == -1 || fromIndex > columns[i]) { // Exhausted or past record
                    execRow.getColumn(i + 1).setToNull();
                    break;
                }
                if (fromIndex == columns[i]) { // set value and move on
                    execRow.getColumn(i + 1).read(row, ordinal);
                    fromIndex = UnsafeRecordUtils.nextSetBit(baseObject, baseOffset+ COLS_BS_INC, fromIndex+1, 8);
                    ordinal++;
                    break;
                } else  { // if (fromIndex < columns[i]) {
                    fromIndex = UnsafeRecordUtils.nextSetBit(baseObject, baseOffset+ COLS_BS_INC, fromIndex+1, 8);
                    ordinal++;
                }
            } while (true);

        }
        return execRow;
    }

    /**
     *
     * Active Records Remove Null Columns Automatically, updates keep them set.
     *
     * @param columns
     * @param data
     * @throws StandardException
     */
    @Override
    public void setData(int[] columns, ExecRow data) throws StandardException {
        assert columns.length == data.nColumns():"Columns Passed have mismatch with data passed";
        int columnCount = isActiveRecord?data.getNonNullCount():columns.length;
        BufferHolder bufferHolder = new BufferHolder(new UnsafeRow(columnCount));
        UnsafeRowWriter writer = new UnsafeRowWriter(bufferHolder,columnCount);
        DataValueDescriptor[] dvds = data.getRowArray();
        int bitSetWidth = UnsafeRecordUtils.calculateBitSetWidthInBytes(columnCount);
        writer.reset();
        int j = 0;
        for (int i = 0; i< columns.length; i++) {
            if (dvds[i].isNull() && isActiveRecord)
                continue;
            UnsafeRecordUtils.set(baseObject, baseOffset + COLS_BS_INC, columns[i]);
            dvds[i].write(writer,j);
            j++;
        }
        Platform.putInt(baseObject,baseOffset+ COLS_BS_INC +bitSetWidth,bufferHolder.cursor);
        Platform.copyMemory(bufferHolder.buffer,16,baseObject,
                baseOffset+UNSAFE_INC+bitSetWidth,bufferHolder.cursor);
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public void setKey(byte[] key) {
        this.key = key;
    }

    @Override
    public RecordType getRecordType() {
        return recordType;
    }

    @Override
    public void setRecordType(RecordType recordType) {
        this.recordType = recordType;
    }

    @Override
    public void setResolved(boolean resolved) {
        this.resolved = resolved;
    }

    @Override
    public boolean isResolved() {
        return resolved;
    }

    @Override
    public void setActive(boolean active) {
        this.active = active;
    }

    @Override
    public boolean isActive() {
        return active;
    }

    @Override
    public Record applyRollback(Iterator iterator) {
        return null;
    }

    @Override
    public Record[] updateRecord(Record updatedRecord, ExecRow rowDefinition) throws StandardException {
        UnsafeRecord uR = (UnsafeRecord) updatedRecord;
        UnsafeRow updateRow = uR.getUnsafeRow();
        UnsafeRow activeRow = getUnsafeRow();
        // Function call
        byte[] activeBitSetArray;
        byte[] redoBitSetArray;
        if (numberOfColumns() >= updatedRecord.numberOfColumns()) {
            activeBitSetArray = new byte[bitSetWidth()];
            redoBitSetArray = new byte[bitSetWidth()];
            Platform.copyMemory(baseObject,baseOffset+COLS_BS_INC,activeBitSetArray,16,activeBitSetArray.length);
            Platform.copyMemory(uR.baseObject,uR.baseOffset+COLS_BS_INC,redoBitSetArray,16,redoBitSetArray.length);
            UnsafeRecordUtils.or(activeBitSetArray,16,activeBitSetArray.length/8,uR.baseObject,uR.baseOffset+COLS_BS_INC,numberOfWords());

        } else {
            activeBitSetArray = new byte[uR.bitSetWidth()];
            redoBitSetArray = new byte[uR.bitSetWidth()];
            Platform.copyMemory(uR.baseObject,uR.baseOffset+COLS_BS_INC,activeBitSetArray,16,activeBitSetArray.length);
            Platform.copyMemory(uR.baseObject,uR.baseOffset+COLS_BS_INC,redoBitSetArray,16,redoBitSetArray.length);
            UnsafeRecordUtils.or(activeBitSetArray,16,activeBitSetArray.length/8,baseObject,baseOffset+COLS_BS_INC,uR.numberOfWords());
        }
        System.out.println("active -> " + UnsafeRecordUtils.displayBitSet(activeBitSetArray,16,activeBitSetArray.length/8));
        System.out.println("redo -> " + UnsafeRecordUtils.displayBitSet(redoBitSetArray,16,redoBitSetArray.length/8));

        // Must Remove Nulls from the active bitset, no reason to waste 8 bytes
        if (updateRow.anyNull()) {
            int nextSetBit = UnsafeRecordUtils.nextSetBit(uR.baseObject,uR.baseOffset+COLS_BS_INC,0,uR.numberOfWords());
            int i = 0;
            while (nextSetBit != -1) {
                if (updateRow.isNullAt(i)) {
                    UnsafeRecordUtils.unset(activeBitSetArray, 16, nextSetBit);
                    if (!UnsafeRecordUtils.isSet(baseObject,baseOffset+COLS_BS_INC,nextSetBit))
                        UnsafeRecordUtils.unset(redoBitSetArray, 16, nextSetBit);
                }
                i++;
                nextSetBit = UnsafeRecordUtils.nextSetBit(uR.baseObject,uR.baseOffset+COLS_BS_INC,nextSetBit+1,uR.numberOfWords());
            }
        }
        System.out.println("active -> " + UnsafeRecordUtils.displayBitSet(activeBitSetArray,16,activeBitSetArray.length/8));
        System.out.println("redo -> " + UnsafeRecordUtils.displayBitSet(redoBitSetArray,16,redoBitSetArray.length/8));
        int activeColumns = UnsafeRecordUtils.cardinality(activeBitSetArray,16,activeBitSetArray.length/8);
        int redoColumns = UnsafeRecordUtils.cardinality(redoBitSetArray,16,redoBitSetArray.length/8);
        UnsafeRow newActiveRow = new UnsafeRow(activeColumns);
        BufferHolder newActiveBuffer = new BufferHolder(newActiveRow);
        UnsafeRow newRedoRow = new UnsafeRow(redoColumns);
        BufferHolder newRedoBuffer = new BufferHolder(newRedoRow);

        newActiveBuffer.reset();
        newRedoBuffer.reset();







        int updatePosition = 0;
        int activePosition = 0;
        int totalPosition = 0;
        int fromIndex = UnsafeRecordUtils.nextSetBit(activeBitSetArray,16,0,activeBitSetArray.length/8);
        while(true) {
            if (fromIndex== -1)
                break;
            if (UnsafeRecordUtils.isSet(uR.baseObject,uR.baseOffset+COLS_BS_INC,fromIndex)) { // update has value, copy
                // remove from active record set field
                if (updateRow.isNullAt(updatePosition)) {

//                    throw new RuntimeException("Nulls should be removed already..");
                } else {
                    // Variable length requires copying offset/size, growing the buffer and copying the byte[] values in the buffer
                    if (rowDefinition.getColumn(fromIndex+1).isVariableLength()) {
                        final long offsetAndSize = updateRow.getLong(updatePosition);
                        final int offset = (int) (offsetAndSize >> 32);
                        final int size = (int) offsetAndSize;
                        newActiveBuffer.grow(size);
                        Platform.putLong(newActiveBuffer.buffer, unsafeRowPosition(newActiveRow,totalPosition), ( (long) (newActiveBuffer.cursor - 16) << 32) | (long) size);
                        Platform.copyMemory(updateRow.getBaseObject(), updateRow.getBaseOffset()+offset, newActiveBuffer.buffer, newActiveBuffer.cursor, size);
                        newActiveBuffer.cursor += size;
                    }
                    else {
                        Platform.copyMemory(updateRow.getBaseObject(), unsafeRowPosition(updateRow,updatePosition),
                                newActiveBuffer.buffer, unsafeRowPosition(newActiveRow,totalPosition), 8);
                    }
                    totalPosition++;
                }
                updatePosition++;
                if (UnsafeRecordUtils.isSet(baseObject,baseOffset+COLS_BS_INC,fromIndex))
                    activePosition++;
            } else {
                if (activeRow.isNullAt(activePosition)) {
                    throw new UnsupportedOperationException("active records can never have null fields set!!!");
                }
                if (rowDefinition.getColumn(fromIndex+1).isVariableLength()) {
                    final long offsetAndSize = activeRow.getLong(activePosition);
                    final int offset = (int) (offsetAndSize >> 32);
                    final int size = (int) offsetAndSize;
                    newActiveBuffer.grow(size);
                    Platform.putLong(newActiveBuffer.buffer, unsafeRowPosition(newActiveRow,totalPosition), ( (long)newActiveBuffer.cursor << 32) | (long) size);
                    Platform.copyMemory(activeRow.getBaseObject(), offset, newActiveBuffer.buffer, newActiveBuffer.cursor, size);
                    newActiveBuffer.cursor += size;
                }
                // 8 byte copy
                else {
                    Platform.copyMemory(activeRow.getBaseObject(), (int) unsafeRowPosition(activeRow,activePosition),
                            newActiveBuffer.buffer, unsafeRowPosition(newActiveRow,totalPosition), 8);
                }
                totalPosition++;
                activePosition++;
            }
            fromIndex = UnsafeRecordUtils.nextSetBit(activeBitSetArray,16,fromIndex+1,activeBitSetArray.length/8);
        }
        UnsafeRecord activeRecord = new UnsafeRecord(this.key,this.version,new byte[COLS_BS_INC+activeBitSetArray.length+newActiveBuffer.cursor],16,true);
        Platform.copyMemory(activeBitSetArray,16,activeRecord.baseObject,(long) (baseOffset+COLS_BS_INC),activeBitSetArray.length);
        activeRecord.setNumberOfColumns(newActiveRow.numFields());
        Platform.copyMemory(newActiveBuffer.buffer,16l,activeRecord.baseObject,
                (long) (baseOffset+UNSAFE_INC+activeBitSetArray.length),(long) newActiveBuffer.cursor);







        return new Record[]{activeRecord,null};
    }

    @Override
    public String toString() {
        return "UnsafeRecord {key=" + Hex.encodeHexString(key) +
                ", version=" + version +
                ", tombstone=" + hasTombstone() +
                ", txnId1=" + getTxnId1() +
                ", txnId2=" + getTxnId2() +
                ", effectiveTimestamp=" + getEffectiveTimestamp() +
                ", numberOfColumns=" + numberOfColumns() +
                "}"
                ;
    }

    private UnsafeRow getUnsafeRow() {
        int numberOfColumns = numberOfColumns();
        int bitSetWidth = UnsafeRecordUtils.calculateBitSetWidthInBytes(numberOfColumns);
        UnsafeRow activeRow = new UnsafeRow(UnsafeRecordUtils.cardinality(baseObject,baseOffset+29,bitSetWidth/8));
        activeRow.pointTo(baseObject,baseOffset+UNSAFE_INC+bitSetWidth,Platform.getInt(baseObject,baseOffset+29+bitSetWidth+4));
        return activeRow;
    }

    public int bitSetWidth() {
        return UnsafeRecordUtils.calculateBitSetWidthInBytes(numberOfColumns());
    }

    public int numberOfWords() {
        return UnsafeRecordUtils.numberOfWordsForColumns(numberOfColumns());
    }

    public static int unsafeRowPosition(UnsafeRow unsafeRow, int position) {
        return (int) (unsafeRow.getBaseOffset() + UnsafeRow.calculateBitSetWidthInBytes(unsafeRow.numFields()) + position * 8l);
    }

}