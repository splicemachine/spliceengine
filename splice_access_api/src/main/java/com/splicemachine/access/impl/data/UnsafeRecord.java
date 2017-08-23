package com.splicemachine.access.impl.data;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.Record;
import com.splicemachine.storage.RecordType;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.IntArrays;
import org.apache.commons.codec.binary.Hex;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.unsafe.Platform;
import java.util.Iterator;

/**
 *
 * Basic Record for Data stored in the following format.
 *
 * byte[] mapping structure
 *
 * [Tombstone(1)]
 * [TXN_ID1(8)]
 * [TXN_ID2(8)]
 * [Effective TS(8)]
 * [Number of Columns(4)]
 * [Column Bit Set (word aligned, 64 bits - > 8 bytes)]
 *
 * UnsafeRow --->
 * [Column Bit Set (word aligned, 64 bits - > 8 bytes)] cardinality of Column Bit Set
 * Word length columns N [(8)],[(8)],[(8)],[(8)],[(8)],[(8)],[(8)],[(8)]
 * Variable Length Data {(variable)}
 */
public class UnsafeRecord implements Record<byte[]> {
    public static final byte[] DEFAULT_FAMILY_ACTIVE_BYTES = Bytes.toBytes("A");
    protected byte[] keyObject;
    protected long keyOffset;
    protected int keyLength;
    protected long version;
    protected byte[] baseObject;
    protected long baseOffset;
    protected long baseLength;
    protected boolean active;
    protected boolean resolved;
    protected boolean isActiveRecord;
    protected RecordType recordType;
    protected static int TOTAL_BYTES = 0;
    protected static int ASIZE = 16;
    protected static int TOMB_INC = TOTAL_BYTES+ASIZE;//+8;
    protected static int TXN_ID1_INC = TOMB_INC+1;
    protected static int EFF_TS_INC = TXN_ID1_INC+8;
    protected static int NUM_COLS_INC = EFF_TS_INC+8;
    protected static int COLS_BS_INC = NUM_COLS_INC+4;
    protected static int UNSAFE_INC = COLS_BS_INC;
    protected static int WORD_SIZE = 8;

    public UnsafeRecord() {}

    public UnsafeRecord(byte[] key, long version) {
        this.keyObject = key;
        this.keyLength = key.length;
        this.keyOffset = 0;
        this.version = version;
    }

    public UnsafeRecord(byte[] key, long version, boolean isActiveRecord) {
        this(key,version,isActiveRecord, 200); // TODO Fix JL
    }

    public UnsafeRecord(byte[] key, long version, boolean isActiveRecord, int startingBackingBufferSize) {
        this(key,version,new byte[startingBackingBufferSize],0,isActiveRecord); // TODO Fix JL
    }

    public UnsafeRecord(byte[] key, long version, byte[] baseObject,long baseOffset, boolean isActiveRecord) {
        this(key,0,key.length,version,baseObject,baseOffset,isActiveRecord);
    }

    public UnsafeRecord(byte[] keyObject, long keyOffset, int keyLength, long version, byte[] baseObject,long baseOffset, boolean isActiveRecord) {
        assert keyObject != null:"key cannot be null";
        assert version > 0L:"version cannot be negative";
        this.keyObject = keyObject;
        this.keyOffset = keyOffset;
        this.keyLength = keyLength;
        this.version = version;
        this.baseObject = baseObject;
        this.baseOffset = baseOffset;
        this.isActiveRecord = isActiveRecord;
    }

    public UnsafeRecord(DataCell dataCell) {
        this.keyObject = dataCell.keyArray();
        this.keyOffset = dataCell.keyOffset();
        this.keyLength = dataCell.keyLength();
        this.version = dataCell.version();
        this.baseObject = dataCell.valueArray();
        this.baseOffset = dataCell.valueOffset();
        this.baseLength = dataCell.valueLength();
        this.isActiveRecord = dataCell.matchesFamily(DEFAULT_FAMILY_ACTIVE_BYTES);
    }

    public void wrap(DataCell dataCell) {
        this.keyObject = dataCell.keyArray();
        this.keyOffset = dataCell.keyOffset();
        this.keyLength = dataCell.keyLength();
        this.version = dataCell.version();
        this.baseObject = dataCell.valueArray();
        this.baseOffset = dataCell.valueOffset();
        this.isActiveRecord = dataCell.matchesFamily(DEFAULT_FAMILY_ACTIVE_BYTES);
        this.baseLength = dataCell.valueLength();
    }

    public void wrap(KVPair kvPair, boolean isActiveRecord, int version) {
        ByteSlice rowKey = kvPair.rowKeySlice();
        this.keyObject = rowKey.array();
        this.keyOffset = rowKey.offset();
        this.keyLength = rowKey.length();
        ByteSlice value = kvPair.valueSlice();
        this.version = version;
        this.baseObject = value.array();
        this.baseOffset = value.offset();
        this.baseLength = value.length();
        this.isActiveRecord = isActiveRecord;
    }

    public KVPair getKVPair() {
        if (hasTombstone())
            return new KVPair(keyObject,(int)keyOffset,(int)keyLength,(byte[]) baseObject,0,UNSAFE_INC-ASIZE, KVPair.Type.DELETE);
        else if (getVersion() == 2)
            return new KVPair(keyObject,(int)keyOffset,(int)keyLength,(byte[]) baseObject,0,(int)baseLength, KVPair.Type.UPDATE);
        else if (getVersion() ==1)
            return new KVPair(keyObject,(int)keyOffset,(int)keyLength,(byte[]) baseObject,0,(int)baseLength, KVPair.Type.INSERT);
        else if (getVersion() ==-1)
            return new KVPair(keyObject,(int)keyOffset,(int)keyLength,(byte[]) baseObject,0,(int)baseLength, KVPair.Type.UPSERT);
        else
            throw new UnsupportedOperationException("missing KVPair Manip");
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
        baseLength = UNSAFE_INC; // Trim the data for the key value store when generated...  Just need tombstone and txn info...
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
    public void getData(FormatableBitSet accessedColumns, ExecRow execRow) throws StandardException {
        getData(accessedColumns,execRow.getRowArray());
    }

    @Override
    public void getData(FormatableBitSet accessedColumns, DataValueDescriptor[] dvds) throws StandardException {
        if (accessedColumns == null) {
            accessedColumns = new FormatableBitSet(dvds.length);
            accessedColumns.setAll();
        }
        assert accessedColumns.getNumBitsSet() == dvds.length:"Columns Passed have mismatch with data passed";
        int numberOfColumns = numberOfColumns();
        int bitSetWidth = UnsafeRecordUtils.calculateBitSetWidthInBytes(numberOfColumns);
        UnsafeRow row = new UnsafeRow(UnsafeRecordUtils.cardinality(baseObject,baseOffset+COLS_BS_INC,bitSetWidth/WORD_SIZE));
        row.pointTo(baseObject,baseOffset+UNSAFE_INC+bitSetWidth,100);
        int fromIndex = UnsafeRecordUtils.nextSetBit(baseObject,baseOffset+ COLS_BS_INC,0,WORD_SIZE);
        int ordinal = 0;
        int accessedIndex = -1;
        while ( (accessedIndex = accessedColumns.anySetBit(accessedIndex)) != -1) {
            do {
                if (fromIndex == -1 || fromIndex > accessedIndex) { // Exhausted or past record
                    dvds[accessedIndex].setToNull();
                    break;
                }
                if (fromIndex == accessedIndex) { // set value and move on
                    dvds[accessedIndex].read(row, ordinal);
                    fromIndex = UnsafeRecordUtils.nextSetBit(baseObject, baseOffset+ COLS_BS_INC, fromIndex+1, WORD_SIZE);
                    ordinal++;
                    break;
                } else  { // if (fromIndex < columns[i]) {
                    fromIndex = UnsafeRecordUtils.nextSetBit(baseObject, baseOffset+ COLS_BS_INC, fromIndex+1, WORD_SIZE);
                    ordinal++;
                }
            } while (true);

        }
    }

    @Override
    public void getData(BitSet accessedColumns, DataValueDescriptor[] dvds) throws StandardException {
        if (accessedColumns == null) {
            accessedColumns = new BitSet(dvds.length);
            accessedColumns.set(0,dvds.length);
        }
        int numberOfColumns = numberOfColumns();
        int bitSetWidth = UnsafeRecordUtils.calculateBitSetWidthInBytes(numberOfColumns);
        UnsafeRow row = new UnsafeRow(UnsafeRecordUtils.cardinality(baseObject,baseOffset+COLS_BS_INC,bitSetWidth/WORD_SIZE));
        row.pointTo(baseObject,baseOffset+UNSAFE_INC+bitSetWidth,100);
        int fromIndex = UnsafeRecordUtils.nextSetBit(baseObject,baseOffset+ COLS_BS_INC,0,WORD_SIZE);
        int ordinal = 0;
        int accessedIndex = -1;
        while ( (accessedIndex = accessedColumns.nextSetBit(accessedIndex+1)) != -1) {
            if (numberOfColumns< accessedIndex || !UnsafeRecordUtils.isSet(baseObject, baseOffset+ COLS_BS_INC, accessedIndex)) {
                dvds[ordinal].setToNull();
            } else {
                dvds[ordinal].read(row, accessedIndex);
            }
            ordinal++;
        }
    }

    @Override
    public void getData(int[] columns, ExecRow execRow) throws StandardException {
        assert columns.length == execRow.nColumns():"Columns Passed have mismatch with data passed";
        int numberOfColumns = numberOfColumns();
        int bitSetWidth = UnsafeRecordUtils.calculateBitSetWidthInBytes(numberOfColumns);
        UnsafeRow row = new UnsafeRow(UnsafeRecordUtils.cardinality(baseObject,baseOffset+COLS_BS_INC,bitSetWidth/WORD_SIZE));
        row.pointTo(baseObject,baseOffset+UNSAFE_INC+bitSetWidth,(int)baseLength);
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
                    fromIndex = UnsafeRecordUtils.nextSetBit(baseObject, baseOffset+ COLS_BS_INC, fromIndex+1, WORD_SIZE);
                    ordinal++;
                    break;
                } else  { // if (fromIndex < columns[i]) {
                    fromIndex = UnsafeRecordUtils.nextSetBit(baseObject, baseOffset+ COLS_BS_INC, fromIndex+1, WORD_SIZE);
                    ordinal++;
                }
            } while (true);

        }
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
    public void setData(FormatableBitSet formatableBitSet, ExecRow data) throws StandardException {
        setData(formatableBitSet==null?IntArrays.count(data.nColumns()):formatableBitSet.getIntArray(),data);
    }

    @Override
    public void setData(FormatableBitSet formatableBitSet, DataValueDescriptor[] dvds) throws StandardException {
        setData(formatableBitSet==null?IntArrays.count(dvds.length):formatableBitSet.getIntArray(),dvds);
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
        DataValueDescriptor[] dvds = data.getRowArray();
        setData(columns,dvds);
    }

    @Override
    public void setData(int[] columns, DataValueDescriptor[] dvds) throws StandardException {
        assert columns.length == dvds.length:"Columns Passed have mismatch with data passed";
        int columnCount = isActiveRecord? ValueRow.nonNullCountFromArray(dvds):columns.length;
        UnsafeRow ur = new UnsafeRow(columnCount);
        BufferHolder bufferHolder = new BufferHolder(ur);
        UnsafeRowWriter writer = new UnsafeRowWriter(bufferHolder,columnCount);
        int bitSetWidth = UnsafeRecordUtils.calculateBitSetWidthInBytes(columnCount);
        writer.reset();
        int j = 0;
        checkCapacity(baseOffset + COLS_BS_INC + bitSetWidth + 8*columnCount);

        for (int i = 0; i< columns.length; i++) {
            if ( (dvds[i] == null || dvds[i].isNull()) && isActiveRecord)
                continue;
            UnsafeRecordUtils.set(baseObject, baseOffset + COLS_BS_INC, columns[i]);
            dvds[i].write(writer,j);
            j++;
        }
        setHasTombstone(false);
        checkCapacity(baseOffset+UNSAFE_INC+bitSetWidth+bufferHolder.cursor);
        Platform.copyMemory(bufferHolder.buffer,16,baseObject,
                baseOffset+UNSAFE_INC+bitSetWidth,bufferHolder.cursor);
        baseLength = UNSAFE_INC+bitSetWidth + bufferHolder.cursor;
    }

    /*

            DataValueDescriptor[] dvds = currentRow.getRowArray();
        for(int i=finalHeapList.anySetBit();i>=0;i=finalHeapList.anySetBit(i)){
            int position = keyColumns[i];
            DataValueDescriptor dvd = dvds[position];
            DescriptorSerializer serializer = serializers[position];
            serializer.encode(encoder,dvd,false);
        }
     */
    @Override
    public void setData(int[] colPosMap, FormatableBitSet heapList, DataValueDescriptor[] dvds) throws StandardException {
        int columnCount = colPosMap.length;
        UnsafeRow ur = new UnsafeRow(columnCount);
        BufferHolder bufferHolder = new BufferHolder(ur);
        UnsafeRowWriter writer = new UnsafeRowWriter(bufferHolder,columnCount);
        int bitSetWidth = UnsafeRecordUtils.calculateBitSetWidthInBytes(columnCount);
        writer.reset();
        int j = 0;
        checkCapacity(baseOffset + COLS_BS_INC + bitSetWidth + 8*columnCount);

        for(int i=heapList.anySetBit();i>=0;i=heapList.anySetBit(i)){
            int pos = colPosMap[i];
            if ( (dvds[pos] == null || dvds[pos].isNull()) && isActiveRecord)
                continue;
            UnsafeRecordUtils.set(baseObject, baseOffset + COLS_BS_INC, i-1);
            dvds[pos].write(writer,j);
            j++;
        }
        setHasTombstone(false);
        checkCapacity(baseOffset+UNSAFE_INC+bitSetWidth+bufferHolder.cursor);
        Platform.copyMemory(bufferHolder.buffer,16,baseObject,
                baseOffset+UNSAFE_INC+bitSetWidth,bufferHolder.cursor);
        baseLength = UNSAFE_INC+bitSetWidth + bufferHolder.cursor;
    }


    @Override
    public void setData(DataValueDescriptor[] dvds) throws StandardException {
        setData(IntArrays.count(dvds.length),dvds);
    }

    @Override
    public byte[] getKey() {
        if (keyLength == keyObject.length && keyOffset == 0)
            return keyObject;
        byte[] key = new byte[keyLength];
        Platform.copyMemory(keyObject,keyOffset+ASIZE,key,ASIZE,keyLength);
        return key;
    }

    @Override
    public byte[] getValue() {
        if (baseLength == (baseObject).length && baseOffset == 0)
            return (byte[])baseObject;
        byte[] value = new byte[(int)baseLength];
        Platform.copyMemory(baseObject,baseOffset+ASIZE,value,ASIZE,baseLength);
        return value;
    }

    @Override
    public void setKey(byte[] key) {
        assert key != null:"Passed in key cannot be null";
        this.keyOffset = 0;
        this.keyObject = key;
        this.keyLength = key.length;
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
    public Record applyRollback(Iterator<Record<byte[]>> iterator, ExecRow rowDefinition) throws StandardException {
        Record rolledBackRecord = this;
        while (iterator.hasNext()) {
            rolledBackRecord = rolledBackRecord.updateRecord(iterator.next(),rowDefinition)[0];
        }
        return rolledBackRecord;
    }

    @Override
    public Record[] updateRecord(Record updatedRecord, ExecRow rowDefinition) throws StandardException {
        if (updatedRecord.hasTombstone()) { // delete record
            updatedRecord.setVersion(getVersion()+1);
            // Must Remove Record
            return new Record[]{updatedRecord,this};
        }

        if (hasTombstone()) { // undelete record
            updatedRecord.setVersion(getVersion()+1);
            return new Record[]{updatedRecord,this};
        }

        UnsafeRecord uR = (UnsafeRecord) updatedRecord;
        UnsafeRow updateRow = uR.getUnsafeRow();
        UnsafeRow activeRow = getUnsafeRow();
        // Column Lengths are not guaranteed, especially with
        int maximumColumns = Math.max(numberOfColumns(),updatedRecord.numberOfColumns());
        int bitSetWidth = UnsafeRow.calculateBitSetWidthInBytes(maximumColumns);
        int bitSetWords = bitSetWidth/8;
        // Active Bit Set
        byte[] newActiveBitSetArray = createZeroedOutBitSetArray(bitSetWidth);
        Platform.copyMemory(baseObject,baseOffset+COLS_BS_INC,newActiveBitSetArray,ASIZE,bitSetWidth());
        // Redo Bit Set
        byte[] newRedoBitSetArray = createZeroedOutBitSetArray(bitSetWidth);
        Platform.copyMemory(uR.baseObject,uR.baseOffset+COLS_BS_INC,newRedoBitSetArray,ASIZE,uR.bitSetWidth());
        UnsafeRecordUtils.or(newActiveBitSetArray,ASIZE,bitSetWords,uR.baseObject,uR.baseOffset+COLS_BS_INC,bitSetWords);
        // Global Or Bit Set
        byte[] globalOrBitSetArray = createZeroedOutBitSetArray(bitSetWidth);
        Platform.copyMemory(newActiveBitSetArray,ASIZE,globalOrBitSetArray,ASIZE,bitSetWidth);
        // Must Remove Nulls from the active bitset, no reason to waste 8 bytes
        if (updateRow.anyNull()) {
            int nextSetBit = UnsafeRecordUtils.nextSetBit(uR.baseObject,uR.baseOffset+COLS_BS_INC,0,uR.numberOfWords());
            int i = 0;
            while (nextSetBit != -1) {
                if (updateRow.isNullAt(i)) {
                    UnsafeRecordUtils.unset(newActiveBitSetArray, ASIZE, nextSetBit); // Nulls are not transferred to active records
                    if (!UnsafeRecordUtils.isSet(baseObject,baseOffset+COLS_BS_INC,nextSetBit))
                        UnsafeRecordUtils.unset(newRedoBitSetArray, ASIZE, nextSetBit); // setting a null field to null does not generate a redo record (special case)
                }
                i++;
                nextSetBit = UnsafeRecordUtils.nextSetBit(uR.baseObject,uR.baseOffset+COLS_BS_INC,nextSetBit+1,uR.numberOfWords());
            }
        }
        int activeColumns = UnsafeRecordUtils.cardinality(newActiveBitSetArray,ASIZE,bitSetWords);
        int redoColumns = UnsafeRecordUtils.cardinality(newRedoBitSetArray,ASIZE,bitSetWords);
        UnsafeRow newActiveRow = new UnsafeRow(activeColumns);
        BufferHolder newActiveBuffer = new BufferHolder(newActiveRow);
        UnsafeRow newRedoRow = new UnsafeRow(redoColumns);
        BufferHolder newRedoBuffer = new BufferHolder(newRedoRow);
        newActiveBuffer.reset();
        newRedoBuffer.reset();

        int fromIndex = UnsafeRecordUtils.nextSetBit(globalOrBitSetArray,ASIZE,0,bitSetWords);

        int activePos = 0;
        int updatePos = 0;
        int newActivePos = 0;
        int newRedoPos = 0;
        while (fromIndex != -1) {
            if (UnsafeRecordUtils.isSet(uR.baseObject,uR.baseOffset+COLS_BS_INC,fromIndex)) { // Has Update Entry
                if (UnsafeRecordUtils.isSet(newActiveBitSetArray,ASIZE,fromIndex)) { // Required for Active write and we know nulls are already removed
                    copyUnsafeData(rowDefinition, fromIndex, updateRow, updatePos, newActiveBuffer, newActiveRow, newActivePos);
                    newActivePos++;
                    if (UnsafeRecordUtils.isSet(baseObject,baseOffset+COLS_BS_INC,fromIndex)) { // Has Active Entry
                        // write active to newRedo
                        copyUnsafeData(rowDefinition, fromIndex, activeRow, activePos, newRedoBuffer, newRedoRow, newRedoPos);
                        activePos++;
                    } else {
                        // Write Null to redo
                        newRedoRow.setNullAt(newRedoPos);
                    }
                    newRedoPos++;
                } else {
                    if (UnsafeRecordUtils.isSet(baseObject,baseOffset+COLS_BS_INC,fromIndex)) { // setting an existing value to null
                        // write active to newRedo
                        copyUnsafeData(rowDefinition, fromIndex, activeRow, activePos, newRedoBuffer, newRedoRow, newRedoPos);
                        activePos++;
                        newRedoPos++;
                    }
                    // NULL Active-NULL Update Write Corner Case (Ignore)
                }
                updatePos++;
            } else { // Only Active Entry
                // write active to newActive
                copyUnsafeData(rowDefinition, fromIndex, activeRow, activePos, newActiveBuffer, newActiveRow, newActivePos);
                activePos++;
                newActivePos++;
            }
            fromIndex = UnsafeRecordUtils.nextSetBit(globalOrBitSetArray,ASIZE,fromIndex+1,bitSetWords);
        }
        // Active Record Generation
        UnsafeRecord activeRecord = new UnsafeRecord(this.keyObject,this.keyOffset,this.keyLength,this.version+1,new byte[COLS_BS_INC+newActiveBitSetArray.length+newActiveBuffer.cursor],0,true);
        Platform.copyMemory(uR.baseObject, uR.baseOffset+TXN_ID1_INC,activeRecord.baseObject,activeRecord.baseOffset+TXN_ID1_INC,ASIZE); // txnid1 and txnid2
        activeRecord.setNumberOfColumns(newActiveRow.numFields());
        Platform.copyMemory(newActiveBitSetArray,ASIZE,activeRecord.baseObject,(long) (activeRecord.baseOffset+COLS_BS_INC),newActiveBitSetArray.length);
        Platform.copyMemory(newActiveBuffer.buffer,ASIZE,activeRecord.baseObject,
                (long) (activeRecord.baseOffset+UNSAFE_INC+newActiveBitSetArray.length),(long) newActiveBuffer.cursor);
        activeRecord.baseLength = UNSAFE_INC+newActiveBitSetArray.length + newActiveBuffer.cursor;
        // Redo Record Generation
        UnsafeRecord redoRecord = new UnsafeRecord(this.keyObject,this.keyOffset,this.keyLength,this.version,new byte[COLS_BS_INC+newRedoBitSetArray.length+newRedoBuffer.cursor],0,true);
        Platform.copyMemory(baseObject, baseOffset+TXN_ID1_INC,redoRecord.baseObject,redoRecord.baseOffset+TXN_ID1_INC,ASIZE); // txnid1, txnid2, eff-ts
        redoRecord.setNumberOfColumns(newRedoRow.numFields());
        Platform.copyMemory(newRedoBitSetArray,ASIZE,redoRecord.baseObject,(long) (redoRecord.baseOffset+COLS_BS_INC),newRedoBitSetArray.length);
        Platform.copyMemory(newRedoBuffer.buffer,ASIZE,redoRecord.baseObject,
                (long) (redoRecord.baseOffset+UNSAFE_INC+newRedoBitSetArray.length),(long) newRedoBuffer.cursor);
        redoRecord.baseLength = UNSAFE_INC+newRedoBitSetArray.length + newRedoBuffer.cursor;
        return new Record[]{activeRecord,redoRecord};
    }

    @Override
    public String toString() {
        return "UnsafeRecord {key=" + Hex.encodeHexString(getKey()) +
                ", version=" + version +
                ", tombstone=" + hasTombstone() +
                ", txnId1=" + getTxnId1() +
                ", effectiveTimestamp=" + getEffectiveTimestamp() +
                ", numberOfColumns=" + numberOfColumns() +
                ", columnBitSet=" + (hasTombstone()?"delete":UnsafeRecordUtils.displayBitSet(baseObject,baseOffset+UNSAFE_INC,UnsafeRecordUtils.numberOfWordsForColumns(numberOfColumns()))) +
                "}"
                ;
    }

    private UnsafeRow getUnsafeRow() {
        int numberOfColumns = numberOfColumns();
        int bitSetWidth = UnsafeRecordUtils.calculateBitSetWidthInBytes(numberOfColumns);
        UnsafeRow activeRow = new UnsafeRow(UnsafeRecordUtils.cardinality(baseObject,baseOffset+UNSAFE_INC,bitSetWidth/8));
        activeRow.pointTo(baseObject,baseOffset+UNSAFE_INC+bitSetWidth,Platform.getInt(baseObject,baseOffset+UNSAFE_INC+bitSetWidth+4));
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

    private byte[] createZeroedOutBitSetArray(int nullBitsSize) {
        byte[] zeroedOutArray = new byte[nullBitsSize];
        for (int i = 0; i < nullBitsSize; i += 8) {
            Platform.putLong(zeroedOutArray, ASIZE + i, 0L);
        }
        return zeroedOutArray;
    }

    /**
     *
     * Copy
     *
     * @param rowDefinition Represents the field definitions of the rows
     * @param rowDefIndex pointer to the zero based representation in the exec row
     * @param dataRow
     * @param dataOrdinal
     * @param newBuffer
     * @param newDataRow
     * @param newDataOrdinal
     * @throws StandardException
     */
    public void copyUnsafeData(ExecRow rowDefinition, int rowDefIndex, UnsafeRow dataRow, int dataOrdinal, BufferHolder newBuffer, UnsafeRow newDataRow, int newDataOrdinal) throws StandardException{
        if (rowDefinition.getColumn(rowDefIndex+1).isVariableLength()) {
            final long offsetAndSize = dataRow.getLong(dataOrdinal);
            final int offset = (int) (offsetAndSize >> 32);
            final int size = (int) offsetAndSize;
            newBuffer.grow(size);
            Platform.putLong(newBuffer.buffer, unsafeRowPosition(newDataRow,newDataOrdinal), ( (long) (newBuffer.cursor - ASIZE) << 32) | (long) size);
            Platform.copyMemory(dataRow.getBaseObject(), dataRow.getBaseOffset()+offset, newBuffer.buffer, newBuffer.cursor, size);
            newBuffer.cursor += size;
        }
        else {
            Platform.copyMemory(dataRow.getBaseObject(), unsafeRowPosition(dataRow,dataOrdinal),
                    newBuffer.buffer, unsafeRowPosition(newDataRow,newDataOrdinal), WORD_SIZE);
        }
    }

    @Override
    public int getSize() {
        return ((byte[])baseObject).length;
    }

    @Override
    public ByteSlice rowKeySlice() {
        return ByteSlice.wrap(keyObject,(int)keyOffset,keyLength);
    }

    // JL - TODO (This does not seem needed to me)
    @Override
    public Record cancelToDelete() {
//        mutations.add(new Record(kvPair.getKey(), kvPair.getValue(), RecordType.DELETE));

        return null;
    }

    @Override
    public Record createIndexDelete(int[] mainColToIndexPosMap, boolean uniqueWithDuplicateNulls, boolean[] descending, ExecRow indexRow) throws StandardException {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public Record createIndexInsert(int[] mainColToIndexPosMap, boolean uniqueWithDuplicateNulls, boolean[] descending, ExecRow indexRow) throws StandardException {
        throw new UnsupportedOperationException("not implemented yet");
    }

    public void checkCapacity(long desiredPosition) {
        if (baseObject==null) {
            baseObject = new byte[(int)desiredPosition];
            baseOffset = 0;
        }
        long currentLength = baseObject.length; // TODO JL
        if (currentLength<desiredPosition) {
            long newLength = (desiredPosition < 1073741823?desiredPosition * 2:2147483647);
            byte[] tmp = new byte[(int)newLength];
            Platform.copyMemory(baseObject, (long)Platform.BYTE_ARRAY_OFFSET, tmp, (long)Platform.BYTE_ARRAY_OFFSET, currentLength);
            this.baseObject = tmp;
        }
    }

}