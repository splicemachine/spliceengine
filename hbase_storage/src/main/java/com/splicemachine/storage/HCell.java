/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.storage;

import com.splicemachine.hbase.CellUtils;
import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An HBase-implementation of a DataCell.
 *
 * @author Scott Fines
 *         Date: 12/14/15
 */
@NotThreadSafe
public class HCell implements DataCell{
    private Cell delegate;
    private CellType cellType;

    public HCell(){ }

    public HCell(Cell delegate){
        this.delegate=delegate;
    }

    public void set(Cell cell){
        this.delegate = cell;
        this.cellType = null;
    }

    @Override
    public byte[] valueArray(){
        return delegate.getValueArray();
    }

    @Override
    public int valueOffset(){
        return delegate.getValueOffset();
    }

    @Override
    public int valueLength(){
        return delegate.getValueLength();
    }

    @Override
    public byte[] keyArray(){
        return delegate.getRowArray();
    }

    @Override
    public int keyOffset(){
        return delegate.getRowOffset();
    }

    @Override
    public int keyLength(){
        return delegate.getRowLength();
    }

    @Override
    public CellType dataType(){
        if(cellType==null)
            cellType = parseType(delegate);
        return cellType;
    }

    @Override
    public DataCell getClone(){
        return new HCell(delegate);
    }

    @Override
    public boolean matchesFamily(byte[] family){
        if(delegate==null) return false;
        return CellUtils.singleMatchingFamily(delegate,family);
    }

    @Override
    public boolean matchesQualifier(byte[] family,byte[] dataQualifierBytes){
        if(delegate==null) return false;
        return CellUtils.singleMatchingFamily(delegate,family)
                && CellUtils.singleMatchingQualifier(delegate,dataQualifierBytes);
    }

    @Override
    public byte[] family(){
        if(delegate==null) return null;
        return delegate.getFamily();
    }

    @Override
    public byte[] qualifier(){
        if(delegate==null) return null;
        return delegate.getQualifier();
    }

    @Override
    public long version(){
        if(delegate==null) return -1;
        return delegate.getTimestamp();
    }

    @Override
    public long valueAsLong(){
        return Bytes.toLong(delegate.getValueArray(),delegate.getValueOffset(),delegate.getValueLength());
    }

    @Override
    public DataCell copyValue(byte[] newValue,CellType newCellType){
        byte[] qualArray;
        int qualOff;
        int qualLen;
        if(newCellType==cellType){
            qualArray = delegate.getQualifierArray();
            qualOff = delegate.getQualifierOffset();
            qualLen = delegate.getQualifierLength();
        }else{
            switch(newCellType){
                case COMMIT_TIMESTAMP:
                    qualArray = SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES;
                    break;
                case ANTI_TOMBSTONE:
                    assert Bytes.equals(SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES,newValue):
                            "Programmer error: cannot create an ANTI-TOMBSTONE cell without an anti-tombstone value";
                case TOMBSTONE:
                    qualArray = SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES;
                    break;
                case USER_DATA:
                    qualArray = SIConstants.PACKED_COLUMN_BYTES;
                    break;
                case FOREIGN_KEY_COUNTER:
                    qualArray = SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES;
                    break;
                case OTHER:
                default:
                    throw new IllegalArgumentException("Programmer error: unexpected cell type "+ newCellType);
            }
            qualOff=0;
            qualLen=qualArray.length;
        }
        Cell c = new KeyValue(delegate.getRowArray(),delegate.getRowOffset(),delegate.getRowLength(),
                delegate.getFamilyArray(),delegate.getFamilyOffset(),delegate.getFamilyLength(),
                qualArray,qualOff,qualLen,
                delegate.getTimestamp(),KeyValue.Type.codeToType(delegate.getTypeByte()),
                newValue,0,newValue.length);
        return new HCell(c);
    }

    @Override
    public int encodedLength(){
        return delegate.getQualifierLength()
                +delegate.getFamilyLength()
                +delegate.getRowLength()
                +delegate.getValueLength()
                +delegate.getTagsLength();
    }

    @Override
    public byte[] value(){
        if(delegate==null)  return null;
        else return delegate.getValue();
    }

    @Override
    public boolean equals(Object o){
        if(o==this) return true;
        if(!(o instanceof DataCell)) return false;
        return compareTo((DataCell)o)==0;
    }

    @Override
    public int hashCode(){
        return delegate!=null?delegate.hashCode():0;
    }

    @Override
    public int compareTo(DataCell o){
        if(o==this) return 0;
        assert o instanceof HCell: "Programmer error: Must implement comparison for non-HCell versions!";
        HCell hc = (HCell)o;
        if(delegate==null){
            if(hc.delegate==null) return 0;
            return -1; //sort nulls first
        }else if(hc.delegate==null) return 1;
        else return KeyValue.COMPARATOR.compare(delegate,hc.delegate);
    }

    @Override
    public byte[] key(){
        if(delegate==null) return null;
        return delegate.getRow();
    }

    @Override
    public byte[] qualifierArray(){
        if(delegate==null) return null;
        return delegate.getQualifierArray();
    }

    @Override
    public int qualifierOffset(){
        if(delegate==null) return 0;
        return delegate.getQualifierOffset();
    }

    public Cell unwrapDelegate(){
        return delegate;
    }

    @Override
    public long familyLength(){
        if(delegate==null) return 0;
        return delegate.getFamilyLength();
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private CellType parseType(Cell cell){
        if (CellUtils.singleMatchingQualifier(cell,SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES)) {
            return CellType.COMMIT_TIMESTAMP;
        } else if (CellUtils.singleMatchingQualifier(cell, SIConstants.PACKED_COLUMN_BYTES)) {
            return CellType.USER_DATA;
        } else if (CellUtils.singleMatchingQualifier(cell, SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES)) {
            if (CellUtils.matchingValue(cell, SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES)) {
                return CellType.ANTI_TOMBSTONE;
            } else {
                return CellType.TOMBSTONE;
            }
        } else if (CellUtils.singleMatchingQualifier(cell, SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES)) {
            return CellType.FOREIGN_KEY_COUNTER;
        }
        return CellType.OTHER;
    }
}
