/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
import org.apache.hadoop.hbase.CellUtil;
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
            cellType = CellUtils.getKeyValueType(delegate);
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
        return CellUtil.cloneFamily(delegate);
    }

    @Override
    public byte[] qualifier(){
        if(delegate==null) return null;
        return CellUtil.cloneQualifier(delegate);
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
                    qualArray = SIConstants.COMMIT_TIMESTAMP_COLUMN_BYTES;
                    break;
                case ANTI_TOMBSTONE:
                    assert Bytes.equals(SIConstants.ANTI_TOMBSTONE_VALUE_BYTES, newValue):
                            "Programmer error: cannot create an ANTI-TOMBSTONE cell without an anti-tombstone value";
                case TOMBSTONE:
                    qualArray = SIConstants.TOMBSTONE_COLUMN_BYTES;
                    break;
                case USER_DATA:
                    qualArray = SIConstants.PACKED_COLUMN_BYTES;
                    break;
                case FOREIGN_KEY_COUNTER:
                    qualArray = SIConstants.FK_COUNTER_COLUMN_BYTES;
                    break;
                case DELETE_RIGHT_AFTER_FIRST_WRITE_TOKEN:
                    assert Bytes.equals(SIConstants.DELETE_RIGHT_AFTER_FIRST_WRITE_VALUE_BYTES, newValue):
                            "Programmer error: cannot create an DELETE_RIGHT_AFTER_FIRST_WRITE cell without a value";
                case FIRST_WRITE_TOKEN:
                    qualArray = SIConstants.FIRST_OCCURRENCE_TOKEN_COLUMN_BYTES;
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
        else return CellUtil.cloneValue(delegate);
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
        return CellUtil.cloneRow(delegate);
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

    @Override
    public String toString() {
        return delegate==null?"null":delegate.toString();
    }
}
