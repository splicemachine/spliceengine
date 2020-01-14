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

package com.splicemachine.si.impl;

import com.splicemachine.access.util.ByteComparisons;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.primitives.ByteComparator;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 1/18/16
 */
public class MOperationFactory implements OperationFactory{
    private final Clock clock;

    public MOperationFactory(Clock clock){
        this.clock=clock;
    }

    @Override
    public DataScan newScan(){
        return new MScan();
    }

    @Override
    public DataGet newGet(byte[] rowKey,DataGet previous){
        if(previous!=null){
            ((MGet)previous).setKey(rowKey);
            return previous;
        }else
            return new MGet(rowKey);
    }

    @Override
    public DataPut newPut(byte[] rowKey){
        return new MPut(rowKey);
    }

    @Override
    public DataPut newPut(ByteSlice slice){
        return new MPut(slice);
    }

    @Override
    public DataDelete newDelete(byte[] rowKey){
        return new MDelete(rowKey);
    }

    @Override
    public DataDelete newDelete(ByteSlice rowKey) {
        return new MDelete(rowKey);
    }

    @Override
    public DataCell newCell(byte[] key,byte[] family,byte[] qualifier,byte[] value){
        return newCell(key,family,qualifier,clock.currentTimeMillis(),value);
    }

    @Override
    public DataCell newCell(byte[] key,byte[] family,byte[] qualifier,long timestamp,byte[] value){
        CellType c;
        ByteComparator byteComparator=ByteComparisons.comparator();
        if(byteComparator.equals(SIConstants.COMMIT_TIMESTAMP_COLUMN_BYTES, qualifier)) {
           c = CellType.COMMIT_TIMESTAMP;
        } else if(byteComparator.equals(SIConstants.TOMBSTONE_COLUMN_BYTES, qualifier)) {
            if(byteComparator.equals(SIConstants.ANTI_TOMBSTONE_VALUE_BYTES, value)) {
                c = CellType.ANTI_TOMBSTONE;
            } else {
                c = CellType.TOMBSTONE;
            }
        } else if(byteComparator.equals(SIConstants.FIRST_OCCURRENCE_TOKEN_COLUMN_BYTES, qualifier)) {
            if(byteComparator.equals(SIConstants.DELETE_RIGHT_AFTER_FIRST_WRITE_VALUE_BYTES, value)) {
                c = CellType.DELETE_RIGHT_AFTER_FIRST_WRITE_TOKEN;
            } else {
                c = CellType.FIRST_WRITE_TOKEN;
            }
        } else if(byteComparator.equals(SIConstants.FK_COUNTER_COLUMN_BYTES, qualifier)) {
            c = CellType.FOREIGN_KEY_COUNTER;
        } else if(byteComparator.equals(SIConstants.PACKED_COLUMN_BYTES, qualifier)) {
            c = CellType.USER_DATA;
        } else {
            c = CellType.OTHER;
        }
        return new MCell(key, family, qualifier, timestamp, value, c);
    }

    @Override
    public void writeScan(DataScan scan,ObjectOutput out) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public DataScan readScan(ObjectInput in) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public DataResult newResult(List<DataCell> visibleColumns){
        return new MResult(visibleColumns);
    }

    @Override
    public DataPut toDataPut(KVPair kvPair,byte[] family,byte[] column,long timestamp){
        MPut mPut=new MPut(kvPair.rowKeySlice());
        mPut.addCell(family,column,timestamp,kvPair.getValue());
        return mPut;
    }
}
