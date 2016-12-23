/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
    public RecordScan newScan(){
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
    public DataCell newCell(byte[] key,byte[] family,byte[] qualifier,byte[] value){
        return newCell(key,family,qualifier,clock.currentTimeMillis(),value);
    }

    @Override
    public DataCell newCell(byte[] key,byte[] family,byte[] qualifier,long timestamp,byte[] value){
        CellType c;
        ByteComparator byteComparator=ByteComparisons.comparator();
        if(byteComparator.equals(SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,qualifier)){
           c = CellType.COMMIT_TIMESTAMP;
        }else if(byteComparator.equals(SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,qualifier)){
            if(byteComparator.equals(SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES,value))
                c =CellType.ANTI_TOMBSTONE;
            else c =CellType.TOMBSTONE;
        }else if(byteComparator.equals(SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES,qualifier))
            c = CellType.FOREIGN_KEY_COUNTER;
        else if(byteComparator.equals(SIConstants.PACKED_COLUMN_BYTES,qualifier))
            c = CellType.USER_DATA;
        else c = CellType.OTHER;
        return new MCell(key,family,qualifier,timestamp,value,c);
    }

    @Override
    public void writeScan(RecordScan scan, ObjectOutput out) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public RecordScan readScan(ObjectInput in) throws IOException{
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
