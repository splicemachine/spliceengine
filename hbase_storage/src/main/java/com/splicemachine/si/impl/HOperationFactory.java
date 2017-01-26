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

package com.splicemachine.si.impl;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 1/18/16
 */
public class HOperationFactory implements OperationFactory{
    public static final HOperationFactory INSTANCE = new HOperationFactory();

    private HOperationFactory(){}

    @Override
    public DataScan newScan(){
        return new HScan();
    }

    @Override
    public DataGet newGet(byte[] rowKey,DataGet previous){
        if(previous!=null){
            ((HGet)previous).reset(rowKey);
            return previous;
        }else
            return new HGet(rowKey);
    }

    @Override
    public DataPut newPut(ByteSlice slice){
        return new HPut(slice);
    }

    @Override
    public DataResult newResult(List<DataCell> visibleColumns){
        List<Cell> cells = new ArrayList<>(visibleColumns.size());
        for(DataCell dc:visibleColumns){
            cells.add(((HCell)dc).unwrapDelegate());
        }
        return new HResult(Result.create(cells));
    }

    @Override
    public DataPut newPut(byte[] rowKey){
        return new HPut(rowKey);
    }

    @Override
    public DataDelete newDelete(byte[] rowKey){
        return new HDelete(rowKey);
    }

    @Override
    public DataCell newCell(byte[] key,byte[] family,byte[] qualifier,byte[] value){
        KeyValue kv = new KeyValue(key,family,qualifier,value);
        return new HCell(kv);
    }

    @Override
    public DataCell newCell(byte[] key,byte[] family,byte[] qualifier,long timestamp,byte[] value){
        KeyValue kv = new KeyValue(key,family,qualifier,timestamp,value);
        return new HCell(kv);
    }

    @Override
    public void writeScan(DataScan scan,ObjectOutput out) throws IOException{
        Scan delegate=((HScan)scan).unwrapDelegate();

        byte[] bytes=ProtobufUtil.toScan(delegate).toByteArray();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    public DataScan readScan(ObjectInput in) throws IOException{
        byte[] bytes = new byte[in.readInt()];
        in.readFully(bytes);
        ClientProtos.Scan scan=ClientProtos.Scan.parseFrom(bytes);
        return new HScan(ProtobufUtil.toScan(scan));
    }

    @Override
    public DataPut toDataPut(KVPair kvPair,byte[] family,byte[] column,long timestamp){
        HPut hp = new HPut(kvPair.getRowKey());
        hp.addCell(family,column,timestamp,kvPair.getValue());
        return hp;
    }
}
