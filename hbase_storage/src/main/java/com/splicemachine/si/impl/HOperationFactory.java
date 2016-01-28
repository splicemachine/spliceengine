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
