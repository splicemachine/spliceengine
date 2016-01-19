package com.splicemachine.si.data.hbase;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.storage.*;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of SDataLib that is specific to the HBase operation and result types.
 */
public class HDataLib implements SDataLib{
    private static final HDataLib INSTANCE= new HDataLib();

    @Override
    public byte[] newRowKey(Object... args){
        List<byte[]> bytes=new ArrayList<>();
        for(Object a : args){
            bytes.add(convertToBytes(a,a.getClass()));
        }
        return Bytes.concat(bytes);
    }

    @Override
    public byte[] encode(Object value){
        return convertToBytes(value,value.getClass());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T decode(byte[] value,Class<T> type){
        if(value==null){
            return null;
        }
        if(type.equals(Boolean.class)){
            return (T)(Boolean)Bytes.toBoolean(value);
        }else if(type.equals(Short.class)){
            return (T)(Short)Bytes.toShort(value);
        }else if(type.equals(Integer.class)){
            if(value.length<4) return (T)Integer.valueOf(-1);
            return (T)(Integer)Bytes.toInt(value);
        }else if(type.equals(Long.class)){
            return (T)(Long)Bytes.toLong(value);
        }else if(type.equals(Byte.class)){
            if(value.length>0){
                return (T)(Byte)value[0];
            }else{
                return null;
            }
        }else if(type.equals(String.class)){
            return (T)Bytes.toString(value);
        }
        throw new RuntimeException("unsupported type conversion: "+type.getName());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T decode(byte[] value,int offset,int length,Class<T> type){
        if(value==null){
            return null;
        }
        if(type.equals(Boolean.class)){
            return (T)(Boolean)Bytes.toBoolean(value,offset);
        }else if(type.equals(Short.class)){
            return (T)(Short)Bytes.toShort(value,offset);
        }else if(type.equals(Integer.class)){
            if(length<4) return (T)Integer.valueOf(-1);
            return (T)(Integer)Bytes.toInt(value,offset);
        }else if(type.equals(Long.class)){
            return (T)(Long)Bytes.toLong(value,offset);
        }else if(type.equals(Byte.class)){
            if(length>0){
                return (T)(Byte)value[offset];
            }else{
                return null;
            }
        }else if(type.equals(String.class)){
            return (T)Bytes.toString(value,offset,length);
        }
        throw new RuntimeException("unsupported type conversion: "+type.getName());
    }

    public static HDataLib instance(){
       return INSTANCE;
    }

    private Put newPut(ByteSlice key){
        return new Put(key.array(),key.offset(),key.length());
    }

    static byte[] convertToBytes(Object value,Class clazz){
        if(clazz==String.class){
            return Bytes.toBytes((String)value);
        }else if(clazz==Integer.class){
            return Bytes.toBytes((Integer)value);
        }else if(clazz==Short.class){
            return Bytes.toBytes((Short)value);
        }else if(clazz==Long.class){
            return Bytes.toBytes((Long)value);
        }else if(clazz==Boolean.class){
            return Bytes.toBytes((Boolean)value);
        }else if(clazz==byte[].class){
            return (byte[])value;
        }else if(clazz==Byte.class){
            return new byte[]{(Byte)value};
        }
        throw new RuntimeException("Unsupported class "+clazz.getName()+" for "+value);
    }

    @Override
    public DataResult newResult(List<DataCell> visibleColumns){
        List<Cell> actualCells = new ArrayList<>(visibleColumns.size());
        for(DataCell dc:visibleColumns){
            actualCells.add(((HCell)dc).unwrapDelegate());
        }
        Result r = Result.create(actualCells);
        return new HResult(r);
    }

    @Override
    public DataScan newDataScan(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public DataPut newDataPut(ByteSlice key){
        return new HPut(key);
    }

    @Override
    public DataPut toDataPut(KVPair kvPair,byte[] family,byte[] column,long timestamp){
        ByteSlice rowKey=kvPair.rowKeySlice();
        ByteSlice val=kvPair.valueSlice();
        Put put=newPut(rowKey);
        Cell kv=new KeyValue(rowKey.array(),rowKey.offset(),rowKey.length(),
                family,0,family.length,
                column,0,column.length,
                timestamp,
                KeyValue.Type.Put,val.array(),val.offset(),val.length());
        try{
            put.add(kv);
        }catch(IOException ignored){
                        /*
						 * This exception only appears to occur if the row in the Cell does not match
						 * the row that's set in the Put. This is definitionally not the case for the above
						 * code block, so we shouldn't have to worry about this error. As a result, throwing
						 * a RuntimeException here is legitimate
						 */
            throw new RuntimeException(ignored);
        }
        return new HPut(put);
    }

    @Override
    public DataDelete newDataDelete(byte[] key){
        return new HDelete(key);
    }

}
