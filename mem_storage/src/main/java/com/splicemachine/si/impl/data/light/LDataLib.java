package com.splicemachine.si.impl.data.light;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;

import java.util.*;

public class LDataLib implements SDataLib{


    @Override
    public DataScan newDataScan(){
        return new MScan();
    }

    @Override
    public byte[] newRowKey(Object... args){
        StringBuilder builder=new StringBuilder();
        for(Object a : args){
            Object toAppend=a;
            if(a instanceof Short){
                toAppend=String.format("%1$06d",a);
            }else if(a instanceof Long){
                toAppend=String.format("%1$020d",a);
            }else if(a instanceof Byte){
                toAppend=String.format("%1$02d",a);
            }
            builder.append(toAppend);
        }
        return Bytes.toBytes(builder.toString());
    }

    @Override
    public byte[] encode(Object value){
        if(value instanceof String){
            return Bytes.toBytes(((String)value));
        }else if(value instanceof Boolean)
            if((Boolean)value){
                return new byte[]{0x01};
            }else return new byte[]{0x00};
        else if(value instanceof Integer)
            return Bytes.toBytes((Integer)value);
        else if(value instanceof Long)
            return Bytes.toBytes((Long)value);
        else if(value instanceof Byte)
            return new byte[]{(Byte)value};
        else if(value instanceof Short)
            return Bytes.toBytes((Short)value);
        else
            return (byte[])value;
    }


    @SuppressWarnings("unchecked")
    public <T> T decode(byte[] value,Class<T> type){
        if(!(value instanceof byte[])){
            return (T)value;
        }

        if(byte[].class.equals(type))
            return (T)value;
        if(String.class.equals(type))
            return (T)Bytes.toString(value);
        else if(Long.class.equals(type))
            return (T)(Long)Bytes.toLong(value);
        else if(Integer.class.equals(type)){
            if(value.length<4)
                return (T)Integer.valueOf(-1);
            return (T)(Integer)Bytes.toInt(value);
        }else if(Boolean.class.equals(type))
            return (T)(Boolean)Bytes.toBoolean(value);
        else if(Byte.class.equals(type))
            return (T)(Byte)value[0];
        else
            throw new RuntimeException("types don't match "+value.getClass().getName()+" "+type.getName()+" "+Arrays.toString(value));
    }

    public <T> T decode(byte[] value,int offset,int length,Class<T> type){
        if(!(value instanceof byte[])){
            return (T)value;
        }

        if(byte[].class.equals(type))
            return (T)value;
        if(String.class.equals(type))
            return (T)Bytes.toString(value,offset,length);
        else if(Long.class.equals(type))
            return (T)(Long)Bytes.toLong(value,offset);
        else if(Integer.class.equals(type)){
            if(length<4)
                return (T)Integer.valueOf(-1);
            return (T)(Integer)Bytes.toInt(value,offset);
        }else if(Boolean.class.equals(type))
            return (T)(Boolean)Bytes.toBoolean(value,offset);
        else if(Byte.class.equals(type))
            return (T)(Byte)value[offset];
        else
            throw new RuntimeException("types don't match "+value.getClass().getName()+" "+type.getName()+" "+Arrays.toString(value));
    }


    @Override
    public MResult newResult(List<DataCell> values){
        return new MResult(values);
    }

    private static boolean matchingColumn(DataCell c,byte[] family,byte[] qualifier){
        return c.matchesQualifier(family,qualifier);
    }

    @Override
    public DataPut newDataPut(ByteSlice key){
        return new MPut(key);
    }

    @Override
    public DataPut toDataPut(KVPair kvPair,byte[] family,byte[] column,long timestamp){
        ByteSlice rowKey=kvPair.rowKeySlice();
        ByteSlice val=kvPair.valueSlice();
        MPut put=new MPut(rowKey);
        DataCell kv=new MCell(rowKey.array(),rowKey.offset(),rowKey.length(),
                family,
                column,
                timestamp,
                val.array(),val.offset(),val.length(),CellType.USER_DATA);
        put.addCell(kv);
        return put;
    }

    @Override
    public DataDelete newDataDelete(byte[] key){
        return new MDelete(key);
    }

}