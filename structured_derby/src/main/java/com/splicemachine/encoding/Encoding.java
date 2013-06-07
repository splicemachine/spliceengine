package com.splicemachine.encoding;

import java.math.BigDecimal;

/**
 * @author Scott Fines
 * Created on: 6/8/13
 */
public final class Encoding {

    private Encoding(){} //can't construct me!

    public static byte[] encode(boolean value){
        return ScalarEncoding.toBytes(value,false);
    }

    public static boolean decodeBoolean(byte[] data){
        return ScalarEncoding.toBoolean(data,false);
    }

    public static byte[] encode(boolean value, boolean desc){
        return ScalarEncoding.toBytes(value,desc);
    }

    public static boolean decodeBoolean(byte[] data,boolean desc){
        return ScalarEncoding.toBoolean(data, desc);
    }

    public static byte[] encode(byte value){
        return ScalarEncoding.toBytes(value,false);
    }

    public static byte decodeByte(byte[] data){
        return (byte)ScalarEncoding.toLong(data, false);
    }

    public static byte[] encode(byte value,boolean desc){
        return ScalarEncoding.toBytes(value,desc);
    }

    public static byte decodeByte(byte[] data,boolean desc){
        return (byte)ScalarEncoding.toLong(data, desc);
    }

    public static byte[] encode(short value){
        return ScalarEncoding.toBytes(value,false);
    }

    public static short decodeShort(byte[] data){
        return (short)ScalarEncoding.toLong(data,false);
    }

    public static byte[] encode(short value,boolean desc){
        return ScalarEncoding.toBytes(value,desc);
    }

    public static short decodeShort(byte[] data,boolean desc){
        return (short)ScalarEncoding.toLong(data, desc);
    }

    public static byte[] encode(int value){
        return ScalarEncoding.toBytes(value,false);
    }

    public static int decodeInt(byte[] data){
        return (int)ScalarEncoding.toLong(data,false);
    }

    public static byte[] encode(int value,boolean desc){
        return ScalarEncoding.toBytes(value,desc);
    }

    public static int decodeInt(byte[] data,boolean desc){
        return (int)ScalarEncoding.toLong(data,desc);
    }

    public static byte[] encode(long value){
        return ScalarEncoding.toBytes(value,false);
    }

    public static long decodeLong(byte[] data){
        return ScalarEncoding.toLong(data,false);
    }

    public static byte[] encode(long value,boolean desc){
        return ScalarEncoding.toBytes(value,desc);
    }

    public static long decodeLong(byte[] data,boolean desc){
        return ScalarEncoding.toLong(data,desc);
    }

    public static byte[] encode(float value){
        return DecimalEncoding.toBytes(value,false);
    }

    public static float decodeFloat(byte[] data){
        return DecimalEncoding.toFloat(data,false);
    }

    public static byte[] encode(float value,boolean desc){
        return DecimalEncoding.toBytes(value,desc);
    }

    public static float decodeFloat(byte[] data,boolean desc){
        return DecimalEncoding.toFloat(data,desc);
    }

    public static byte[] encode(double value){
        return DecimalEncoding.toBytes(value,false);
    }

    public static double decodeDouble(byte[] data){
        return DecimalEncoding.toDouble(data,false);
    }

    public static byte[] encode(double value,boolean desc){
        return DecimalEncoding.toBytes(value,desc);
    }

    public static double decodeDouble(byte[] data,boolean desc){
        return DecimalEncoding.toDouble(data,desc);
    }

    public static byte[] encode(BigDecimal value){
        return DecimalEncoding.toBytes(value,false);
    }

    public static BigDecimal decodeBigDecimal(byte[] data){
        return DecimalEncoding.toBigDecimal(data,false);
    }

    public static byte[] encode(BigDecimal value,boolean desc){
        return DecimalEncoding.toBytes(value,desc);
    }

    public static BigDecimal decodeBigDecimal(byte[] data,boolean desc){
        return DecimalEncoding.toBigDecimal(data,desc);
    }

    public static byte[] encodeNullFree(String value){
        return StringEncoding.toNonNullBytes(value,false);
    }

    public static byte[] encodeNullFree(String value, boolean desc){
        return StringEncoding.toNonNullBytes(value,desc);
    }

    public static byte[] encode(String value){
        return StringEncoding.toBytes(value,false);
    }

    public static String decodeString(byte[] value){
        return StringEncoding.getString(value,false);
    }

    public static byte[] encode(String value,boolean desc){
        return StringEncoding.toBytes(value,desc);
    }

    public static String decodeString(byte[] value,boolean desc){
        return StringEncoding.getString(value,desc);
    }

    public static byte[] encode(byte[] data){
        return ByteEncoding.encode(data,false);
    }

    public static byte[] decodeBytes(byte[] encodedData){
        return ByteEncoding.decode(encodedData,false);
    }

    public static byte[] encode(byte[] data, boolean desc){
        return ByteEncoding.encode(data,desc);
    }

    public static byte[] decodeBytes(byte[] encodedData,boolean desc){
        return ByteEncoding.decode(encodedData,desc);
    }
}
