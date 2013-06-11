package com.splicemachine.encoding;

import java.math.BigDecimal;

/**
 * Utilities for encoding various values using a sort-order preserving encoding
 * inspired by orderly (github.com/ndimiduk/orderly).
 *
 * @author Scott Fines
 * Created on: 6/8/13
 */
public final class Encoding {

    private Encoding(){} //can't construct me!

    /**
     * Encode a boolean in an ascending, sort-order-preserving encoding.
     *
     * Ascending in this case means that {@code true} comes before {@code false}.
     *
     * This is equivalent to calling {@link #encode(boolean, false)}.
     *
     * @param value the value to encode
     * @return an ascending sort-order-preserving encoding.
     */
    public static byte[] encode(boolean value){
        return ScalarEncoding.toBytes(value,false);
    }

    /**
     * Decode an ascending, sort-order-preserved encoding into a boolean. Ascending in this
     * case means that {@code true} sorts before {@code false}.
     *
     * This is equivalent to calling {@link #decodeBoolean(byte[],false)}.
     *
     * @param data ascending, sort-order-preserved encoding of a boolean.
     * @return {@code true} if {@code data} represents {@code true} in an ascending, order-preserving
     * encoding, false otherwise.
     */
    public static boolean decodeBoolean(byte[] data){
        return ScalarEncoding.toBoolean(data,false);
    }

    /**
     * Encode a boolean into an order-preserving encoding. The {@code desc} flag denotes
     * whether to sort in ascending or descending order.
     *
     * Ascending in this case means that {@code true} comes before {@code false}, while
     * descending reverses this order.
     *
     * @param value the value to encode
     * @param desc {@code true} if values are to be sorted in descending order, {@code false} for
     *                         ascending order encoding.
     * @return an order-preserving encoding respecting the ascending/descending order specified by {@code desc}.
     */
    public static byte[] encode(boolean value, boolean desc){
        return ScalarEncoding.toBytes(value,desc);
    }

    /**
     * Decode an order-preserving encoding into a boolean. The {@code desc} flag denotes
     * whether the encoding was sorted in ascending or descending order.
     *
     * Ascending in this case means that {@code true} comes before {@code false}, while
     * descending reverses this order.
     *
     * WARNING: In general, the encoding is not knowledgeless of the {@code desc} flag. This means that
     * the {@code desc} flag <em>must</em> be set the same as when the data was encoded,
     * or incorrect results may be returned. I.e. if {@code true} is encoded with {@code desc=true},
     * then the resulting bytes are decoded with {@code desc=false}, the returned value may be {@code false},
     * which is incorrect.
     *
     * @param data
     * @param desc
     * @return
     */
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
        return StringEncoding.getStringCopy(value,false);
    }

    public static String decodeStringInPlace(byte[] value){
        return StringEncoding.getString(value,false);
    }

    public static byte[] encode(String value,boolean desc){
        return StringEncoding.toBytes(value,desc);
    }

    public static String decodeString(byte[] value,boolean desc){
        return StringEncoding.getStringCopy(value,desc);
    }

    public static String decodeStringInPlace(byte[] value,boolean desc){
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
