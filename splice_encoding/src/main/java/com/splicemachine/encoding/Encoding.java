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

package com.splicemachine.encoding;

import com.splicemachine.primitives.Bytes;

import java.math.BigDecimal;

/**
 * Utilities for encoding various values using a sort-order preserving encoding
 * inspired by orderly (github.com/ndimiduk/orderly).
 *
 * Different data types are, in general, encoded using different strategies. This allows compact
 * representations that are custom for their particular type. For more information, see
 * the encoding descriptions for each type.
 *
 * One thing that is true for all encoding represented here, however, is that the byte value {@code 0x00}
 * is reserved by <em>all</em> encodings. This means that {@code 0x00} can be used as an effective terminator/
 * field separator in streams.
 *
 * @author Scott Fines
 * Created on: 6/8/13
 */
public final class Encoding {

    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private Encoding(){} //can't construct me!

    /**
     * Encode a boolean in an ascending, sort-order-preserving encoding.
     *
     * Ascending in this case means that {@code true} comes before {@code false}.
     *
     * This is equivalent to calling {@link #encode(boolean, boolean)} with {@code desc=false}.
     *
     * @param value the value to encode
     * @return an ascending sort-order-preserving encoding.
     */
    public static byte[] encode(boolean value){
        return ScalarEncoding.writeBoolean(value,false);
    }


    /**
     * Decode an ascending, sort-order-preserved encoding into a boolean. Ascending in this
     * case means that {@code true} sorts before {@code false}.
     *
     * This is equivalent to calling {@link #decodeBoolean(byte[],boolean)} with {@code desc=false}.
     *
     * @param data ascending, sort-order-preserved encoding of a boolean.
     * @return {@code true} if {@code data} represents {@code true} in an ascending, order-preserving
     * encoding, false otherwise.
     */
    public static boolean decodeBoolean(byte[] data,int offset){
        return ScalarEncoding.readBoolean(data,offset,false);
    }

    /**
     * Decode an ascending, sort-order-preserved encoding into a boolean. Ascending in this
     * case means that {@code true} sorts before {@code false}.
     *
     * This is equivalent to calling {@link #decodeBoolean(byte[],boolean)} with {@code desc=false}.
     *
     * @param data ascending, sort-order-preserved encoding of a boolean.
     * @return {@code true} if {@code data} represents {@code true} in an ascending, order-preserving
     * encoding, false otherwise.
     */
    public static boolean decodeBoolean(byte[] data){
        return ScalarEncoding.readBoolean(data,false);
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
        return ScalarEncoding.writeBoolean(value,desc);
    }

    /**
     * Decode an order-preserving encoding into a boolean. The {@code desc} flag denotes
     * whether the encoding was sorted in ascending or descending order.
     *
     * Ascending in this case means that {@code true} comes before {@code false}, while
     * descending reverses this order.
     *
     * WARNING: In general, the encoding is not immune to the {@code desc} flag. This means that
     * the {@code desc} flag <em>must</em> be set the same as when the data was encoded,
     * or incorrect results may be returned. I.e. if {@code true} is encoded with {@code desc=true},
     * then the resulting bytes are decoded with {@code desc=false}, the returned value may be {@code false},
     * which is incorrect.
     *
     * @param data the encoded boolean data
     * @param desc the sort-order of the encoding
     * @return an order-preserving encoding respecting the ascending/descending order specified by {@code desc}.
     */
    public static boolean decodeBoolean(byte[] data,int offset,boolean desc){
        return ScalarEncoding.readBoolean(data,offset,desc);
    }

    /**
     * Decode an order-preserving encoding into a boolean. The {@code desc} flag denotes
     * whether the encoding was sorted in ascending or descending order.
     *
     * Ascending in this case means that {@code true} comes before {@code false}, while
     * descending reverses this order.
     *
     * WARNING: In general, the encoding is not immune to the {@code desc} flag. This means that
     * the {@code desc} flag <em>must</em> be set the same as when the data was encoded,
     * or incorrect results may be returned. I.e. if {@code true} is encoded with {@code desc=true},
     * then the resulting bytes are decoded with {@code desc=false}, the returned value may be {@code false},
     * which is incorrect.
     *
     * @param data the encoded boolean data
     * @param desc the sort-order of the encoding
     * @return an order-preserving encoding respecting the ascending/descending order specified by {@code desc}.
     */
    public static boolean decodeBoolean(byte[] data,boolean desc){
        return ScalarEncoding.readBoolean(data,desc);
    }

    /**
     * Encode a byte into an ascending,order-preserving byte encoding.
     *
     * @param value the value to be encoded
     * @return an order-preserving byte encoding.
     */
    public static byte[] encode(byte value){
        return ScalarEncoding.writeLong(value,false);
//        return encode(new byte[]{value}); //have to encode this to avoid 0-entries
    }

    /**
     * Decode an ascending, order-preserving  byte encoding into a byte.
     *
     * @param data an order-preserving representation of a byte
     * @return the decoded byte.
     */
    public static byte decodeByte(byte[] data){
        return (byte)ScalarEncoding.readLong(data,false);
    }

    /**
     * Encode a byte into an order-preserving encoding, with order being determined
     * by the {@code desc} flag.
     *
     * WARNING: Encoding and decoding <em>must</em> use the same {@code desc} flag,
     * or else incorrect results may be returned.
     *
     * @param value the value to be encoded
     * @param desc {@code true} if descending order is desired, {@code false} for ascending.
     * @return an order-preserving representation of {@code value}
     */
    public static byte[] encode(byte value,boolean desc){
        return ScalarEncoding.writeLong(value,desc);
    }

    /**
     * Decode a byte from an order-preserving encoding, with ordering determined by the {@code desc} flag.
     *
     * WARNING: Encoding and decoding <em>must</em> use the same {@code desc} flag, or else incorrect results
     * may be returned.
     *
     * @param data the encoded data
     * @param desc {@code true} if the data is encoded in descending order, {@code false} otherwise.
     * @return the byte represented by {@code data}
     */
    public static byte decodeByte(byte[] data,boolean desc){
        return (byte)ScalarEncoding.readLong(data,desc);
    }

    public static byte decodeByte(byte[] data, int offset){
        return (byte)ScalarEncoding.readLong(data,offset,false);
    }

    public static byte decodeByte(byte[] data, int offset,boolean desc){
        return (byte)ScalarEncoding.readLong(data,offset,desc);
    }

    public static byte decodeByte(byte[] data, int offset,boolean desc,long[] lengthHolder){
        ScalarEncoding.readLong(data,offset,desc,lengthHolder);
        return (byte)lengthHolder[0];
    }

    /**
     * Encode a short into an ascending, order-preserving encoding.
     *
     * Equivalent to {@link #encode(short,boolean)} with {@code desc=false}.
     *
     * @param value the value to be encoded.
     * @return an ascending, order-preserving encoding of {@code value}
     */
    public static byte[] encode(short value){
        return ScalarEncoding.writeLong(value,false);
    }

    /**
     * Decode an ascending, order-preserving encoding into a short.
     *
     * Equivalent to {@link #decodeShort(byte[],boolean)} with {@code desc=false}.
     *
     * @param data the encoded data.
     * @return the short represented by {@code data}
     */
    public static short decodeShort(byte[] data){
        return (short)ScalarEncoding.readLong(data,false);
    }

    /**
     * Encode a short into an order-preserving encoding, with
     * ordering determined by {@code desc}.
     *
     * WARNING: Encoding and decoding <em>must</em> use the same
     * {@code desc} flag, or else incorrect results may be returned.
     *
     * @param value the value to encode
     * @param desc {@code true} if descending order is desired, {@code false} otherwise.
     * @return an order-preserving encoding of a short.
     */
    public static byte[] encode(short value,boolean desc){
        return ScalarEncoding.writeLong(value,desc);
    }


    /**
     * Decode a short into an order-preserving encoding, with
     * ordering determined by {@code desc}.
     *
     * WARNING: Encoding and decoding <em>must</em> use the same {@code desc}
     * flag, or else incorrect results may be returned.
     *
     * @param data an order-preserving encoding of a short
     * @param desc {@code true} if {@code data} was encoded in ascending order, {@code false} otherwise.
     * @return the short encoded by {@code data}
     */
    public static short decodeShort(byte[] data,boolean desc){
        return (short)ScalarEncoding.readLong(data,desc);
    }

    public static short decodeShort(byte[] data, int offset){
        return (short)ScalarEncoding.readLong(data,offset,false);
    }

    public static short decodeShort(byte[] data, int offset,boolean desc){
        return (short)ScalarEncoding.readLong(data,offset,desc);
    }

    public static short decodeShort(byte[] data, int offset,boolean desc,long[] lengthHolder){
        ScalarEncoding.readLong(data,offset,desc,lengthHolder);
        return (short)lengthHolder[0];
    }

    /**
     * Encode an integer into an ascending, order-preserving encoding.
     *
     * Equivalent to {@link #encode(int, boolean)} with {@code desc=false}.
     *
     * @param value the value to encode.
     * @return an ascending, order-preserving encoding of {@code value}.
     */
    public static byte[] encode(int value){
        return ScalarEncoding.writeLong(value,false);
    }

    public static int encode(int value, byte[] buffer, int offset){
        return encode(value,buffer,offset,false);
    }

    public static int encode(int value, byte[] buffer, int offset, boolean desc){
        return ScalarEncoding.writeLong(value,buffer,offset,desc);
    }

    public static int encodedLength(int value){
        return ScalarEncoding.encodedLength(value);
    }

    /**
     * Decode an ascending, order-preserving encoding into an integer.
     *
     * Equivalent to {@link #decodeInt(byte[], boolean)}.
     *
     * @param data an ascending, order-preserving encoding of an int.
     * @return the int represented by {@code data}
     */
    public static int decodeInt(byte[] data){
        return ScalarEncoding.readInt(data,false);
    }


    /**
     * Encode an integer into an order-preserving byte encoding, with {@code desc}
     * determining the order.
     *
     * WARNING: Encoding and Decoding values <em>must</em> be done with the same {@code desc}
     * flag set, or else incorrect results may be returned.
     *
     * @param value the value to be encoded
     * @param desc {@code true} if descending order is desired, {@code false} otherwise.
     * @return an order-preserving encoding of {@code value}
     */
    public static byte[] encode(int value,boolean desc){
        return ScalarEncoding.writeLong(value,desc);
    }

    /**
     * Decode an order-preserving byte encoding into an integer, with {@code desc} determining
     * the order.
     *
     * WARNING: Encoding and Decoding values <em>must</em> be done with the same {@code desc}
     * flag set, or else incorrect results may be returned.
     *
     * @param data the data to be decoded.
     * @param desc {@code true} if {@code data} was encoded in descending order, {@code false} otherwise.
     * @return the int represented by {@code data}
     */
    public static int decodeInt(byte[] data,boolean desc){
        return ScalarEncoding.readInt(data,desc);
    }

    public static int decodeInt(byte[] data, int offset){
        return (int)ScalarEncoding.readLong(data,offset,false);
    }

    public static int decodeInt(byte[] data, int offset,boolean desc){
        return (int)ScalarEncoding.readLong(data,offset,desc);
    }

    public static int decodeInt(byte[] data, int offset,boolean desc,long[] lengthHolder){
        ScalarEncoding.readLong(data,offset,desc,lengthHolder);
        return (int)lengthHolder[0];
    }

    /**
     * Encode a long into an ascending, order-preserving byte representation of {@code value}.
     *
     * Equivalent to {@link #encode(long, boolean)} with {@code desc=false}.
     *
     * @param value the value to be encoded
     * @return an ascending, order-preserving encoding of {@code value}
     */
    public static byte[] encode(long value){
        return ScalarEncoding.writeLong(value,false);
    }

    /**
     * Decode an ascending, order-preserving encoding into a long.
     *
     * Equivalent to {@link #decodeLong(byte[],boolean)} with {@code desc=false}.
     *
     * @param data an ascending, order-preserving encoding of a long.
     * @return the long represented by {@code data}.
     */
    public static long decodeLong(byte[] data){
        return ScalarEncoding.readLong(data,false);
    }

    /**
     * Encode a long into an order-preserving byte representation. {@code desc} is used
     * to determine whether that order is ascending or descending.
     *
     * WARNING: Encoding and decoding <em>must</em> be done with the same {@code desc} flag,
     * or else incorrect answers may be returned.
     *
     * @param value the long to encode
     * @param desc {@code true} if descending order is needed, {@code false} otherwise.
     * @return an order-preserving encoding of {@code value}
     */
    public static byte[] encode(long value,boolean desc){
        return ScalarEncoding.writeLong(value,desc);
    }

    /**
     * Decode an order-preserving encoded byte[] into a long. The {@code desc} flag is used
     * to determine whether the encoding is ascending or descending.
     *
     * WARNING: Encoding and decoding <em>must</em> be done with the same {@code desc} flag,
     * or else incorrect answers may be returned
     *
     * @param data the data to decode
     * @param desc {@code true} if {@code data} is encoded in descending order, {@code false } otherwise.
     * @return the long represented by {@code data}
     */
    public static long decodeLong(byte[] data,boolean desc){
        return ScalarEncoding.readLong(data,desc);
    }

    public static long decodeLong(byte[] data, int offset,boolean desc){
        return ScalarEncoding.readLong(data,offset,desc);
    }

    public static void decodeLongWithLength(byte[] data, int offset, boolean  desc, long[] valueAndLength){
        ScalarEncoding.readLong(data,offset,desc,valueAndLength);
    }

    /**
     * Encode a float into an ascending, order-preserving byte[].
     *
     * Equivalent to {@link #encode(float, boolean)} with {@code desc=false}.
     *
     * @param value the value to encode
     * @return an ascending, order-preserving encoding of {@code value}.
     */
    public static byte[] encode(float value){
        return FloatEncoding.toBytes(value, false);
    }

    /**
     * Decode an ascending, order-preserving byte[] into a float.
     *
     * Equivalent to {@link #decodeFloat(byte[],boolean)} with {@code desc=false}.
     *
     * @param data an ascending, order-preserving encoding of a float.
     * @return the float represented by {@code data}.
     */
    public static float decodeFloat(byte[] data){
        return FloatEncoding.toFloat(data, false);
    }

    public static float decodeFloat(byte[] data, int offset){
        return FloatEncoding.toFloat(data, offset, false);
    }

    /**
     * Encode a float into an order-preserving byte representation. The flag {@code desc} determines
     * whether the encoding is in ascending or descending order.
     *
     * WARNING: Encoding and Decoding <em>must</em> be performed with the same {@code desc} flag, or else
     * incorrect results may be returned.
     *
     * @param value the float to encode
     * @param desc {@code true} if descending order is desired, {@code false} otherwise.
     * @return an order-preserving encoding of {@code value}
     */
    public static byte[] encode(float value,boolean desc){
        return FloatEncoding.toBytes(value, desc);
    }

    /**
     * Decode an order-preserving byte representation into a float. The flag {@code desc} determines
     * whether {@code data} was encoded in ascending or descending order.
     *
     * WARNING: Encoding and Decoding <em>must</em> be performed with the same {@code desc} flag, or else
     * incorrect results may be returned.
     *
     * @param data the data to be decoded
     * @param desc {@code true} if  {@code data} was encoded in descending order, {@code false } otherwise.
     * @return the float represented by {@code data}
     */
    public static float decodeFloat(byte[] data,boolean desc){
        return FloatEncoding.toFloat(data, desc);
    }

    public static float decodeFloat(byte[] data,int offset,boolean desc){
        return FloatEncoding.toFloat(data, offset, desc);
    }

    /**
     * Encode a double into an ascending, order-preserving byte[].
     *
     * Equivalent to {@link #encode(double, boolean)} with {@code desc=false}.
     *
     * @param value the value to encode
     * @return an ascending, order-preserving encoding of {@code value}.
     */
    public static byte[] encode(double value){
        return DoubleEncoding.toBytes(value, false);
    }

    /**
     * Decode an ascending, order-preserving byte[] into a double.
     *
     * Equivalent to {@link #decodeFloat(byte[],boolean)} with {@code desc=false}.
     *
     * @param data an ascending, order-preserving encoding of a double.
     * @return the double represented by {@code data}.
     */
    public static double decodeDouble(byte[] data){
        return DoubleEncoding.toDouble(data, false);
    }

    public static double decodeDouble(byte[] data,int offset){
        return DoubleEncoding.toDouble(data, offset, false);
    }

    /**
     * Encode a double into an order-preserving byte representation. The flag {@code desc} determines
     * whether the encoding is in ascending or descending order.
     *
     * WARNING: Encoding and Decoding <em>must</em> be performed with the same {@code desc} flag, or else
     * incorrect results may be returned.
     *
     * @param value the double to encode
     * @param desc {@code true} if descending order is desired, {@code false} otherwise.
     * @return an order-preserving encoding of {@code value}
     */
    public static byte[] encode(double value,boolean desc){
        return DoubleEncoding.toBytes(value, desc);
    }

    /**
     * Decode an order-preserving byte representation into a double. The flag {@code desc} determines
     * whether {@code data} was encoded in ascending or descending order.
     *
     * WARNING: Encoding and Decoding <em>must</em> be performed with the same {@code desc} flag, or else
     * incorrect results may be returned.
     *
     * @param data the data to be decoded
     * @param desc {@code true} if  {@code data} was encoded in descending order, {@code false } otherwise.
     * @return the double represented by {@code data}
     */
    public static double decodeDouble(byte[] data,boolean desc){
        return DoubleEncoding.toDouble(data, desc);
    }

    public static double decodeDouble(byte[] data,int offset,boolean desc){
        return DoubleEncoding.toDouble(data, offset, desc);
    }
 
    /**
     * Encode a BigDecimal into an ascending, order-preserving byte[].
     *
     * Equivalent to {@link #encode(BigDecimal, boolean)} with {@code desc=false}.
     *
     * @param value the value to encode
     * @return an ascending, order-preserving encoding of {@code value}.
     */
    public static byte[] encode(BigDecimal value){
        return BigDecimalEncoding.toBytes(value, false);
    }

    /**
     * Decode an ascending, order-preserving byte[] into a BigDecimal.
     *
     * Equivalent to {@link #decodeFloat(byte[],boolean)} with {@code desc=false}.
     *
     * @param data an ascending, order-preserving encoding of a BigDecimal.
     * @return the BigDecimal represented by {@code data}.
     */
    public static BigDecimal decodeBigDecimal(byte[] data){
        return BigDecimalEncoding.toBigDecimal(data, false);
    }

    /**
     * Encode a BigDecimal into an order-preserving byte representation. The flag {@code desc} determines
     * whether the encoding is in ascending or descending order.
     *
     * WARNING: Encoding and Decoding <em>must</em> be performed with the same {@code desc} flag, or else
     * incorrect results may be returned.
     *
     * @param value the BigDecimal to encode
     * @param desc {@code true} if descending order is desired, {@code false} otherwise.
     * @return an order-preserving encoding of {@code value}
     */
    public static byte[] encode(BigDecimal value,boolean desc){
        return BigDecimalEncoding.toBytes(value, desc);
    }

    /**
     * Decode an order-preserving byte representation into a BigDecimal. The flag {@code desc} determines
     * whether {@code data} was encoded in ascending or descending order.
     *
     * WARNING: Encoding and Decoding <em>must</em> be performed with the same {@code desc} flag, or else
     * incorrect results may be returned.
     *
     * @param data the data to be decoded
     * @param desc {@code true} if  {@code data} was encoded in descending order, {@code false } otherwise.
     * @return the BigDecimal represented by {@code data}
     */
    public static BigDecimal decodeBigDecimal(byte[] data,boolean desc){
        return BigDecimalEncoding.toBigDecimal(data, desc);
    }

    public static BigDecimal decodeBigDecimal(byte[] data,int offset,int length,boolean desc){
        return BigDecimalEncoding.toBigDecimal(data, offset, length, desc);
    }

    /**
     * Encode a String into an ascending, order-preserving byte[].
     *
     * Equivalent to {@link #encode(String, boolean)} with {@code desc=false}.
     *
     * @param value the value to encode
     * @return an ascending, order-preserving encoding of {@code value}.
     */
    public static byte[] encode(String value){
        return StringEncoding.toBytes(value, false);
    }

    public static int encode(String value, byte[] buffer, int offset){
        return StringEncoding.toBytes(value, false, buffer, offset);
    }

    public static int encode(String value, byte[] buffer, int offset,boolean desc){
        return StringEncoding.toBytes(value,desc,buffer,offset);
    }

    /**
     * Decode an ascending, order-preserving byte[] into a String.
     *
     * Equivalent to {@link #decodeFloat(byte[],boolean)} with {@code desc=false}.
     *
     * @param value an ascending, order-preserving encoding of a String.
     * @return the String represented by {@code data}.
     */
    public static String decodeString(byte[] value){
        return StringEncoding.getStringCopy(value, 0, value.length, false);
    }

    /**
     * Encode a String into an order-preserving byte representation. The flag {@code desc} determines
     * whether the encoding is in ascending or descending order.
     *
     * WARNING: Encoding and Decoding <em>must</em> be performed with the same {@code desc} flag, or else
     * incorrect results may be returned.
     *
     * Strings are formatted using UTF-8 encoding, then shifted by two positions to avoid using the values
     * {@code 0x00} or {@code 0x01}.
     *
     * an empty String is serialized into the byte 0x01, and {@code null} will return and empty byte[].
     *
     * @param value the String to encode
     * @param desc {@code true} if descending order is desired, {@code false} otherwise.
     * @return an order-preserving encoding of {@code value}
     */
    public static byte[] encode(String value,boolean desc){
        return StringEncoding.toBytes(value, desc);
    }

    /**
     * Decode an order-preserving byte representation into a String. The flag {@code desc} determines
     * whether {@code data} was encoded in ascending or descending order.
     *
     * WARNING: Encoding and Decoding <em>must</em> be performed with the same {@code desc} flag, or else
     * incorrect results may be returned.
     *
     * @param value the data to be decoded
     * @param desc {@code true} if  {@code data} was encoded in descending order, {@code false } otherwise.
     * @return the String represented by {@code data}
     */
    public static String decodeString(byte[] value,boolean desc){
        return StringEncoding.getStringCopy(value,0,value.length,desc);
    }

    public static String decodeString(byte[] value,int offset,int length,boolean desc){
        return StringEncoding.getStringCopy(value, offset, length, desc);
    }

    /**
     * Encode a byte[] into a byte[] in such a manner as to remove {@code 0x00} from the array (if present).
     *
     * @param data the data to encode
     * @return an encoding of {@code data} which does not have {@code 0x00} anywhere in it.
     */
    public static byte[] encode(byte[] data){
        return ByteEncoding.encode(data,false);
    }

    /**
     * Decode a {@code 0x00}-free byte[] into it's original format.
     *
     * @param encodedData the data to decode
     * @return the byte[] represented by {@code encodedData}.
     */
    public static byte[] decodeBytes(byte[] encodedData){
        return ByteEncoding.decode(encodedData,false);
    }

    /**
     * Encode byte[] into a byte[] in such a manner as to remove {@code 0x00}. The flag {@code desc} is
     * used to order the bytes in ascending or descending order.
     *
     * @param data the data to encode
     * @param desc {@code true} if the encoding should sort in reverse order, {@code false} otherwise.
     * @return an encoded representation of {@code data}
     */
    public static byte[] encode(byte[] data, boolean desc){
        return ByteEncoding.encode(data,desc);
    }

    /**
     * Decode a {@code 0x00}-free byte[] into it's original format. The flag {@code desc} is used to determine
     * if the bytes were encoded in ascending or descending order.
     *
     * @param encodedData the encoded representation.
     * @param desc {@code true} if {@code encodedData} is in descending order, {@code false} otherwise.
     * @return the original, unencoded byte[] represented by {@code encodedData}.
     */
    public static byte[] decodeBytes(byte[] encodedData,boolean desc){
        return ByteEncoding.decode(encodedData,desc);
    }

    public static byte[] decodeBytes(byte[] encodedData,int offset,int length,boolean desc){
        return ByteEncoding.decode(encodedData,offset,length,desc);
    }

    public static byte[] decodeBytesUnsortd(byte[] encodedData,int offset,int length){
        return ByteEncoding.decodeUnsorted(encodedData,offset,length);
    }

    public static byte[] encodeBytesUnsorted(byte[] dataToEncode){
        return ByteEncoding.encodeUnsorted(dataToEncode);
    }

    public static byte[] encodedNullDouble() {
        return DoubleEncoding.NULL_DOUBLE_BYTES;
    }

    public static byte[] encodedNullFloat() {
        return DoubleEncoding.NULL_FLOAT_BYTES;
    }
    
    public static int encodedNullDoubleLength() {
        return DoubleEncoding.NULL_DOUBLE_BYTES_LENGTH;
    }

   
    public static int encodedNullFloatLength() {
        return DoubleEncoding.NULL_FLOAT_BYTES_LENGTH;
    }
    

    public static void main(String... args) throws Exception{
				byte b = (byte)0x01;
				System.out.println(Bytes.toStringBinary(encode(104300001L, true)));
				System.out.println(Bytes.toLong(Bytes.toBytesBinary("\\x00\\x00\\x00\\x00:\\xD6?\\x00")));

    }

		public static boolean isNullDOuble(byte[] data, int offset, int length) {
				return length == DoubleEncoding.NULL_DOUBLE_BYTES.length;
		}
		public static boolean isNullFloat(byte[] data, int offset,int length){
				return length == DoubleEncoding.NULL_FLOAT_BYTES.length;
		}

    public static byte[] encodeBytesUnsorted(byte[] array, int offset, int length) {
        return ByteEncoding.encodeUnsorted(array,offset,length);
    }

}
