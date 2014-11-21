package com.splicemachine.primitives;

import com.splicemachine.utils.UnsafeUtil;
import sun.misc.Unsafe;

/**
 * @author Scott Fines
 *         Date: 11/21/14
 */
public class BigEndianBits {
    /*short encoding/decoding*/
    public static byte[] toBytes(short i){
        byte[] newArray = new byte[2];
        toBytes(i,newArray,0);
        return newArray;
    }

    public static void toBytes(short i, byte[] destination, int pos){
        destination[pos] = (byte)(i>> 8);
        destination[pos+1] = (byte)(i    );
    }

    public static short toShort(byte[] data){
        return toShort(data,0);
    }

    public static short toShort(byte[] data, int offset){
        assert offset+2<=data.length: "Not enough bytes to decode an int!";
        short value = 0;
        value |= (data[offset] & 0xff)<< 8;
        value |= (data[offset+1] & 0xff);
        return value;
    }

    /*integer encoding/decoding methods*/
    public static byte[] toBytes(int i){
        byte[] newArray = new byte[4];
        toBytes(i,newArray,0);
        return newArray;
    }

    public static void toBytes(int i, byte[] destination, int pos){
        destination[pos  ] = (byte)(i>>24);
        destination[pos+1] = (byte)(i>>16);
        destination[pos+2] = (byte)(i>> 8);
        destination[pos+3] = (byte)(i    );
    }

    public static int toInt(byte[] data){
        return toInt(data,0);
    }

    public static int toInt(byte[] data, int offset){
        assert offset+4<=data.length: "Not enough bytes to decode an int!";
        int value = 0;
        value |= (data[offset] & 0xff)<<24;
        value |= (data[offset+1] & 0xff)<<16;
        value |= (data[offset+2] & 0xff)<< 8;
        value |= (data[offset+3] & 0xff);
        return value;
    }

    public static byte[] toBytes(long l){
        byte[] newArray = new byte[8];
        toBytes(l,newArray,0);
        return newArray;
    }

    public static void toBytes(long l,byte[] bytes,int pos){
        bytes[pos] =   (byte)(l>>56);
        bytes[pos+1] = (byte)(l>>48);
        bytes[pos+2] = (byte)(l>>40);
        bytes[pos+3] = (byte)(l>>32);
        bytes[pos+4] = (byte)(l>>24);
        bytes[pos+5] = (byte)(l>>16);
        bytes[pos+6] = (byte)(l>> 8);
        bytes[pos+7] = (byte)(l    );
    }

    public static long toLong(byte[] data){
        return toLong(data, 0);
    }

    public static long toLong(byte[] data, int offset){
        assert offset+8 <= data.length: "Not enough bytes to decode a long!";
        Unsafe unsafe = UnsafeUtil.unsafe();
        long value = unsafe.getLong(data,(long)offset+UnsafeUtil.byteArrayOffset());
        if(Bytes.isLittleEndian)
            return Long.reverseBytes(value);
        else return value;
    }


}
