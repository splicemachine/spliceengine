package com.splicemachine.encoding;

import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;

/**
 * Encapsulates logic for Float encoding.
 */
class FloatEncoding {
    public static byte[] toBytes(float value,boolean desc){

        int j = Float.floatToIntBits(value);
        j = (j^((j>>Integer.SIZE-1) | Integer.MIN_VALUE))+1;

        if(desc)
            j^=0xffffffff;

        return Bytes.toBytes(j);
    }

    public static float toFloat(byte[] data, boolean desc){
        return toFloat(data,0,desc);
    }

    public static float toFloat(ByteBuffer data, boolean desc){
        int j = data.asIntBuffer().get();
        if(desc)
            j ^= 0xffffffff;

        j--;
        j ^= (~j >> Integer.SIZE-1)|Integer.MIN_VALUE;

        return Float.intBitsToFloat(j);
    }

    public static float toFloat(byte[] data, int offset,boolean desc){
        int j = Bytes.toInt(data,offset);
        if(desc)
            j ^= 0xffffffff;

        j--;
        j ^= (~j >> Integer.SIZE-1)|Integer.MIN_VALUE;

        return Float.intBitsToFloat(j);
    }
}
