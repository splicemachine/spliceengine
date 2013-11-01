package com.splicemachine.utils;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Scott Fines
 * Created on: 11/2/13
 */
public class MurmurHash {
    private static final int c1 = 0xcc9e2d51;
    private static final int c2 = 0x1b873593;

    public static int murmur3_32(byte[] bytes){
        return murmur3_32(bytes,0,bytes.length,0);
    }

    public static int murmur3_32(byte[] bytes,int offset, int length,int seed){
        int len = 0;
        int currentOffset = offset;
        int h1 = seed;
        while(length-len>=4){
            int k1 = littleEndianInt(bytes, currentOffset);
            currentOffset+=4;
            len+=4;

            k1 *=c1;
            k1 = Integer.rotateLeft(k1,15);
            k1 *=c2;

            h1^=k1;
            h1 = Integer.rotateLeft(h1,13);
            h1 = h1 *5 + 0xe6546b64;
        }

        int k1 = 0;
        switch(length-len){
            case 3:
                k1 ^= (bytes[currentOffset+2] & 0xFF)<<16;
                len++;
            case 2:
                k1 ^= ((bytes[currentOffset+1] & 0xFF) <<8);
                len++;
            case 1:
                k1 ^= bytes[currentOffset] & 0xFF;
                len++;
            default:
                k1 *= c1;
                k1 = Integer.rotateLeft(k1,15);
                k1 *= c2;
                h1 ^=k1;
        }

        h1 ^= len;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }

    private static int littleEndianInt(byte[] bytes, int offset) {
        byte b0 = bytes[offset];
        byte b1 = bytes[offset+1];
        byte b2 = bytes[offset+2];
        byte b3 = bytes[offset+3];
        return (((b3       ) << 24) |
                ((b2 & 0xff) << 16) |
                ((b1 & 0xff) <<  8) |
                ((b0 & 0xff)      ));
    }


}
