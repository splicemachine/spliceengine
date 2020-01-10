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

package com.splicemachine.primitives;

import com.splicemachine.utils.UnsafeUtil;

/**
 * @author Scott Fines
 *         Date: 11/21/14
 */
public class LittleEndianBits {

    public static byte[] toBytes(long l){
        byte[] bytes = new byte[8];
        toBytes(l,bytes,0);
        return bytes;
    }

    public static void toBytes(long l,byte[] bytes,int offset){
        assert offset+8 <= bytes.length: "Not enough bytes to encode a long!";
        bytes[offset+7] = (byte)((l & 0xff00000000000000l) >>>56);
        bytes[offset+6] = (byte)((l & 0x00ff000000000000l) >>>48);
        bytes[offset+5] = (byte)((l & 0x0000ff0000000000l) >>>40);
        bytes[offset+4] = (byte)((l & 0x000000ff00000000l) >>>32);
        bytes[offset+3] = (byte)((l & 0x00000000ff000000l) >>>24);
        bytes[offset+2] = (byte)((l & 0x0000000000ff0000l) >>>16);
        bytes[offset+1] = (byte)((l & 0x000000000000ff00l) >>>8);
        bytes[offset] = (byte)((l & 0x00000000000000ffl));
    }


    public static long toLong(byte[] data){
        return toLong(data,0);
    }
    public static long toLong(byte[] data, int pos){
        assert pos+8 <= data.length: "Not enough bytes to decode a long!";
        long l = UnsafeUtil.unsafe().getLong(data,(long)pos+ UnsafeUtil.byteArrayOffset());
        if(Bytes.isLittleEndian) return l;
        else return Long.reverseBytes(l);
    }

    public static long toLong(char[] data, int pos){
        assert pos+8 <= data.length: "Not enough bytes to decode a long!";
        char b1 = data[pos];
        char b2 = data[pos+1];
        char b3 = data[pos+2];
        char b4 = data[pos+3];
        char b5 = data[pos+4];
        char b6 = data[pos+5];
        char b7 = data[pos+6];
        char b8 = data[pos+7];

        return  (((long)b8       )<<56) |
                (((long)b7 & 0xff)<<48) |
                (((long)b6 & 0xff)<<40) |
                (((long)b5 & 0xff)<<32) |
                (((long)b4 & 0xff)<<24) |
                (((long)b3 & 0xff)<<16) |
                (((long)b2 & 0xff)<<8) |
                (((long)b1 & 0xff));
    }


    /*int encoding/decoding*/
    public static byte[] toBytes(int i){
        byte[] bytes = new byte[4];
        toBytes(i,bytes,0);
        return bytes;
    }

    public static void toBytes(int l,byte[] bytes,int offset){
        assert offset+4 <= bytes.length: "Not enough bytes to encode an int !";
        bytes[offset+3] = (byte)((l & 0xff000000l) >>>24);
        bytes[offset+2] = (byte)((l & 0x00ff0000l) >>>16);
        bytes[offset+1] = (byte)((l & 0x0000ff00l) >>>8);
        bytes[offset]   = (byte)((l & 0x000000ffl));
    }


    public static int toInt(byte[] data){
        return toInt(data,0);
    }

    public static int toInt(byte[] data, int pos){
        assert pos+4 <= data.length: "Not enough bytes to decode an int!";
        byte b1 = data[pos];
        byte b2 = data[pos+1];
        byte b3 = data[pos+2];
        byte b4 = data[pos+3];

        return  ((b4 & 0xff)<<24) |
                ((b3 & 0xff)<<16) |
                ((b2 & 0xff)<< 8) |
                ((b1 & 0xff)    );
    }

    public static int toInt(char[] bytes, int offset) {
        char b0 = bytes[offset];
        char b1 = bytes[offset+1];
        char b2 = bytes[offset+2];
        char b3 = bytes[offset+3];
        return (((b3       ) << 24) |
                ((b2 & 0xff) << 16) |
                ((b1 & 0xff) <<  8) |
                ((b0 & 0xff)      ));
    }

    /*short encoding/decoding*/
    public static byte[] toBytes(short i){
        byte[] bytes = new byte[2];
        toBytes(i,bytes,0);
        return bytes;
    }

    public static void toBytes(short l,byte[] bytes,int offset){
        assert offset+2 <= bytes.length: "Not enough bytes to encode an int !";
        bytes[offset+1] = (byte)((l & 0xff00l) >>>8);
        bytes[offset]   = (byte)((l & 0x00ffl));
    }


    public static short toShort(byte[] data){
        return toShort(data, 0);
    }

    public static short toShort(byte[] data, int pos){
        assert pos+2 <= data.length: "Not enough bytes to decode an int!";
        byte b1 = data[pos];
        byte b2 = data[pos+1];

        int i = (((short) b2 & 0xff) << 8) |
                (((short) b1 & 0xff));
        return (short)(i & 0xffff);
    }

}
