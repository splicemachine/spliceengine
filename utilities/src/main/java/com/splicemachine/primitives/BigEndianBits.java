/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

    public static String toHex(byte[] value, int offset, int length){
        int remaining = length;
        StringBuilder sb = new StringBuilder();
        int pos = offset;
        while(remaining>=8){
            long v = BigEndianBits.toLong(value, pos);
            sb = sb.append(Long.toHexString(v));
            remaining-=8;
            pos+=8;
        }
        if(remaining==0) return sb.toString();
        if(remaining>=4){
            //can append an integer
            int v = BigEndianBits.toInt(value,pos);
            sb = sb.append(Integer.toHexString(v));
            pos+=4;
            remaining-=4;
        }
        if(remaining==0) return sb.toString();

        byte b0 = 0,b1 = 0,b2 = 0;
        switch(remaining){
            case 3:
                b2 = value[pos+2];
            case 2:
                b1 = value[pos+1];
            case 1:
                b0 = value[pos];
            default:
        }
        int v =  (b0 & 0xff)<<24
                |(b1 & 0xff)<<16
                |(b2 & 0xff)<<8;
        sb = sb.append(Integer.toHexString(v));
        return sb.toString();
    }

    public static byte[] fromHex(String hex){
        int remaining = hex.length();
        int pos = 0;
        int strPos = 0;
        byte[] dataSize = new byte[remaining/2]; //there are 2 characters for each byte
        while(remaining>=16){
            long next = Long.parseLong(hex.substring(strPos,strPos+16),16);
            BigEndianBits.toBytes(next,dataSize,pos);
            pos+=8;
            strPos+=16;
            remaining-=16;
        }
        if(remaining==0) return dataSize;
        while(remaining>=8){
            int next = (int)(Long.parseLong(hex.substring(strPos,strPos+8),16));
            BigEndianBits.toBytes(next,dataSize,pos);
            pos+=4;
            strPos+=8;
            remaining-=8;
        }
        if(remaining==0) return dataSize;
        int n = Integer.parseInt(hex.substring(strPos),16);
        /*
         * we only have part of an integer at this point, so we have to put it in by hand
         */
        byte b0=0,b1=0,b2=0;
        switch(remaining){
            case 6:
                b0 = (byte)((n>>24) & 0xff);
                dataSize[pos] = b0;
                pos++;
            case 4:
                b1 = (byte)((n>>16) & 0xff);
                dataSize[pos] = b1;
                pos++;
            case 2:
                b2 = (byte)((n>>8) & 0xff);
                dataSize[pos] = b2;
            default:
        }
        return dataSize;
    }

}
