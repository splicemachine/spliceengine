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

package com.splicemachine.encoding;

import com.splicemachine.primitives.Bytes;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import java.nio.ByteBuffer;

/**
 * UTF-8 encodes Strings in such a way that NULL < "" < Character.MIN_CODE_POINT < aa < aaa < b< ba<...<
 * Character.MAX_CODE_POINT < ..., and does not use 0x00 (reserved for separators).
 *
 * Note that UTF-8 encoding is already lexicographically sorted in bytes, by design. Hence, all we
 * really have to do is remove 0x00 elements. Since UTF-8 never uses the values 0xff or 0xfe, so adding
 * 2 to every byte will suffice.
 *
 * To distinguish between empty strings and {@code null}, we use 0x01 to denote an empty string, but an empty byte[]
 * denotes {@code null}.
 *
 * @author Scott Fines
 * Created on: 6/7/13
 */
public class StringEncoding {

    /**
     * Wraps the Lucene UnicodeUtil.UTF16toUTF8 bytes serializatiom...
     */
    public static byte[] toBytes(String value, boolean desc){
        if(value==null) return Encoding.EMPTY_BYTE_ARRAY;
        if(value.length()==0){
            if(desc)
                return new byte[]{(byte)(0x01^0xff)};
            else
                return new byte[]{0x01};
        }

        //convert to UTF-8 encoding
        BytesRef result = new BytesRef();
        UnicodeUtil.UTF16toUTF8(value, 0, value.length(), result);
        byte[] returnArray = new byte[result.length];
        for(int i=0;i<result.length;i++){
            byte newD = (byte)(result.bytes[i+result.offset] + 2);
            if(desc)
                newD ^= 0xff; //reverse the sign bit so that data is reversed in 2's complement
            returnArray[i] = newD;
        }
        return returnArray;
    }

    public static int toBytes(String value, boolean desc, byte[] buffer, int offset){
        if(value==null || value.length()==0) return 0;

        //convert to UTF-8 encoding
        BytesRef result = new BytesRef();
        UnicodeUtil.UTF16toUTF8(value, 0, value.length(), result);
        for(int i=0;i<result.length;i++){
            byte newD = (byte)(result.bytes[i+result.offset] + 2);
            if(desc)
                newD ^= 0xff; //reverse the sign bit so that data is reversed in 2's complement
            buffer[offset+i] = newD;
        }
        return value.length();
    }

    @Deprecated
    public static byte[] toBytesOld(String value, boolean desc){
        if(value==null) return Encoding.EMPTY_BYTE_ARRAY;
        if(value.length()==0) return new byte[]{0x01};

        //convert to UTF-8 encoding
        byte[] data = Bytes.toBytes(value);
        for(int i=0;i<data.length;i++){
            byte newD = (byte)(data[i] + 2);
            if(desc)
                newD ^= 0xff; //reverse the sign bit so that data is reversed in 2's complement
            data[i] = newD;
        }
        return data;
    }
    /**
     * SIDE EFFECT WARNING: Transforms the passed in byte[] in place!
     *
     * @param data the string data to deserialize
     * @param desc
     * @return
     */
    public static String getString(byte[] data, boolean desc){
        if(data.length==0) return null;
        if(data.length==1){
            if(desc && (byte)(data[0])==(byte)0xFE) return "";
            else if(!desc && (byte)data[0]==(byte)0x01) return "";
        }

        for(int i=0;i<data.length;i++){
            byte datum = data[i];
            if(desc)
                datum ^= 0xff;
            data[i] = (byte)(datum-2);
        }
        return Bytes.toString(data);
    }

    public static String getStringCopy(byte[] data,int offset,int length, boolean desc){
        byte[] dataToCopy = new byte[length];
        System.arraycopy(data,offset,dataToCopy,0,length);
        return getString(dataToCopy,desc);
    }

    public static String getStringCopy(ByteBuffer buffer,boolean desc){
        byte[] dataToCopy = new byte[buffer.remaining()];
        buffer.get(dataToCopy);
        return getString(dataToCopy,desc);
    }

}
