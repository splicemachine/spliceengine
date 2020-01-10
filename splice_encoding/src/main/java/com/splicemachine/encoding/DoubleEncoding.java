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

import java.nio.ByteBuffer;

/**
 * Encapsulates logic for Double encoding.
 */
class DoubleEncoding {

    static final byte[] NULL_DOUBLE_BYTES = {0,0};
    static final int NULL_DOUBLE_BYTES_LENGTH = 2;
    static final byte[] NULL_FLOAT_BYTES = {0, 0};
    static final int NULL_FLOAT_BYTES_LENGTH = 2;

    /**
     * Will generate an 8-byte, big-endian, sorted representation of the Double, in accordance
     * with IEEE 754, except that all NaNs will be coalesced into a single "canonical" NaN.
     *
     * Because it is not possible to
     *
     * The returned byte[] will <em>never</em> encode to all zeros. This makes an 8-byte zero field
     * available to use as a NULL indicator.
     *
     * @param value the double to encode
     * @param desc whether or not to encode in descending order
     * @return an 8-byte, big-endian, sorted encoding of the double.
     */
    public static byte[] toBytes(double value, boolean desc){
        long l = Double.doubleToLongBits(value);
        l = (l^ ((l >> Long.SIZE-1) | Long.MIN_VALUE))+1;

        byte[] bytes = Bytes.toBytes(l);
        if(desc) {
        	for(int i=0;i<bytes.length;i++){
        		bytes[i] ^= 0xff;
        	}
        }
        return bytes;
    }

    public static double toDouble(byte[] data, boolean desc){
        return toDouble(data, 0, desc);
    }

    public static double toDouble(ByteBuffer data,boolean desc){
        long l = data.asLongBuffer().get();
        if(desc)
            l ^= 0xffffffffffffffffl;

        l--;
        l ^= (~l >> Long.SIZE-1) | Long.MIN_VALUE;
        return Double.longBitsToDouble(l);
    }

    public static double toDouble(byte[] data, int offset,boolean desc){
    	byte[] val = data;
    	if(desc){
    		val = new byte[8];
    		System.arraycopy(data,offset,val,0,val.length);
    		for(int i=0;i<8;i++){
    			val[i] ^= 0xff;
    		}
    		offset=0;
    	}
    		
        long l = Bytes.toLong(val,offset);

        l--;
        l ^= (~l >> Long.SIZE-1) | Long.MIN_VALUE;
        return Double.longBitsToDouble(l);
    }

}
