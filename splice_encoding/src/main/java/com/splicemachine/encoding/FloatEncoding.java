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
