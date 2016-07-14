/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
