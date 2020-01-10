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

import com.splicemachine.utils.ByteSlice;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;

/**
 * A raw encoder to encode data into a single, automatically expanding byte array.
 *
 * @author Scott Fines
 *         Date: 1/19/15
 */
public class ExpandingEncoder {
    private byte[] buffer;
    private int currentOffset;

    private final float resizeFactor;

    public ExpandingEncoder(int initialSize, float resizeFactor){
        this.buffer = new byte[initialSize];
        this.resizeFactor = resizeFactor;
        this.currentOffset = 0;
    }

    public ExpandingEncoder(int initialSize) {
        this(initialSize,1.5f);
    }

    public ExpandingEncoder(float resizeFactor) {
        this(10,resizeFactor);
    }

    public ExpandingEncoder encode(byte value){
        ensureCapacity(Encoding.encodedLength(value));
        currentOffset+=Encoding.encode(value,buffer,currentOffset);
        return this;
    }

    public ExpandingEncoder rawEncode(byte value){
        ensureCapacity(1);
        buffer[currentOffset] = value;
        currentOffset++;
        return this;
    }

    public ExpandingEncoder encode(short value){
        ensureCapacity(Encoding.encodedLength(value));
        currentOffset+=Encoding.encode(value,buffer,currentOffset);
        return this;
    }

    public ExpandingEncoder encode(int value){
        ensureCapacity(Encoding.encodedLength(value));
        currentOffset+=Encoding.encode(value,buffer,currentOffset);
        return this;
    }

    public ExpandingEncoder encode(String value){
        ensureCapacity(value.length());
        /*
         * We add an extra 0x00 here to delimit the end of a String, so that
         * we can easily parse  list of Strings easily.
         */
        currentOffset+=Encoding.encode(value,buffer,currentOffset)+1;
        return this;
    }

    public ExpandingEncoder rawEncode(byte[] value){
        return rawEncode(value,0,value.length);
    }

    public ExpandingEncoder rawEncode(byte[] value, int offset, int length){
        ensureCapacity(length+Encoding.encodedLength(length));
        currentOffset+=Encoding.encode(length,buffer,currentOffset);
        assert currentOffset+length<=buffer.length: "Did not ensure enough capacity!";
        if(value != null) {
            System.arraycopy(value, offset, buffer, currentOffset, length);
        }
        currentOffset+=length;

        return this;
    }

    public ExpandingEncoder rawEncode(ByteSlice byteSlice){
        return rawEncode(byteSlice.array(),byteSlice.offset(),byteSlice.length());
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public byte[] getBuffer(){
        if(currentOffset<buffer.length){
            byte[] newBytes = new byte[currentOffset];
            System.arraycopy(buffer,0,newBytes,0,currentOffset);
            return newBytes;
        }else
            return buffer;
    }

    /****************************************************************************************************************/
    /*private helper methods*/
    private void ensureCapacity(int requiredLength) {
        if(buffer.length-currentOffset>requiredLength) return; //we have enough space, no worries!

        long len = buffer.length;
        do {
            len = (long)(resizeFactor * len);
        }while(len-currentOffset<requiredLength);

        int newSize;
        if(len>Integer.MAX_VALUE)
           newSize = Integer.MAX_VALUE;
        else
            newSize = (int)len;
        buffer = Arrays.copyOf(buffer,newSize);
    }


}
