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

import com.splicemachine.utils.ByteSlice;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * The Decoder complement to an {@link com.splicemachine.encoding.ExpandingEncoder}
 *
 * @author Scott Fines
 *         Date: 1/19/15
 */
public class ExpandedDecoder {
    private final byte[] buffer;
    private final int offset;
    private int currentOffset;
    private final int length;

    private long[] lengthHolder;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public ExpandedDecoder(byte[] data, int offset,int length){
        this.buffer = data;
        this.currentOffset = this.offset = offset;
        this.length = length;
    }

    public ExpandedDecoder(byte[] data,int offset) {
        this(data,offset,data.length-offset);
    }

    public ExpandedDecoder(byte[] data) {
        this(data,0,data.length);
    }

    public byte decodeByte(){
        assert currentOffset<offset+length: "Buffer Underflow!";
        ensureLengthHolderExists();
        byte b = Encoding.decodeByte(buffer,currentOffset,false,lengthHolder);
        currentOffset+=lengthHolder[1];
        return b;
    }

    public byte rawByte(){
        assert currentOffset<offset+length: "Buffer Underflow!";
        byte b = buffer[currentOffset];
        currentOffset++;
        return b;
    }

    public short decodeShort(){
        assert currentOffset<offset+length: "Buffer Underflow!";
        ensureLengthHolderExists();
        short s = Encoding.decodeShort(buffer,currentOffset,false,lengthHolder);
        currentOffset+=lengthHolder[1];
        return s;
    }

    public int decodeInt(){
        assert currentOffset<offset+length: "Buffer Underflow!";
        ensureLengthHolderExists();
        int v = Encoding.decodeInt(buffer,currentOffset,false,lengthHolder);
        currentOffset+=lengthHolder[1];
        return v;
    }

    public String decodeString(){
        int offset = currentOffset;
        while(buffer[offset]!=0x00)
            offset++;
        int length = offset-currentOffset;
        String s = Encoding.decodeString(buffer,currentOffset,length,false);
        currentOffset = offset+1;
        return s;
    }

    public void sliceNext(ByteSlice slice){
        int length = decodeInt();
        slice.set(buffer,currentOffset,length);
        currentOffset+=length;
    }

    public byte[] rawBytes() {
        int length = decodeInt();
        byte[] data = new byte[length];
        System.arraycopy(buffer,currentOffset,data,0,length);
        currentOffset+=length;
        return data;
    }

    /*****************************************************************************************************************/
    /*private helper methods*/
    private void ensureLengthHolderExists() {
        if(lengthHolder==null)
            lengthHolder = new long[2];
    }

    public int currentOffset() {
        return currentOffset;
    }

}
