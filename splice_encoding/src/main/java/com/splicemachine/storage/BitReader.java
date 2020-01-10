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

package com.splicemachine.storage;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Utility tool for reading Bits from a byte[] as if they were Java primitives.
 *
 * Useful for hiding the annoyances with byte position modification, etc.
 *
 * @author Scott Fines
 * Created on: 7/9/13
 */
public class BitReader {
    private int[] byteAndBitOffset;
    private byte[] data;

    private int length;

    private int initialOffset;
    private int initialBitPos;

    private boolean useContinuationBit;

    public BitReader(byte[] data, int offset, int length, int startingBitPos){
        this(data,offset,length,startingBitPos,true);
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public BitReader(byte[] data, int offset, int length, int startingBitPos,boolean useContinuationBit){
        this.data = data;
        this.byteAndBitOffset = new int[]{offset,startingBitPos};
        this.length = length;
        this.initialBitPos=startingBitPos;
        this.initialOffset = offset;
        this.useContinuationBit = useContinuationBit;
    }

    /**
     * @return a byte containing <em>only</em> the bit of interest. For example, if the next bit is
     * in the 8s position, and is set to 1, then the result of this call is 0000 1000 (8).
     *
     * @throws IndexOutOfBoundsException if a bit is asked past the end of the Buffer
     */
    public int next(){
        adjustBit();
        int val = data[byteAndBitOffset[0]] & (1<<Byte.SIZE-byteAndBitOffset[1]);
        byteAndBitOffset[1]++;
        return val;
    }

    private void adjustBit() {
        if(byteAndBitOffset[1]==9){
            byteAndBitOffset[0]++;
            if(byteAndBitOffset[0]>=initialOffset+length) throw new IndexOutOfBoundsException();
            if(useContinuationBit)
                byteAndBitOffset[1]=2;
            else
                byteAndBitOffset[1]=1;
        }
    }

    public int peek(){
        if(!hasNext()) throw new IndexOutOfBoundsException();
        return data[byteAndBitOffset[0]] & (1<<Byte.SIZE-byteAndBitOffset[1]);
    }

    /**
     *
     * @return true if there are more bits in the reader, false otherwise.
     */
    @SuppressWarnings("SimplifiableIfStatement") // it's clearer unsimplified, and the JVM will optimize it anyway
    public boolean hasNext(){
        if(byteAndBitOffset[0]<initialOffset+length-1)
            return true;
        else if(byteAndBitOffset[0]>=initialOffset+length)
            return false;
        else{
            return byteAndBitOffset[1]<9;
        }
    }

    /**
     * Replaces the underlying buffer with a new one.
     *
     * @param data the buffer to read from
     * @param offset the byte position within the buffer to start reading
     * @param length the length of the buffer to read
     * @param startingBitPos the starting bit position within the starting byte to read from
     * @param useContinuationBit whether or not each byte should be assumed to have a continuation bit.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public void reset(byte[] data, int offset, int length,int startingBitPos,boolean useContinuationBit){
        this.data = data;
        byteAndBitOffset[0] = offset;
        byteAndBitOffset[1] = startingBitPos;
        this.length=length;
        this.initialBitPos = startingBitPos;
        this.initialOffset = offset;
        this.useContinuationBit = useContinuationBit;
    }

    public void rewind(){
        this.byteAndBitOffset[0] = initialOffset;
        this.byteAndBitOffset[1] = initialBitPos;
    }

    public int bitPosition() {
        return byteAndBitOffset[1];
    }

    public int bytePosition(){
        return byteAndBitOffset[0];
    }

    public int nextSetBit() {
        //read until the next bit is set to 1
        //check if there are bits remaining in this byte
        if(!hasNext()) return -1;
        int zerosSkipped=0;
        while(byteAndBitOffset[1]<9){
            adjustBit();
            if((data[byteAndBitOffset[0]] &(1<<Byte.SIZE-byteAndBitOffset[1]))!=0){
                byteAndBitOffset[1]++;
                return zerosSkipped;
            }else
                zerosSkipped++;
            byteAndBitOffset[1]++;
        }
        if(!hasNext()) return -1;

        adjustBit();

        //skip all zero bytes
        if(useContinuationBit){
            while(data[byteAndBitOffset[0]]==(byte)0x80){
                zerosSkipped+=7;
                byteAndBitOffset[0]++;
                if(!hasNext()) return -1;
            }
        }else{
            while(data[byteAndBitOffset[0]]==(byte)0x00){
                zerosSkipped+=8;
                byteAndBitOffset[0]++;
                if(!hasNext()) return -1;
            }
        }
        //read from the remainder
        if(!hasNext()) return -1;
        while(next()==0){
            zerosSkipped++;
            if(!hasNext())return -1;
        }
        return zerosSkipped;
    }
}
