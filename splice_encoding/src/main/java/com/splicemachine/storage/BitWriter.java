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
 * @author Scott Fines
 * Created on: 7/9/13
 */
public class BitWriter {
    private byte[] buffer;

    private int[] byteAndBitOffset;

    private int length;
    private boolean useContinuationBit;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public BitWriter(byte[] buffer, int offset, int length, int initialBitPos,
                     boolean useContinuationBit){
        this.buffer = buffer;
        this.length = length;
        this.byteAndBitOffset = new int[]{offset,initialBitPos};
        this.useContinuationBit = useContinuationBit;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public void set(byte[] buffer, int offset, int length, int initialBitPos){
        this.buffer = buffer;
        this.byteAndBitOffset[0] = offset;
        byteAndBitOffset[1] = initialBitPos;
        this.length = length;
    }

    public void setNext(){
        set(1);
    }

    /**
     * Set the next {@code n} bits to 1.
     *
     * @param n the number of bits to set to 1
     */
    public void set(int n){
        /*
         * We do this whole odd writing pattern because most modern CPUs are more
         * efficient operating on words (and thus whole bytes) than they are operating
         * on individual bits. Also, if n is larger than the byte size, we don't have
         * to perform any operations at all, we just have to set the final byte into position.
         */
        while(byteAndBitOffset[1]<9&&n>0){
            buffer[byteAndBitOffset[0]] |= (1<<Byte.SIZE-byteAndBitOffset[1]);
            byteAndBitOffset[1]++;
            n--;
        }
        if(n==0) return;
        adjustBitPosition();
        int byteSize = useContinuationBit?7:8;
        while(n>byteSize){
            byteAndBitOffset[0]++;
            if(byteAndBitOffset[1]>=buffer.length) throw new IndexOutOfBoundsException();
            buffer[byteAndBitOffset[0]] = (byte)0xff;
            n-=byteSize;
        }
        if(n==0) return;
        adjustBitPosition();
        while(n>0){
            buffer[byteAndBitOffset[0]] |= (1<<Byte.SIZE-byteAndBitOffset[1]);
            byteAndBitOffset[1]++;
            n--;
        }
    }

    public void skipNext(){
        skip(1);
    }

    public void skip(int n){
        //skip to the end of this byte
        while(byteAndBitOffset[1]<9&&n>0){
            byteAndBitOffset[1]++;
            n--;
        }
        if(n==0) return;

        adjustBitPosition();
        //skip whole bytes
        int byteSize = useContinuationBit?7:8;
        while(n>byteSize){
            byteAndBitOffset[0]++;
            if(byteAndBitOffset[0]>=buffer.length)  throw new IndexOutOfBoundsException();
            if(useContinuationBit)
                buffer[byteAndBitOffset[0]] = (byte)0x80;
            n-=byteSize;
        }
        if(n==0) return;
        adjustBitPosition();
        byteAndBitOffset[1]+=n;
    }

    private void adjustBitPosition() {
        if(byteAndBitOffset[1]==9){
            byteAndBitOffset[0]++;
            if(byteAndBitOffset[0]>=length) throw new IndexOutOfBoundsException();
            if(useContinuationBit){
                buffer[byteAndBitOffset[0]] = (byte)0x80;
                byteAndBitOffset[1] = 2;
            }else{
                byteAndBitOffset[1] = 1;
            }
        }
    }
}
