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

package com.splicemachine.storage.index;

import com.splicemachine.storage.BitReader;
import com.splicemachine.storage.BitWriter;

/**
 * Utility class to conform to correctness of an Elias Delta Encoding.
 *
 * Elias Delta Encoding is a Universal Code for positive integers with near-optimal storage capacities.
 * Most importantly, Delta Encoding requires {@code lg(x) + 2*lg(lg(x)+1)+1} bits to store
 * {@code x} (where {@code lg(x)} is the floor of the base-2 logarithm of x, or the largest number {@code N} such that
 * {@code 2^N < x}).
 * For more information, see <a href="http://en.wikipedia.org/wiki/Elias_delta_coding">Wikipedia's
 * article</a>.
 *
 * Because there is no clear demarcation between the end of a stream of Delta-coded numbers and
 * the start of additional streams, this implementation uses a continuation-bit mechanism similar to that
 * of Protocol Buffers. In this case, the Most Significant Bit (the leftmost bit in the byte) is always set
 * to 1. This means that no bytes in the encoding are fully zero, but it also means that only 7 bits in each
 * byte are available for storage, which increases the length of the encoded stream.
 *
 * @author Scott Fines
 * Created on: 7/8/13
 */
class DeltaCoding {

    /**
     * Compute the number of bits required to encode {@code number} using Delta encoding.
     * @param number the number to be checked
     * @return the number of bits required to encode {@code number} using Delta encoding.
     */
    static int getEncodedLength(int number){
        /*
         * We get the nice advantage of having a proven formula for computing the length (in bits)
         * of the number, which is lg(x) + 2*lg(lg(x)+1)+1
         */
        //note that floor(log2(x)) = 31-numberOfLeadingZeros(i)
        int log2x = 31-Integer.numberOfLeadingZeros(number);
        int log2x1 = 31-Integer.numberOfLeadingZeros(log2x+1);
        return log2x+2*log2x1+1;
    }

    /**
     * Decode the next number from the stream (or buffer) of data.The {@code byteAndBitOffset} is used
     * to determine (and update) the position in the stream during processing.
     *
     * @param data the data stream
     * @param byteAndBitOffset a representation of the position in the bitstream. The first entry is the byte
     *                         offset in the data, and the second is the bit Position in the stream.
     * @return the next decoded number
     */
    static int decode(byte[] data, int[] byteAndBitOffset) {
        int byteOffset = byteAndBitOffset[0];
        int bitPos = byteAndBitOffset[1];

        byte byt;
        if(bitPos==9){
            byteOffset++;
            if(byteOffset>=data.length)
                return -1;
            byt = data[byteOffset];
            bitPos=2;
        }else
            byt = data[byteOffset];
        int l = 0;
        while((byt & 1<<Byte.SIZE-bitPos)==0){
            l++;
            bitPos++;
            if(bitPos==9){
                byteOffset++;
                if(byteOffset>=data.length)
                    return -1; //we've exhausted the array, so there can't be any more data in the set
                byt = data[byteOffset];
                bitPos=2;
            }
        }
        bitPos++;
        //read the next l digits in
        int n = 1<<l;
        for(int i=1;i<=l;i++){
            if(bitPos==9){
                byteOffset++;
                if(byteOffset>=data.length)
                    return -1;
                byt = data[byteOffset];
                bitPos=2;
            }
            int val = byt & (1<< Byte.SIZE-bitPos);
            if(val!=0)
                n |= (1<<l-i);
            bitPos++;
        }
        n--;
        int retVal = 1<<n;
        //read remaining digits
        for(int i=1;i<=n;i++){
            if(bitPos==9){
                byteOffset++;
                if(byteOffset>=data.length)
                    return -1;
                byt = data[byteOffset];
                bitPos=2;
            }
            int val = byt & (1<<Byte.SIZE-bitPos);
            if(val!=0)
                retVal |= (1<<n-i);
            bitPos++;
        }
        byteAndBitOffset[0] = byteOffset;
        byteAndBitOffset[1] = bitPos;
        return retVal;
    }

    static int decode(BitReader bitReader){
        int l = 0;
        if(!bitReader.hasNext()) return -1;
        while(bitReader.next()==0){
            if(!bitReader.hasNext()) return -1;
            l++;
        }

        //read the next l digits in
        int n = 1<<l;
        for(int i=1;i<=l;i++){
            if(!bitReader.hasNext()) return -1;
            int next = bitReader.next();
            if(next!=0)
                n |= (1<<l-i);
        }
        n--;
        int retVal = 1<<n;
        //read remaining digits
        for(int i=1;i<=n;i++){
            if(!bitReader.hasNext()) return -1;
            int val = bitReader.next();
            if(val!=0)
                retVal |= (1<<n-i);
        }
        return retVal;
    }

    /**
     * Delta-Encode a number into a stream/buffer, updating positional information as we go.
     *
     * @param bytes the stream/buffer to encode into
     * @param val the value to encode
     * @param byteAndBitOffset the positional information of the stream (first entry is the byte offset,
     *                         second entry is the bit position within that byte).
     */
    static void encode(byte[] bytes,int val,int[] byteAndBitOffset) {
        int x = 32-Integer.numberOfLeadingZeros(val);
        int numZeros = 32-Integer.numberOfLeadingZeros(x)-1;

        int byteOffset = byteAndBitOffset[0];
        int bitPos = byteAndBitOffset[1];
        byte next = bytes[byteOffset];
        for(int i=0;i<numZeros;i++){
            if(bitPos==9){
                bytes[byteOffset]=next;
                byteOffset++;
                next = bytes[byteOffset];
                if(byteOffset>0)
                    next = (byte)0x80;
                bitPos=2;
            }
            bitPos++;
        }

        //append bits of x
        int numBitsToWrite = 32-Integer.numberOfLeadingZeros(x);
        for(int i=1;i<=numBitsToWrite;i++){
            if(bitPos==9){
                bytes[byteOffset]=next;
                byteOffset++;
                next = bytes[byteOffset];
                if(byteOffset>0)
                    next = (byte)0x80; //continuation byte
                bitPos=2;
            }
            int v = (x & (1<<numBitsToWrite-i));
            if(v!=0){
                next |= (1<<Byte.SIZE-bitPos);
            }
            bitPos++;
        }

        //append bits of y
        int pos = 1<<x-1;
        int y = val & ~pos;
        numBitsToWrite = 32-Integer.numberOfLeadingZeros(pos)-1;
        //y might be a bunch of zeros, so if y
        for(int i=1;i<=numBitsToWrite;i++){
            if(bitPos==9){
                bytes[byteOffset]=next;
                byteOffset++;
                next = bytes[byteOffset];
                if(byteOffset>0)
                    next = (byte)0x80; //continuation byte
                bitPos=2;
            }
            int v = (y & (1<<numBitsToWrite-i));
            if(v!=0){
                next |= (1<<Byte.SIZE-bitPos);
            }
            bitPos++;
        }
        if(bitPos!=2){
            bytes[byteOffset]=next;
        }
        byteAndBitOffset[0] = byteOffset;
        byteAndBitOffset[1] = bitPos;
    }

    static void encode(int val,BitWriter bitWriter) {
        int x = 32-Integer.numberOfLeadingZeros(val);
        int numZeros = 32-Integer.numberOfLeadingZeros(x)-1;

        bitWriter.skip(numZeros);

        //append bits of x
        int numBitsToWrite = 32-Integer.numberOfLeadingZeros(x);
        for(int i=1;i<=numBitsToWrite;i++){
            int v = x & (1<<numBitsToWrite-i);
            if(v!=0)
                bitWriter.setNext();
            else
                bitWriter.skipNext();
        }

        //append bits of y
        int pos = 1<<x-1;
        int y = val & ~pos;
        numBitsToWrite = 32-Integer.numberOfLeadingZeros(pos)-1;
        //y might be a bunch of zeros, so if y
        for(int i=1;i<=numBitsToWrite;i++){
            int v = (y & (1<<numBitsToWrite-i));
            if(v!=0)
                bitWriter.setNext();
            else
                bitWriter.skipNext();
        }
    }

    public static void main(String... args) throws Exception{
        int numBits = DeltaCoding.getEncodedLength(4);
        int length = numBits-3;
        int numBytes = length/7;
        if(length%7!=0){
            numBytes++;
        }
        numBytes++;
        byte[] buffer = new byte[numBytes];
        BitWriter writer = new BitWriter(buffer,0,buffer.length,5,true);
        DeltaCoding.encode(4,writer);

        int val =DeltaCoding.decode(buffer,new int[]{0,5});
        System.out.println(val);

    }
}
