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

import org.spark_project.guava.primitives.UnsignedBytes;

import java.nio.ByteBuffer;

/**
 * Utility class for encoding byte arrays to a combinable format.
 *
 * @author Scott Fines
 * Created on: 6/10/13
 */
final class ByteEncoding {
    private static final String ONE_ZERO_PAD="0";
    private static final String TWO_ZERO_PAD="00";

    /*Lookup table for encoding */
//    private static final byte[] BCD_ENC_LOOKUP = new byte[]{3,4,5,6,7,9,10,12,14,15};

    /*Lookup table for decoding. -1 means ignored*/
    private static final byte[] BCD_ENC_LOOKUP = new byte[]{      2,3,4,5,6,7,8,9,   11,13,14};
    private static final byte[] BCD_DEC_LOOKUP = new byte[]{-1,-1,0,1,2,3,4,5,6,7,-1,8,-1,9,10};

    static byte[] encode(byte[] value, boolean desc){
        StringBuilder sb = new StringBuilder();
        for (byte next : value) {
            String unsignedByte = Integer.toString(UnsignedBytes.toInt(next));
            if (unsignedByte.length() == 1) {
                sb = sb.append(TWO_ZERO_PAD);
                sb = sb.append(unsignedByte);
            } else if (unsignedByte.length() == 2) {
                sb = sb.append(ONE_ZERO_PAD);
                sb = sb.append(unsignedByte);
            } else
                sb = sb.append(unsignedByte);
        }
        String strRep = sb.toString();

        int serializedLength = (strRep.length()+1)/2;

        byte[] data = new byte[serializedLength];
        char[] digits = strRep.toCharArray();
        for(int i=0;i<serializedLength;i++){
            byte bcd = 0x11; //initialize with 1s in every slot
            int digitIdx = 2*i;
            boolean firstNibbleWritten = false;
            if(digitIdx < digits.length){
                bcd = (byte)(BCD_ENC_LOOKUP[Character.digit(digits[digitIdx],10)] <<4);
                firstNibbleWritten=true;
            }
            digitIdx++;
            if(digitIdx < digits.length){
                bcd |= BCD_ENC_LOOKUP[Character.digit(digits[digitIdx],10)];
            }else if(firstNibbleWritten)
                bcd |= 0x01; //can't let the end of the byte be 0x00, because 0x00 is
            //reserved

            if(desc)
                bcd ^=0xff;
            data[i] = bcd;
        }
        return data;
    }

    static byte[] decode(ByteBuffer value, boolean desc){
        //convert back into character encoding
        StringBuilder sb = new StringBuilder();
        int total = value.remaining();
        for(int i=0;i<total;i++){
            byte c = value.get();
            if(desc)
                c ^= 0xff;

            byte leftNibble = (byte)((c >>>4) & 0x0f);
            sb.append(BCD_DEC_LOOKUP[leftNibble]);

            byte rightNibble = (byte)(c &0x0f);
            if(rightNibble!=0x01)
                sb.append(BCD_DEC_LOOKUP[rightNibble]);
        }
        String bcdStr = sb.toString();
        //every 3 digits is 1 byte
        final byte[] result = new byte[bcdStr.length()/3];

        final char[] digits = bcdStr.toCharArray();
        for(int i=0;i<result.length;i++){
            int digitIdx = 3*i;
            //TODO -sf- this is really ugly
            StringBuilder b = new StringBuilder();
            for(int j=0;j<3;j++){
                b.append(digits[digitIdx+j]);
            }
            result[i] =(byte)Integer.parseInt(b.toString());

        }
        return result;
    }

    static byte[] decode(byte[] value, boolean desc){
        return decode(value,0,value.length,desc);
    }

    static byte[] decode(byte[] value, int offset,int length,boolean desc){
        //convert back into character encoding
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<length;i++){
            byte c = value[offset+i];
            if(desc)
                c ^= 0xff;

            byte leftNibble = (byte)((c >>>4) & 0x0f);
            sb.append(BCD_DEC_LOOKUP[leftNibble]);

            byte rightNibble = (byte)(c &0x0f);
            if(rightNibble!=0x01)
                sb.append(BCD_DEC_LOOKUP[rightNibble]);
        }
        String bcdStr = sb.toString();
        //every 3 digits is 1 byte
        final byte[] result = new byte[bcdStr.length()/3];

        final char[] digits = bcdStr.toCharArray();
        for(int i=0;i<result.length;i++){
            int digitIdx = 3*i;
            //TODO -sf- this is really ugly
            StringBuilder b = new StringBuilder();
            for(int j=0;j<3;j++){
                b.append(digits[digitIdx+j]);
            }
            result[i] =(byte)Integer.parseInt(b.toString());

        }
        return result;
    }

    private static final int[][] MASK_SHIFT_TABLE = new int[][]{
            new int[]{0xfe,1,0x01,6},
            new int[]{0xfc,2,0x03,5},
            new int[]{0xf8,3,0x07,4},
            new int[]{0xf0,4,0x0f,3},
            new int[]{0xe0,5,0x1f,2},
            new int[]{0xc0,6,0x3f,1},
            new int[]{0x80,7,0x7f,0}
    };

    private static final int[][] INVERSE_MASK_SHIFT_TABLE= new int[][]{
            new int[]{0x7f,1,0x40,6},
            new int[]{0x3f,2,0x60,5},
            new int[]{0x1f,3,0x70,4},
            new int[]{0x0f,4,0x78,3},
            new int[]{0x07,5,0x7c,2},
            new int[]{0x03,6,0x7e,1},
            new int[]{0x01,7,0x7f,0}
    };

    static byte[] encodeUnsorted(byte[] value){
        return encodeUnsorted(value,0,value.length);
    }

    static byte[] encodeUnsorted(byte[] value, int offset, int limit) {
        /*
         * Encodes using 7-bit bytes. The first bit of every byte is always set to one (to
         * disambiguate from 0x00, which is reserved), and then the remaining bits are set
         * as in the data array itself.
         *
         * This means that the first byte will actually occupy 2 bytes (7 bits in the first
         * location, then 1 bit in the next byte), the second will take 6 bits in the second,
         * then 2 bits in the third, and so on. This can be compactly represented as two masks and
         * two shifts-- the byte-1 mask, the byte-2 mask, the byte-1 shift and the byte-2 shift. This
         * combination will change with every data byte, but will repeat every 7th byte. The combinations
         * look like
         *
         * byte 0 (b1mask, b1shift, b2mask, b2shift): (0xfe, 1,0x01,6)
         * byte 1 : (0xfc,2,0x03,5)
         * byte 2 : (0xf8,3,0x07,4)
         * byte 3 : (0xf0,4, 0xf0,3)
         * byte 4 : (0xe0,5, 0x1f,2)
         * byte 5 : (0xc0,6, 0x3f,1)
         * byte 6 : (0x80,7, 0x7f,0)
         * byte 8 : (byte 0)
         *
         * In other words, for byte i in the data stream, it's "byte" number is i%7, which determines
         * its masks and shifts. At that point, apply the byte-1 mask and the byte-1 shift on the existing
         * byte, then move to another byte, and compute the byte-2 mask and the byte-2 shift.
         *
         * The total size is 1 extra byte every 7 data bytes, which gives us a size to copy bytes
         * into (value.length +value.length/7 + value.length%7 (remainder bits)
         */
        int numBits = 8*limit;
        int length = numBits/7+1;
        if(numBits%8!=0)
            length++;

        byte[] output = new byte[length];

        byte byt = output[0];
        byt |= 0x80; //set the continuation bit
        int outputPos =0;
        long end = offset+limit;
        for(int pos=0,i=offset;i<end;pos++,i++){
            int[] maskAndShift = MASK_SHIFT_TABLE[pos%7];

            //apply the byte-1 mask and shift
            byte byteToStore = value[i];
            byte val = (byte)((byteToStore & maskAndShift[0])>> maskAndShift[1]);
            byt |= val;
            output[outputPos] = byt;
            outputPos++;

            byt = (byte)(output[outputPos] | 0x80);

            //apply the byte-2 mask and shift
            val = (byte)((byteToStore & maskAndShift[2])<<maskAndShift[3]);
            byt |= val;
            if((pos+1)%7==0){
                output[outputPos] = byt;
                outputPos++; //we've completed this byte
                byt = (byte)(output[outputPos] | 0x80);
            }
        }
        output[outputPos] = byt;
        return output;
    }

    static byte[] decodeUnsorted(byte[] data,int offset,int length){
        int numBits = length*7;
        int l = numBits/8;
        if(numBits%7!=0)
            l++;

        byte[] output = new byte[l];

        int inputPos=0;
        int outputPos=0;
        byte input = data[offset];
        while(inputPos<length-1){
            byte outputByte = output[outputPos];
            int[] maskShift = INVERSE_MASK_SHIFT_TABLE[outputPos%7];

            //deal with byte-1
            byte val = (byte)((input & maskShift[0])<<maskShift[1]);
            outputByte |= val;

            inputPos++; //go to byte-2
            input = data[offset+inputPos];
            val = (byte)((input & maskShift[2])>>maskShift[3]);
            outputByte |= val;

            //output byte is finished, store it and move to the next output byte
            output[outputPos] = outputByte;
            outputPos++;

            if(outputPos%7==0){
                //we've filled 7 data bytes, so there is no more information in the input byte
                inputPos++;
                input = data[offset+inputPos];
            }
        }
        return output;
    }

}
