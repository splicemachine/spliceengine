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

/**
 * @author Scott Fines
 *         Created on: 6/6/13
 */
final class ScalarEncoding{
    /*
     * bit marking the position of the sign bit on a 64-bit long
     */
    private static final byte LONG_SIGN_BIT=(byte)0x80;
    /*
     * 0x40 = 0100 0000 which is the bit marking the position of
     * the first header entry. If we can get away with it, we
     * only use one header bit, which is this one
     */
    private static final byte SINGLE_HEADER_BIT=(byte)0x40;
    /*
     * 0x20 = 0010 0000, which is the bit marking the position of
     * the second header entry.
     */
    private static final byte DOUBLE_HEADER_BIT=(byte)0x20;

    private static final int SIGN_SHIFT=63;

    public static byte[] writeLong(long x,boolean desc){
        long sign=x&Long.MIN_VALUE;
        int length=getEncodedLength(x,sign);
        byte[] data=new byte[length];
        doEncode(x,desc,sign,data,0,length);
        return data;
    }

    /**
     * Encode a long into the specified byte array, at the specified offset.
     *
     * Encoding:
     * <p/>
     * The first bit is a sign bit. When it is set to 1, the number is positive, otherwise it is negative.
     * The second,third, and 4th are a packed length delimiter, which works as follows:
     * <p/>
     * if x requires only 6 bits, then we use only the second bit as a length field,
     * and pack the value into the remaining 6 bits, for a total of 1 full byte.
     * <p/>
     * if x takes between 7 and 13 bits, then we need 2 bits to represent the length,
     * so we use the second and third bit positions as a length header. Then we use
     * the remaining 5 bits from the header, plus an additional full byte to store x.
     * <p/>
     * if x takes more than 13 bits, then we encode the second and third bit (just as for a 2-byte number),
     * then encode the length in the next 4 bits(Since 9 is the maximum number of bytes that a number
     * requires including the header byte, and 9 requires 4 bits to encode). This gives us 2 bits left in the header.
     * We can't leave those bits empty, because otherwise the sort order would be incorrect with respect
     * to shorter-encoded numbers. To avoid this, we encode the remaining 2 bits with the most significant bits
     * from the base number. The length is encoded using the formula {@code encodedLength = ((length-3)^~sign)<<3-1}
     * <p/>
     * After the header byte is written, we pack the most significant bits into the remaining bytes, using big-endian
     * encoding (which preserves the natural sort order of the data).
     * <p/>
     * Example: Consider encoding the number {@code 18278}. In this case, we know that the sign is positive, so the first
     * bit in the header is 1. We also know that {@code 18278 = 0x00 0x00 0x47 0x66} in hex. Thus, we see that 18278 has
     * 15 bits to encode. Since that is > 13, the header looks like {@code 111x xxxx} (the x denotes an as-yet undetermined bit).
     * We then encode the length as {@code eLength = ((3-3)^~sign)<<3-1 = 0}, so we have {@code 1110 00xx}. Finally,
     * we stuff the 2 most significant bits of the number in the remaining bytes of the header, giving {@code 1110 0000}.
     * The body is encoded as the big-endian bytes of the data itself, which is {@code 0x47 0x66}; thus, the final
     * encoded value is {@code 0xE0 0x47 0x66}
     *
     * @param x the value to encode
     * @param data the array to encode into. This array must be at least as large as the number of bytes
     *             required to encode {@code x}, or an exception will be thrown
     * @param offset the location inside of {@code data} at which to start writing the encoded contents of {@code x}.
     *               if {@code offset + encodedLength(x)>=data.length}, and exception will be thrown.
     * @param desc {@code true} if bytes are to be sorted in descending order, {@code false} otherwise
     * @return the number of bytes used to encode {@code x}
     * @throws AssertionError if assertions are enabled and the length of {@code data} is insufficient to hold the
     *          encoded contents of {@code x}
     * @throws ArrayIndexOutOfBoundsException if assertions are disabled and the length of {@code data} is insufficient
     *          to hold the encoded contents of {@code x}
     */
    public static int writeLong(long x,byte[] data,int offset,boolean desc){
        long sign=x&Long.MIN_VALUE;

        //serialize the first byte
        int length=getEncodedLength(x,sign);

        doEncode(x,desc,sign,data,offset,length);
        return length;
    }

    /**
     * Get the number of bytes occupied by x when encoded.
     *
     * @param x the value whose encoded size is to be computed
     * @return the number of bytes occupied by x when encoded.
     */
    public static int encodedLength(long x){
        return getEncodedLength(x,x&Long.MIN_VALUE);
    }

    /**
     * Decode the data as an encoded long. Equivalent to
     * {@link #readLong(byte[], int, boolean)},with {@code offset==0}.
     *
     * @param data the array containing the long to be decoded.
     * @param desc when set to {@code true}, the data is to be treated as encoded
     *             in descending order.
     * @return the long which is encoded in the byte array
     */
    public static long readLong(byte[] data,boolean desc){
        return readLong(data,0,desc);
    }

    /**
     * Decode the data as an encoded long.
     *
     * @param data the data to decode as
     * @param offset the position in the byte array to begin decoding at.
     * @param desc when set to {@code true}, the data is to be treated as encoded
     *             in descending order.
     * @return the long which is encoded in the byte array at location {@code offset}
     */
    public static long readLong(byte[] data,int offset,boolean desc){
        assert data.length>0; //need at least one byte
        byte headerByte=data[offset];
        if(desc)
            headerByte^=0xff;

        int sign=(headerByte&LONG_SIGN_BIT)!=0?0:Byte.MIN_VALUE;
        int negSign=~sign>>Integer.SIZE-1;

        int h=headerByte^negSign;
        int length;
        int numHeaderDataBits;
        if((h&SINGLE_HEADER_BIT)!=0){
            length=1;
            numHeaderDataBits=0x6;
        }else if((h&DOUBLE_HEADER_BIT)!=0){
            length=2;
            numHeaderDataBits=0x5;
        }else{
            length=(headerByte^~negSign)>>>0x2;
            length&=(1<<0x3)-1;
            length+=0x3;
            numHeaderDataBits=0x2;
        }

        long x=decodeHeader(headerByte,sign,length,numHeaderDataBits);

        x=decodeBody(data,offset,desc,sign,length,x);
        return x;
    }

    /**
     * Decode the int encoded in the byte array. Equivalent to {@link #readLong(byte[], boolean)},
     * but returning an int for convenience.
     *
     * @param data the byte array containing the int to be decoded.
     * @param desc when {@code true}, the data is treated as encoded in descending order, otherwise
     *             it's treated as encoded in ascending order.
     * @return the int encoded in the byte array.
     */
    public static int readInt(byte[] data,boolean desc){
        return (int)readLong(data,desc);
    }

    /**
     * Decode a long, similar to {@link #readLong(byte[], int,boolean)}, but which returns an array. The
     * first element in the array is the decoded long, and the second element is the number of bytes that were
     * used to store the encoded data.
     *
     * @param data the data storing the encoded long
     * @param byteOffset the offset at which to start reading
     * @param reservedBits the number of bits in the header which are "reserved"--i.e. used by something else.
     * @return a long[] holding the elements {@code (x,length(x))}
     */
    public static long[] readLong(byte[] data, int byteOffset, boolean desc, int reservedBits){
        assert data.length>0; //need at least one byte
        byte headerByte=data[byteOffset];
        if (desc) {
            headerByte ^= 0xff;
        }
        headerByte<<=reservedBits;

        int sign=(headerByte&LONG_SIGN_BIT)!=0?0:Byte.MIN_VALUE;
        int negSign=~sign>>Integer.SIZE-1;

        int h=headerByte^negSign;
        int length;
        int numHeaderDataBits;
        if((h&SINGLE_HEADER_BIT)!=0){
            length=1;
            numHeaderDataBits=0x6-reservedBits;
        }else if((h&DOUBLE_HEADER_BIT)!=0){
            length=2;
            numHeaderDataBits=0x5-reservedBits;
        }else{
            length=(headerByte^~negSign)>>>0x2;
            length&=(1<<0x3)-1;
            length+=0x3;
            numHeaderDataBits=0x2-reservedBits;
        }

        long x=(long)sign>>Long.SIZE-1;
        byte d=(byte)(x<<numHeaderDataBits);
        d|=(byte)((headerByte>>>reservedBits)&((1<<numHeaderDataBits)-1));
        if(sign!=0)
            x&=~(((long)~d&0xff)<<(length-1)*8);
        else
            x|=(((long)d&0xff)<<(length-1)*8);

        x=decodeBody(data,byteOffset,desc,sign,length,x);
        return new long[]{x,length};
    }

    /**
     * Serializes a boolean into a 1-byte byte[].
     * <p/>
     * When sorted in ascending order,{@code true} is encoded as 1, and {@code false} is encoded as 2.
     * <p/>
     * When sorted in descending order, {@code true} is encoded as 2, and {@code false} is encoded as 1.
     *
     * @param value the value to serialize
     * @param desc  if it should be sorted in descending order.
     * @return [1] if {@code value} is true, [2] otherwise
     */
    public static byte[] writeBoolean(boolean value,boolean desc){
        byte f = (byte)(desc ? 0x01^0xff : 0x01);
        byte t = (byte)(desc ? 0x02^0xff : 0x02);

        if (value)
            return new byte[]{t};
        else
            return new byte[]{f};

    }

    /**
     * decode the data as a boolean. Equivalent to {@link #readBoolean(byte[],int, boolean)}.
     * @param data the data to be decoded
     * @param desc when {@code true}, the data is encoded in descending order, otherwise encoded in ascending
     *             order.
     * @return the boolean encoded in the array.
     */
    public static boolean readBoolean(byte[] data,boolean desc){
        return readBoolean(data,0,desc);
    }

    /**
     * Decode the data in the array as a boolean, starting the read at the {@code offset} position.
     *
     * @param data the data to be decoded
     * @param offset the position in the array to start reading from
     * @param desc when {@code true}, the data is encoded in descending order, otherwise encoded in ascending order.
     * @return the boolean encoded in the array.
     */
    public static boolean readBoolean(byte[] data,int offset,boolean desc){
        if(desc) {
            return (byte)(data[offset]^0xff) == 0x02;
        }
        else return data[offset]==0x02;
    }

    /**
     * Read the length of the encoded long in the array, without reading the entire long.
     *
     * @param data the data to read from
     * @param byteOffset the offset in the array to begin reading from
     * @param desc when {@code true}, the data is encoded in descending order, otherwise encoded in ascending order.
     * @return the number of bytes used to encode this long.
     */
    public static int readLength(byte[] data,int byteOffset,boolean desc){
        assert data.length>0; //need at least one byte
        byte headerByte=data[byteOffset];
        if(desc)
            headerByte^=0xff;

        int sign=(headerByte&LONG_SIGN_BIT)!=0?0:Byte.MIN_VALUE;
        int negSign=~sign>>Integer.SIZE-1;

        int h=headerByte^negSign;
        int length;
        if((h&SINGLE_HEADER_BIT)!=0){
            length=1;
        }else if((h&DOUBLE_HEADER_BIT)!=0){
            length=2;
        }else{
            length=(headerByte^~negSign)>>>0x2;
            length&=(1<<0x3)-1;
            length+=0x3;
        }
        return length;
    }

    public static void readLong(byte[] data,int byteOffset,boolean desc,long[] valueAndLength){
        assert data.length>0; //need at least one byte
        byte headerByte=data[byteOffset];
        if(desc)
            headerByte^=0xff;

        int sign=(headerByte&LONG_SIGN_BIT)!=0?0:Byte.MIN_VALUE;
        int negSign=~sign>>Integer.SIZE-1;

        int h=headerByte^negSign;
        int length;
        int numHeaderDataBits;
        if((h&SINGLE_HEADER_BIT)!=0){
            length=1;
            numHeaderDataBits=0x6;
        }else if((h&DOUBLE_HEADER_BIT)!=0){
            length=2;
            numHeaderDataBits=0x5;
        }else{
            length=(headerByte^~negSign)>>>0x2;
            length&=(1<<0x3)-1;
            length+=0x3;
            numHeaderDataBits=0x2;
        }

        long x = decodeHeader(headerByte,sign,length,numHeaderDataBits);

        x = decodeBody(data,byteOffset,desc,sign,length,x);
        valueAndLength[0]=x;
        valueAndLength[1]=length;
    }

    /*package-local methods*/
    static byte[] writeLong(long x,byte extraHeader,int extraHeaderSize){
        long sign=x&Long.MIN_VALUE;
        long diffBits=x^(sign>>SIGN_SHIFT);
        int numBits=Long.SIZE-Long.numberOfLeadingZeros(diffBits);

        /*
         * We are sneaky with our bit-packing here. If we can fit the entire number in 6 bits, then
         * we can use just a single byte. If we can fit it in 12 bits, then we can use 2 bytes. Otherwise,
         * we'll need between 3 and 9 bytes. Remember that we use the first 2 bits in every byte for order
         * encoding
         */
        int length;

        //serialize the first byte
        int negSign=sign!=0?0:-1; //negate the sign

        //start with 1000000
        int b=sign!=0?0:LONG_SIGN_BIT;
        int numHeaderBits; //the number of bits available for use in the first byte
        if(numBits<=0x6-extraHeaderSize){
            b|=(~negSign&SINGLE_HEADER_BIT); //b now becomes either 00000000(pos) or 01000000(neg), depending on sign
            numHeaderBits=0x6-extraHeaderSize; //we have 6 bits available
            length=1;
        }else if(numBits<=13-extraHeaderSize){
            b|=(negSign&SINGLE_HEADER_BIT)|(~negSign&DOUBLE_HEADER_BIT); //b = 00100000(neg) or 01000000(pos) depending on sign
            numHeaderBits=0x5-extraHeaderSize; //we only have 5 available
            length=2;
        }else{
            /*
             * we need at least 3 bytes to store the value.
             *
             * Since Long.MAX_VALUE needs 8 bytes to represent, we must have a 4-bit length
             * header, for a total of 5 used bits in the header byte. That leaves us three
             * bits in the header byte. Since Long.MAX_VALUE requires 63 bits, minus 3 available
             * in the header byte, leaves a header byte + 64-3 = 61 bits to store. 61 bits
             * requires 7 bytes + 5 extra bits. Round that up to a full byte, and we require
             * a header byte + 8 data bytes = 9 bytes total.
             *
             * Thus, we can have a long between 3 and 9 bytes of storage, at best.
             */
            length=1+((numBits+0x5+extraHeaderSize)>>>3);
            //get the encoded length
            int encodedLength=(length-0x3)^~negSign;
            encodedLength&=(1<<0x3)-1;
            encodedLength<<=0x2;

            b|=(negSign&(SINGLE_HEADER_BIT|DOUBLE_HEADER_BIT))|encodedLength;
            numHeaderBits=0x2-extraHeaderSize; //only 2 bits are available in the first byte
        }

        byte[] data=new byte[length];
        /*
         * Pack the header bytes. We can't use encodeHeader() here, because of the extraHeaderSize
         * elements that we have to make use of
         */
        b=b>>>extraHeaderSize;
        b=encodeHeader(x,false,sign,length,b,numHeaderBits);
        b&=(0xff>>>extraHeaderSize);
        b|=(extraHeader<<Byte.SIZE-extraHeaderSize);

        data[0]=(byte)b;

        encodeBody(x,false,data,0,length);
        return data;
    }
    /*****************************************************************************************************************/
    /*private helper methods*/
    private static void doEncode(long x,boolean desc,long signBit,byte[] data,int offset,int length){
        assert offset+length<=data.length:"Not enough room to encode "+x;
        int negSign=signBit!=0?0:-1; //negate the sign
        int b=signBit!=0?0:LONG_SIGN_BIT;
        int numHeaderBits; //the number of bits available for use in the first byte
        switch(length){
            case 1:
                b|=(~negSign&SINGLE_HEADER_BIT); //b now becomes either 00000000(pos) or 01000000(neg), depending on sign
                numHeaderBits=0x6; //we have 6 bits available
                break;
            case 2:
                b|=(negSign&SINGLE_HEADER_BIT)|(~negSign&DOUBLE_HEADER_BIT); //b = 00100000(neg) or 01000000(pos) depending on sign
                numHeaderBits=0x5; //we only have 5 available
                break;
            default:
                //get the encoded length
                int encodedLength=(length-0x3)^~negSign;
                encodedLength&=(1<<0x3)-1;
                encodedLength<<=0x2;

                b|=(negSign&(SINGLE_HEADER_BIT|DOUBLE_HEADER_BIT))|encodedLength;
                numHeaderBits=0x2; //only 2 bits are available in the first byte
        }

        data[offset]=encodeHeader(x,desc,signBit,length,b,numHeaderBits);
        encodeBody(x,desc,data,offset,length);
    }

    private static int getEncodedLength(long x,long sign){
        /*
         * Here we are trying to find the number of bits which differ from the sign bit. This is
         * totally stolen from Hacker's Delight, number 5.3, under the section called
         * "Relation to the Log Function".
         *
         * In essence, we compute "bitsize(x)", which is the number of bits required to
         * represent our value in two's complement form. By arithmetically shifting the sign
         * bit over 63 places, we essentially create a number which is all 1's if the sign bit is 1,
         * or all 0s if the sign bit is 0. Then, by XORing that result with our actual value, we end
         * up with zeros everywhere value and the shifted sign value match, and 1s where they don't, which
         * is basically just 1s wherever the value doesn't agree with the sign bit. then we need at least
         * as many bits to represent this last number as it takes to put a 1 in the least significant bit location,
         * which is the number of leading zeros.
         */
        long diffBits=x^(sign>>SIGN_SHIFT);
        int numBits=Long.SIZE-Long.numberOfLeadingZeros(diffBits);

        int length;
        /*
         * We are sneaky with our bit-packing here. If we can fit the entire number in 6 bits, then
         * we can use just a single byte. If we can fit it in 12 bits, then we can use 2 bytes. Otherwise,
         * we'll need between 3 and 9 bytes. Remember that we use the first 2 bits in every byte for order
         * encoding
         */
        if(numBits<=0x6){
            length=1;
        }else if(numBits<=13){
            length=2;
        }else{
            /*
             * we need at least 3 bytes to store the value.
             *
             * Since Long.MAX_VALUE needs 8 bytes to represent, we must have a 4-bit length
             * header, for a total of 5 used bits in the header byte. That leaves us three
             * bits in the header byte. Since Long.MAX_VALUE requires 63 bits, minus 3 available
             * in the header byte, leaves a header byte + 64-3 = 61 bits to store. 61 bits
             * requires 7 bytes + 5 extra bits. Round that up to a full byte, and we require
             * a header byte + 8 data bytes = 9 bytes total.
             *
             * Thus, we can have a long between 3 and 9 bytes of storage, at best.
             */
            length=1+((numBits+0x5)>>>3);
        }
        return length;
    }

    private static byte encodeHeader(long x,boolean desc,long signBit,int length,int currHeaderByte,int numHeaderBits){
        /*
         * Pack the header byte
         */
        byte firstDataByte;
        if(length>8){
            firstDataByte=(byte)(signBit>>Integer.SIZE-1);
        }else
            firstDataByte=(byte)(x>>>(length-1)*8);
        firstDataByte&=(1<<numHeaderBits)-1;
        currHeaderByte=(byte)(currHeaderByte|firstDataByte);
        if(desc)
            currHeaderByte^=0xff; //reverse the sign bit so that data is reversed in 2's-complement
        return (byte)currHeaderByte;
    }

    private static void encodeBody(long x,boolean desc,byte[] data,int offset,int length){
        /*
         * pack the remaining bytes in big-endian order.
         */
        for(int i=1, pos=2;i<length;i++,pos++){
            int bytePos=(length-pos)*8;
            byte nextByte=(byte)(x>>>bytePos);
            if(desc)
                nextByte^=0xff;
            data[offset+i]=nextByte;
        }
    }

    private static long decodeHeader(byte headerByte,int sign,int length,int numHeaderDataBits){
        long x=(long)sign>>Long.SIZE-1;
        if(length<=8){
            byte d=(byte)(x<<numHeaderDataBits);
            d|=(byte)(headerByte&((1<<numHeaderDataBits)-1));
            if(sign!=0)
                x&=~(((long)~d&0xff)<<(length-1)*8);
            else
                x|=(((long)d&0xff)<<(length-1)*8);
        }
        return x;
    }

    private static long decodeBody(byte[] data,int offset,boolean desc,int sign,int length,long x){
        for(int i=1, pos=2;i<length;i++,pos++){
            byte next=data[offset+i];
            if(desc)
                next^=0xff;
            int nextByteOffset=(length-pos)*8;
            if(sign!=0)
                x&=~(((long)~next&0xff)<<nextByteOffset);
            else
                x|=(((long)next&0xff)<<nextByteOffset);
        }
        return x;
    }

}
