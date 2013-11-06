package com.splicemachine.encoding;

/**
 * @author Scott Fines
 * Created on: 6/6/13
 */
final class ScalarEncoding {
    /*
     * bit marking the position of the sign bit on a 64-bit long
     */
    private static final byte LONG_SIGN_BIT = (byte)0x80;
    /*
     * 0x40 = 0100 0000 which is the bit marking the position of
     * the first header entry. If we can get away with it, we
     * only use one header bit, which is this one
     */
    private static final byte SINGLE_HEADER_BIT = (byte)0x40;
    /*
     * 0x20 = 0010 0000, which is the bit marking the position of
     * the second header entry.
     */
    private static final byte DOUBLE_HEADER_BIT = (byte)0x20;

    private static final int SIGN_SHIFT =63;

    /**
     *
     * first bit is a sign bit.
     * second,third, and 4th are a packed length delimiter, which works as follows:
     *
     * if x requires only 6 bits, then we use only the second bit as a length field,
     * and pack the value into the remaining 6 bits, for a total of 1 full byte.
     *
     * if x takes between 7 and 13 bits, then we need 2 bits to represent the length,
     * so we use the second and third bit positions as a length header. Then we use
     * the remaining 5 bits from the header, plus an additional full byte to store x.
     *
     * if x takes more than 13 bits, then it can take anywhere from 3 to 8 bytes to
     * fully represent. Since the number 7 can fit within 3 bits, so we use the 2nd, 3rd,
     * and 4th bits will be used to store the length, and the remaining 4 bits
     *
     * @param x
     * @param desc true if bytes are to be sorted in descending order
     * @return
     */
    public static byte[] toBytes(long x,boolean desc){
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
        long sign = x&Long.MIN_VALUE;
        long diffBits = x^(sign >> SIGN_SHIFT);
        int numBits = Long.SIZE-Long.numberOfLeadingZeros(diffBits);

        /*
         * We are sneaky with our bit-packing here. If we can fit the entire number in 6 bits, then
         * we can use just a single byte. If we can fit it in 12 bits, then we can use 2 bytes. Otherwise,
         * we'll need between 3 and 9 bytes. Remember that we use the first 2 bits in every byte for order
         * encoding
         */
        int length;

        //serialize the first byte
        int negSign = sign !=0 ? 0: -1; //negate the sign

        //start with 1000000
        int b = sign !=0 ? 0: LONG_SIGN_BIT;
        int numHeaderBits; //the number of bits available for use in the first byte
        if(numBits <=0x6){
            b |= (~negSign & SINGLE_HEADER_BIT); //b now becomes either 00000000(pos) or 01000000(neg), depending on sign
            numHeaderBits = 0x6; //we have 6 bits available
            length=1;
        }else if(numBits<= 13){
            b |= (negSign & SINGLE_HEADER_BIT) | (~negSign & DOUBLE_HEADER_BIT); //b = 00100000(neg) or 01000000(pos) depending on sign
            numHeaderBits = 0x5; //we only have 5 available
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
            //get the encoded length
            int encodedLength = (length - 0x3) ^ ~negSign;
            encodedLength &= (1 << 0x3)-1;
            encodedLength <<= 0x2;

            b |= (negSign & (SINGLE_HEADER_BIT|DOUBLE_HEADER_BIT)) | encodedLength;
            numHeaderBits = 0x2; //only 2 bits are available in the first byte
        }

        byte[] data = new byte[length];

        /*
         * Pack the header bytes
         */
        byte firstDataByte = (byte)(x >>> (length-1)*8);
        firstDataByte &= (1<<numHeaderBits)-1;
        b = (byte)(b | firstDataByte);
        if(desc)
            b ^= 0xff; //reverse the sign bit so that data is reversed in 2's-complement

        data[0] = (byte)b;

        /*
         * pack the remaining bytes in big-endian order.
         */
        for(int i=1,pos=2;i<length;i++,pos++){
            int bytePos = (length-pos)*8;
            byte nextByte = (byte)(x >>> bytePos);
            if(desc)
                nextByte^=0xff;
            data[i] = nextByte;
        }
        return data;
    }

    static byte[] toBytes(long x, byte extraHeader,int extraHeaderSize, boolean desc){
        long sign = x&Long.MIN_VALUE;
        long diffBits = x^(sign >> SIGN_SHIFT);
        int numBits = Long.SIZE-Long.numberOfLeadingZeros(diffBits);

        /*
         * We are sneaky with our bit-packing here. If we can fit the entire number in 6 bits, then
         * we can use just a single byte. If we can fit it in 12 bits, then we can use 2 bytes. Otherwise,
         * we'll need between 3 and 9 bytes. Remember that we use the first 2 bits in every byte for order
         * encoding
         */
        int length;

        //serialize the first byte
        int negSign = sign !=0 ? 0: -1; //negate the sign

        //start with 1000000
        int b = sign !=0 ? 0: LONG_SIGN_BIT;
        int numHeaderBits; //the number of bits available for use in the first byte
        if(numBits <=0x6-extraHeaderSize){
            b |= (~negSign & SINGLE_HEADER_BIT); //b now becomes either 00000000(pos) or 01000000(neg), depending on sign
            numHeaderBits = 0x6-extraHeaderSize; //we have 6 bits available
            length=1;
        }else if(numBits<= 13-extraHeaderSize){
            b |= (negSign & SINGLE_HEADER_BIT) | (~negSign & DOUBLE_HEADER_BIT); //b = 00100000(neg) or 01000000(pos) depending on sign
            numHeaderBits = 0x5-extraHeaderSize; //we only have 5 available
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
            int encodedLength = (length - 0x3) ^ ~negSign;
            encodedLength &= (1 << 0x3)-1;
            encodedLength <<= 0x2;

            b |= (negSign & (SINGLE_HEADER_BIT|DOUBLE_HEADER_BIT)) | encodedLength;
            numHeaderBits = 0x2-extraHeaderSize; //only 2 bits are available in the first byte
        }

        byte[] data = new byte[length];
        /*
         * Pack the header bytes
         */
        byte firstDataByte = (byte)(x >>> (length-1)*8);
        firstDataByte &= (1<<numHeaderBits)-1;
        b = (byte)((b>>>extraHeaderSize) | firstDataByte);
        b &= (0xff >>> extraHeaderSize);
        b |= (extraHeader <<Byte.SIZE - extraHeaderSize);
        if(desc)
            b ^= 0xff; //reverse the sign bit so that data is reversed in 2's-complement

        data[0] = (byte)b;

        /*
         * pack the remaining bytes in big-endian order.
         */
        for(int pos=2,i=1;i<length;i++,pos++){
            int bytePos = (length-pos)*8;
            byte nextByte = (byte)(x >>> bytePos);
            if(desc)
                nextByte^=0xff;
            data[i] = nextByte;
        }
        return data;
    }

    public static long toLong(byte[] data, boolean desc){
        return toLong(data,0,desc);
    }

    public static long toLong(byte[] data,int offset, boolean desc){
        assert data.length >0; //need at least one byte
        byte headerByte = data[offset];
        if(desc)
            headerByte ^= 0xff;

        int sign = (headerByte & LONG_SIGN_BIT) !=0 ? 0: Byte.MIN_VALUE;
        int negSign = ~sign >>Integer.SIZE-1;

        int h = headerByte ^ negSign;
        int length;
        int numHeaderDataBits;
        if((h&SINGLE_HEADER_BIT)!=0){
            length =1;
            numHeaderDataBits = 0x6;
        }else if((h&DOUBLE_HEADER_BIT)!=0){
            length =2;
            numHeaderDataBits = 0x5;
        }else{
            length = (headerByte^~negSign)>>>0x2;
            length &= (1<<0x3)-1;
            length += 0x3;
            numHeaderDataBits = 0x2;
        }

        long x = (long)sign >>Long.SIZE-1;
        byte d = (byte)(x<<numHeaderDataBits);
        d |= (byte)(headerByte & ((1<<numHeaderDataBits)-1));
        if(sign!=0)
            x &= ~(((long)~d & 0xff)<<(length-1)*8);
        else
            x |= (((long)d & 0xff)<<(length-1)*8);

        for(int i=1,pos=2;i<length;i++,pos++){
            byte next = data[offset+i];
            if(desc)
                next ^=0xff;
            int nextByteOffset = (length-pos)*8;
            if(sign!=0)
                x &= ~(((long)~next&0xff)<<nextByteOffset);
            else
                x |= (((long)next&0xff)<<nextByteOffset);
        }
        return x;
    }

    public static int getInt(byte[] data,boolean desc){
        return (int) toLong(data, desc);
    }


    /**
     * @param data
     * @param desc
     * @return
     */
    public static long[] toLongWithOffset(byte[] data,int byteOffset, int reservedBits,boolean desc) {
        assert data.length >0; //need at least one byte
        byte headerByte = data[byteOffset];
        if(desc)
            headerByte ^= 0xff;
        headerByte <<=reservedBits;

        int sign = (headerByte & LONG_SIGN_BIT) !=0 ? 0: Byte.MIN_VALUE;
        int negSign = ~sign >>Integer.SIZE-1;

        int h = headerByte ^ negSign;
        int length;
        int numHeaderDataBits;
        if((h&SINGLE_HEADER_BIT)!=0){
            length =1;
            numHeaderDataBits = 0x6-reservedBits;
        }else if((h&DOUBLE_HEADER_BIT)!=0){
            length =2;
            numHeaderDataBits = 0x5-reservedBits;
        }else{
            length = (headerByte^~negSign)>>>0x2;
            length &= (1<<0x3)-1;
            length += 0x3;
            numHeaderDataBits = 0x2 -reservedBits;
        }

        long x = (long)sign >>Long.SIZE-1;
        byte d = (byte)(x<<numHeaderDataBits);
        d |= (byte)((headerByte>>>reservedBits) & ((1<<numHeaderDataBits)-1));
        if(sign!=0)
            x &= ~(((long)~d & 0xff)<<(length-1)*8);
        else
            x |= (((long)d & 0xff)<<(length-1)*8);

        int i=1;
        for(int pos=2;i<length;i++,pos++){
            byte next = data[byteOffset+i];
            if(desc)
                next ^=0xff;
            int offset = (length-pos)*8;
            if(sign!=0)
                x &= ~(((long)~next&0xff)<<offset);
            else
                x |= (((long)next&0xff)<<offset);
        }
        return new long[]{x,i};
    }

    /**
     * Serializes a boolean into a 1-byte byte[].
     *
     * We cannot use {@link org.apache.hadoop.hbase.util.Bytes.toBytes(boolean)}, because it will
     * serialize as either 1 or 0, which violates our reserved bit of 0x00. So this serializes true as
     * 1 and false as 2 instead.
     *
     * @param value the value to serialize
     * @param desc if it should be sorted in descending order.
     * @return [1] if {@code value} is true, [2] otherwise
     */
    public static byte[] toBytes(boolean value, boolean desc){
        if(value)
            return desc? new byte[]{0x02}: new byte[]{0x01};
        else
            return desc? new byte[]{0x01}: new byte[]{0x02};
    }

    public static boolean toBoolean(byte[] data, boolean desc){
        return toBoolean(data,0,desc);
    }

    public static boolean toBoolean(byte[] data, int offset,boolean desc){
        if(desc)
            return data[offset] == 0x02;
        else return data[offset] == 0x01;
    }

    public static int toLongLength(byte[] data, int byteOffset, boolean desc) {
        assert data.length >0; //need at least one byte
        byte headerByte = data[byteOffset];
        if(desc)
            headerByte ^= 0xff;

        int sign = (headerByte & LONG_SIGN_BIT) !=0 ? 0: Byte.MIN_VALUE;
        int negSign = ~sign >>Integer.SIZE-1;

        int h = headerByte ^ negSign;
        int length;
        if((h&SINGLE_HEADER_BIT)!=0){
            length =1;
        }else if((h&DOUBLE_HEADER_BIT)!=0){
            length =2;
        }else{
            length = (headerByte^~negSign)>>>0x2;
            length &= (1<<0x3)-1;
            length += 0x3;
        }
        return length;
    }
    
    public static void toLong(byte[] data, int byteOffset, boolean desc, long[] valueAndLength) {
        assert data.length >0; //need at least one byte
        byte headerByte = data[byteOffset];
        if(desc)
            headerByte ^= 0xff;

        int sign = (headerByte & LONG_SIGN_BIT) !=0 ? 0: Byte.MIN_VALUE;
        int negSign = ~sign >>Integer.SIZE-1;

        int h = headerByte ^ negSign;
        int length;
        int numHeaderDataBits;
        if((h&SINGLE_HEADER_BIT)!=0){
            length =1;
            numHeaderDataBits = 0x6;
        }else if((h&DOUBLE_HEADER_BIT)!=0){
            length =2;
            numHeaderDataBits = 0x5;
        }else{
            length = (headerByte^~negSign)>>>0x2;
            length &= (1<<0x3)-1;
            length += 0x3;
            numHeaderDataBits = 0x2;
        }

        long x = (long)sign >>Long.SIZE-1;
        byte d = (byte)(x<<numHeaderDataBits);
        d |= (byte)((headerByte) & ((1<<numHeaderDataBits)-1));
        if(sign!=0)
            x &= ~(((long)~d & 0xff)<<(length-1)*8);
        else
            x |= (((long)d & 0xff)<<(length-1)*8);

        int i=1;
        for(int pos=2;i<length;i++,pos++){
            byte next = data[byteOffset+i];
            if(desc)
                next ^=0xff;
            int offset = (length-pos)*8;
            if(sign!=0)
                x &= ~(((long)~next&0xff)<<offset);
            else
                x |= (((long)next&0xff)<<offset);
        }
        valueAndLength[0] = x;
        valueAndLength[1] = length;
    }
}
