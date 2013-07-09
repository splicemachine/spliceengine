package com.splicemachine.storage.index;

/**
 * Utility class to conform to correctness of an Elias Delta Encoding
 *
 * @author Scott Fines
 * Created on: 7/8/13
 */
class DeltaCoding {

    static int getEncodedLength(int number){
        //note that floor(log2(x)) = 31-numberOfLeadingZeros(i)
        int log2x = 31-Integer.numberOfLeadingZeros(number);
        int log2x1 = 31-Integer.numberOfLeadingZeros(log2x+1);
        return log2x+2*log2x1+1;
    }

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
                    next = (byte)0x80;
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
                    next = (byte)0x80;
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
}
