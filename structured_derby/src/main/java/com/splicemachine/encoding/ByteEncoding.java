package com.splicemachine.encoding;

import com.google.common.primitives.UnsignedBytes;
import com.gotometrics.orderly.VariableLengthByteArrayRowKey;

import java.util.Arrays;

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
    private static final byte[] BCD_ENC_LOOKUP = new byte[]{3,4,5,6,7,9,10,12,14,15};

    /*Lookup table for decoding. -1 means ignored*/
    private static final byte[] BCD_DEC_LOOKUP = new byte[]{-1,-1,-1,0,1,2,3,4,-1,5,6,-1,7,-1,8,9};

    static byte[] encode(byte[] value, boolean desc){
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<value.length;i++){
            byte next = value[i];
            String unsignedByte = Integer.toString(UnsignedBytes.toInt(next));
            if(unsignedByte.length()==1){
                sb = sb.append(TWO_ZERO_PAD);
                sb = sb.append(unsignedByte);
            }else if(unsignedByte.length()==2){
                sb = sb.append(ONE_ZERO_PAD);
                sb = sb.append(unsignedByte);
            }else
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

    static byte[] decode(byte[] value, boolean desc){
        //convert back into character encoding
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<value.length;i++){
            byte c = value[i];
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

    public static void main(String... args) throws Exception{
        byte[] test = new byte[]{5};
        byte[] myEncoding = encode(test,false);
        System.out.println(Arrays.toString(myEncoding));

        byte[] theirEncoding = new VariableLengthByteArrayRowKey().serialize(test);
        System.out.println(Arrays.toString(theirEncoding));
        System.out.println(Arrays.toString(decode(myEncoding,false)));
    }

}
