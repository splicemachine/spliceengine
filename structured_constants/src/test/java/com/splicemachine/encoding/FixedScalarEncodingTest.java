package com.splicemachine.encoding;

import org.junit.Test;

import java.util.Arrays;

/**
 * @author Scott Fines
 *         Created on: 7/11/13
 */
public class FixedScalarEncodingTest {

    @Test
    public void testEncodeDecodeInteger() throws Exception {
        byte[] data = new byte[]{(byte)0xE0,(byte)0x47,(byte)0x66};
//        byte[] data = new byte[]{(byte)0xB4};
        int val = Encoding.decodeInt(data);
        System.out.println(val);

//        data = new byte[]{(byte)0x82};
//        val = Encoding.decodeInt(data);
//        System.out.println(val);
    }

    @Test
    public void testUnsignedComparisonsMakeSense() throws Exception {
        byte[][] dataElements = new byte[256][];
        for(int i=0;i<256;i++){
            dataElements[i] = new byte[]{-47,(byte)i};
            try{
                Encoding.decodeInt(dataElements[i]);
            }catch(Exception e){
                System.out.println("byte [] "+ Arrays.toString(dataElements[i])+ " is not a valid scalar");
            }
        }

//        System.out.println(Encoding.decodeInt(new byte[]{-47,0}));
    }

}
