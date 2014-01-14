package com.splicemachine.encoding;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author Scott Fines
 *         Created on: 6/18/13
 */
public class FixedDecimalEncodingTest {

    @Test
    public void testCanEncodeDecodeByteSpecificByteBuffersCorrectly() throws Exception {
        BigDecimal value = new BigDecimal("37661026");
        byte[] encoding = DecimalEncoding.toBytes(value,false);
        BigDecimal decoded = DecimalEncoding.toBigDecimal(encoding,false);
        Assert.assertTrue("Incorrect decoding. Expected <"+value+">, Actual <"+ value+">",value.compareTo(decoded)==0);

    }

    @Test
    public void testCanEncodedOnlyDecimalDescending() {
        /*
         * Regression test to ensure this specific number decodes properly.
         */
        BigDecimal value = new BigDecimal("0.02308185311033106312805784909869544208049774169921875");
        byte[] encoding = DecimalEncoding.toBytes(value, true);
        BigDecimal decoded = DecimalEncoding.toBigDecimal(encoding,true);
        Assert.assertTrue("Incorrect decoding. Expected <" + value + ">, Actual <" + value + ">", value.compareTo(decoded) == 0);
    }

    @Test
    @Ignore("not actually a test")
    public void testEncodeAsDoubleDecodeAsFloat() throws Exception {
        double val = 1.0;

        byte[] encoded = DecimalEncoding.toBytes(val,false);

        float decodedFloat = DecimalEncoding.toFloat(encoded,false);
        double decodedDouble = DecimalEncoding.toDouble(encoded,false);
        System.out.printf("%f,%f%n",decodedFloat,decodedDouble);

        float v2= 1.0f;

        byte[] encoded2 = DecimalEncoding.toBytes(v2, false);
//        System.out.println(Arrays.toString(encoded2));
//        System.out.println(Arrays.toString(encoded));
    }

    @Test
    @Ignore("not actually a test")
    public void testEncodeBigDecimalZeros() throws Exception {
        BigDecimal decimal = new BigDecimal("00000000000000.012340004");

        byte[] data = Encoding.encode(decimal);
//        System.out.println(Arrays.toString(data));

    }

    @Test
    public void testCanGetAllZerosDouble() throws Exception {
        double a = 0;

        System.out.println(pad(Double.doubleToLongBits(a)));
        byte[] data = DecimalEncoding.toBytes(a,false);
        System.out.println(Arrays.toString(data));

        long l = Long.MAX_VALUE | Long.MIN_VALUE;
        System.out.println(pad(l));
        System.out.println(Arrays.toString(DecimalEncoding.toBytes(Double.longBitsToDouble(l), false)));
        System.out.println(Double.longBitsToDouble(l));
        l ^=(l>>>11);
        System.out.println(pad(l));
        System.out.println(Arrays.toString(DecimalEncoding.toBytes(Double.longBitsToDouble(l), false)));
        System.out.println(Double.longBitsToDouble(l));
        l &=(l>>>8);
        l |= Long.MIN_VALUE>>2;
        l &= Long.MIN_VALUE>>2;
        System.out.println(pad(l));
        System.out.println(Arrays.toString(DecimalEncoding.toBytes(Double.longBitsToDouble(l), false)));
        System.out.println(Double.longBitsToDouble(l));
        l &=(l>>>8);
        l |= Long.MIN_VALUE>>2;
        l &= Long.MIN_VALUE>>2;
        System.out.println(pad(l));
        System.out.println(Arrays.toString(DecimalEncoding.toBytes(Double.longBitsToDouble(l),false)));
        System.out.println(Double.longBitsToDouble(l));

        int i = Integer.MAX_VALUE | Integer.MIN_VALUE;
        System.out.println(pad(i));
        System.out.println(Arrays.toString(DecimalEncoding.toBytes(Float.intBitsToFloat(i),false)));
        System.out.println(Float.intBitsToFloat(i));
        i ^= (i>>>9);
        System.out.println(pad(i));
        System.out.println(Arrays.toString(DecimalEncoding.toBytes(Float.intBitsToFloat(i),false)));
        System.out.println(Float.intBitsToFloat(i));
    }

    @Test
    public void testEncodeDecodeBytesAsZeros() throws Exception {
        for(int i=Byte.MIN_VALUE;i<=Byte.MAX_VALUE;i++){
            System.out.printf("%d,%s%n",i,Arrays.toString(Encoding.encode(i)));
        }

    }

    private static String pad(int number){
        String num = Integer.toBinaryString(number);
        char[] zeros = new char[Integer.numberOfLeadingZeros(number)];
        for(int i=0;i<zeros.length;i++){
            zeros[i] = '0';
        }
        return new String(zeros)+num;
    }

    private static String pad(long number){
        String num = Long.toBinaryString(number);
        char[] zeros = new char[Long.numberOfLeadingZeros(number)];
        for(int i=0;i<zeros.length;i++){
            zeros[i] = '0';
        }
        return new String(zeros)+num;
    }
}
