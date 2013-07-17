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

        ByteBuffer encodeBuffer = ByteBuffer.wrap(encoding);
        decoded = DecimalEncoding.toBigDecimal(encodeBuffer,false);
        Assert.assertTrue("Incorrect decoding. Expected <"+value+">, Actual <"+ value+">",value.compareTo(decoded)==0);
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
}
