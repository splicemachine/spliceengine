package com.splicemachine.encoding;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;

/**
 * Tests special case encoding scenarios (regression tests, etc.)
 *
 * @author Scott Fines
 * Created on: 6/13/13
 */
public class SpecialEncodingTest {

    @Test
    public void testDecodeSYS() throws Exception {
        byte[] testBytes = StringEncoding.toBytes("SYS",false);
        System.out.println(Arrays.toString(testBytes));
    }

    @Test
    public void testEncodeDecodeIntegerZero() throws Exception {
        byte[] test = ScalarEncoding.toBytes(0,false);
        int retVal = ScalarEncoding.getInt(test,false);

        Assert.assertEquals(0,retVal);
    }

    @Test
    public void testEncodeNegativeBigDecimal() throws Exception {
        BigDecimal value = new BigDecimal("-4440.3232");
        byte[] test = DecimalEncoding.toBytes(value,false);
        BigDecimal ret = DecimalEncoding.toBigDecimal(test,false);
        Assert.assertTrue(value.subtract(ret).compareTo(BigDecimal.valueOf(Math.pow(10,-6)))<0);

    }


}
