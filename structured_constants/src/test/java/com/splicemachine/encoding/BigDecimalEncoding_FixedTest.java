package com.splicemachine.encoding;

import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertTrue;

/*
 * Test BigDecimalEncoding with specific (aka fixed) values.
 */
public class BigDecimalEncoding_FixedTest {

    @Test
    public void testCanEncodeDecodeByteSpecificByteBuffersCorrectly() throws Exception {
        BigDecimal value = new BigDecimal("37661026");
        byte[] encoding = BigDecimalEncoding.toBytes(value, false);
        BigDecimal decoded = BigDecimalEncoding.toBigDecimal(encoding, false);
        assertTrue("Incorrect decoding. Expected <" + value + ">, Actual <" + value + ">", value.compareTo(decoded) == 0);
    }

    @Test
    public void testCanEncodedOnlyDecimalDescending() {
        /* Regression test to ensure this specific number decodes properly.  */
        BigDecimal value = new BigDecimal("0.02308185311033106312805784909869544208049774169921875");
        byte[] encoding = BigDecimalEncoding.toBytes(value, true);
        BigDecimal decoded = BigDecimalEncoding.toBigDecimal(encoding, true);
        assertTrue("Incorrect decoding. Expected <" + value + ">, Actual <" + value + ">", value.compareTo(decoded) == 0);
    }

    @Test
    public void testEncodeBigDecimalZeros() throws Exception {
        BigDecimal bigDecimalIn = new BigDecimal("00000000000000.012340004");
        byte[] data = Encoding.encode(bigDecimalIn);
        BigDecimal bigDecimalOut = Encoding.decodeBigDecimal(data);
        assertTrue(bigDecimalIn.compareTo(bigDecimalOut) == 0);
    }

}
