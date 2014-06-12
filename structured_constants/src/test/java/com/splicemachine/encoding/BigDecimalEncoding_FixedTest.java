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
        assertEncodeDecode(new BigDecimal("37661026"));
        assertEncodeDecode(new BigDecimal("-4440.3232"));
    }

    @Test
    public void testCanEncodedOnlyDecimalDescending() {
        /* Regression test to ensure this specific number decodes properly.  */
        assertEncodeDecode(new BigDecimal("0.02308185311033106312805784909869544208049774169921875"));
    }

    @Test
    public void testEncodeBigDecimalZeros() throws Exception {
        assertEncodeDecode(new BigDecimal("00000000000000.012340004"));
    }

    @Test
    public void testNearZero() {
        assertEncodeDecode(new BigDecimal("0"));
        assertEncodeDecode(new BigDecimal("-1"));
        assertEncodeDecode(new BigDecimal("1"));
        assertEncodeDecode(new BigDecimal("1e100"));
        assertEncodeDecode(new BigDecimal("0.0"));
        assertEncodeDecode(new BigDecimal("-1e-100"));
        assertEncodeDecode(new BigDecimal("-1.0"));
        assertEncodeDecode(new BigDecimal("1.0"));
    }

    @Test
    public void testJavaBoundary() {
        assertEncodeDecode(new BigDecimal(String.valueOf(Byte.MIN_VALUE)));
        assertEncodeDecode(new BigDecimal(String.valueOf(Byte.MAX_VALUE)));
        assertEncodeDecode(new BigDecimal(String.valueOf(Integer.MIN_VALUE)));
        assertEncodeDecode(new BigDecimal(String.valueOf(Integer.MAX_VALUE)));
        assertEncodeDecode(new BigDecimal(String.valueOf(Long.MIN_VALUE)));
        assertEncodeDecode(new BigDecimal(String.valueOf(Long.MAX_VALUE)));
        assertEncodeDecode(new BigDecimal(String.valueOf(Float.MIN_VALUE)));
        assertEncodeDecode(new BigDecimal(String.valueOf(Float.MAX_VALUE)));
        assertEncodeDecode(new BigDecimal(String.valueOf(Double.MIN_VALUE)));
        assertEncodeDecode(new BigDecimal(String.valueOf(Double.MAX_VALUE)));
    }

    @Test
    public void testDerbyDecimalMinAndMaxes() {
        assertEncodeDecode(new BigDecimal("1.79769E+308"));
        assertEncodeDecode(new BigDecimal("-1.79769E+308"));
    }

    private void assertEncodeDecode(BigDecimal bigDecimalIn) {
        byte[] bytesDes = BigDecimalEncoding.toBytes(bigDecimalIn, true);
        byte[] bytesAsc = BigDecimalEncoding.toBytes(bigDecimalIn, false);

        BigDecimal decimalOutDes = BigDecimalEncoding.toBigDecimal(bytesDes, true);
        BigDecimal decimalOutAsc = BigDecimalEncoding.toBigDecimal(bytesAsc, false);

        assertTrue(decimalOutAsc.compareTo(bigDecimalIn) == 0);
        assertTrue(decimalOutDes.compareTo(bigDecimalIn) == 0);
    }

}
