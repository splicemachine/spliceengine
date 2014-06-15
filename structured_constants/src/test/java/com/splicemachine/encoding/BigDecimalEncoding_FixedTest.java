package com.splicemachine.encoding;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import static com.splicemachine.encoding.EncodingTestUtil.assertEncodeDecode;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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

    @Test
    public void testEncodingOfZeroIsAlwaysOneByte() {
        assertArrayEquals(new byte[]{-128}, BigDecimalEncoding.toBytes(BigDecimal.ZERO, false));
        assertArrayEquals(new byte[]{64}, BigDecimalEncoding.toBytes(BigDecimal.ZERO, true));
    }

    @Test
    public void testEncodingNumbersAroundSixBitExponentBoundary() {

        assertEncodeDecode(BigDecimal.ONE.divide(BigDecimal.TEN.pow(63)));
        assertEncodeDecode(BigDecimal.ONE.divide(BigDecimal.TEN.pow(64)));
        assertEncodeDecode(BigDecimal.ONE.divide(BigDecimal.TEN.pow(65)));

        assertArrayEquals(new byte[]{48, 62, -33}, BigDecimalEncoding.toBytes(BigDecimal.ONE.divide(BigDecimal.TEN.pow(63)), true));
        assertArrayEquals(new byte[]{-49, -63, 32}, BigDecimalEncoding.toBytes(BigDecimal.ONE.divide(BigDecimal.TEN.pow(63)), false));

        assertArrayEquals(new byte[]{48, 64, -33}, BigDecimalEncoding.toBytes(BigDecimal.ONE.divide(BigDecimal.TEN.pow(65)), true));
        assertArrayEquals(new byte[]{-49, -65, 32}, BigDecimalEncoding.toBytes(BigDecimal.ONE.divide(BigDecimal.TEN.pow(65)), false));

        assertEncodeDecode(BigDecimal.TEN.pow(62));
        assertEncodeDecode(BigDecimal.TEN.pow(63));
        assertEncodeDecode(BigDecimal.TEN.pow(64));

        assertArrayEquals(new byte[]{15, -63, -33}, BigDecimalEncoding.toBytes(BigDecimal.TEN.pow(62), true));
        assertArrayEquals(new byte[]{-16, 62, 32}, BigDecimalEncoding.toBytes(BigDecimal.TEN.pow(62), false));

        assertArrayEquals(new byte[]{15, -65, -33}, BigDecimalEncoding.toBytes(BigDecimal.TEN.pow(64), true));
        assertArrayEquals(new byte[]{-16, 64, 32}, BigDecimalEncoding.toBytes(BigDecimal.TEN.pow(64), false));
    }

    @Test
    public void sort_positiveValue_negativeExponents() {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal(".5"),
                new BigDecimal(".55"),
                new BigDecimal(".555"),
                new BigDecimal(".5555"),
                new BigDecimal(".55555"),
                new BigDecimal(".555555"),
                new BigDecimal(".5555555"),
                new BigDecimal(".55555555"),
                new BigDecimal(".555555555"),
                new BigDecimal(".5555555555"),
                new BigDecimal(".55555555555"),
                new BigDecimal(".555555555555"),
                new BigDecimal(".5555555555555"),
                new BigDecimal(".55555555555555"),
                new BigDecimal(".555555555555555"),
                new BigDecimal(".5555555555555555"),
                new BigDecimal(".5555555555555555555555555555555555")
        );
        List<byte[]> encodedBigDecimalsBytes = EncodingTestUtil.toBytes(bigDecimalList);
        Collections.shuffle(encodedBigDecimalsBytes);
        Collections.sort(encodedBigDecimalsBytes, Bytes.BYTES_COMPARATOR);
        assertEquals(bigDecimalList, EncodingTestUtil.toBigDecimal(encodedBigDecimalsBytes));
    }

    @Test
    public void sort_positiveValue_positiveExponents() {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal("5"),
                new BigDecimal("55"),
                new BigDecimal("555"),
                new BigDecimal("5555"),
                new BigDecimal("55555"),
                new BigDecimal("555555"),
                new BigDecimal("5555555"),
                new BigDecimal("55555555"),
                new BigDecimal("555555555"),
                new BigDecimal("5555555555"),
                new BigDecimal("55555555555"),
                new BigDecimal("555555555555"),
                new BigDecimal("5555555555555"),
                new BigDecimal("55555555555555"),
                new BigDecimal("555555555555555"),
                new BigDecimal("5555555555555555"),
                new BigDecimal("5555555555555555555555555555555555")
        );
        List<byte[]> encodedBigDecimalsBytes = EncodingTestUtil.toBytes(bigDecimalList);
        Collections.shuffle(encodedBigDecimalsBytes);
        Collections.sort(encodedBigDecimalsBytes, Bytes.BYTES_COMPARATOR);
        assertEquals(bigDecimalList, EncodingTestUtil.toBigDecimal(encodedBigDecimalsBytes));
    }

    @Test
    public void sort_negativeValue_negativeExponents() {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal("-.5555555555555555555555555555555555"),
                new BigDecimal("-.5555555555555555"),
                new BigDecimal("-.555555555555555"),
                new BigDecimal("-.55555555555555"),
                new BigDecimal("-.5555555555555"),
                new BigDecimal("-.555555555555"),
                new BigDecimal("-.55555555555"),
                new BigDecimal("-.5555555555"),
                new BigDecimal("-.555555555"),
                new BigDecimal("-.55555555"),
                new BigDecimal("-.5555555"),
                new BigDecimal("-.555555"),
                new BigDecimal("-.55555"),
                new BigDecimal("-.5555"),
                new BigDecimal("-.555"),
                new BigDecimal("-.55"),
                new BigDecimal("-.5")
        );
        List<byte[]> encodedBigDecimalsBytes = EncodingTestUtil.toBytes(bigDecimalList);
        Collections.shuffle(encodedBigDecimalsBytes);
        Collections.sort(encodedBigDecimalsBytes, Bytes.BYTES_COMPARATOR);
        assertEquals(bigDecimalList, EncodingTestUtil.toBigDecimal(encodedBigDecimalsBytes));
    }

    @Test
    public void sort_negativeValue_positiveExponents() {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal("-5555555555555555555555555555555555"),
                new BigDecimal("-5555555555555555"),
                new BigDecimal("-555555555555555"),
                new BigDecimal("-55555555555555"),
                new BigDecimal("-5555555555555"),
                new BigDecimal("-555555555555"),
                new BigDecimal("-55555555555"),
                new BigDecimal("-5555555555"),
                new BigDecimal("-555555555"),
                new BigDecimal("-55555555"),
                new BigDecimal("-5555555"),
                new BigDecimal("-555555"),
                new BigDecimal("-55555"),
                new BigDecimal("-5555"),
                new BigDecimal("-555"),
                new BigDecimal("-55"),
                new BigDecimal("-5")
        );
        List<byte[]> encodedBigDecimalsBytes = EncodingTestUtil.toBytes(bigDecimalList);
        Collections.shuffle(encodedBigDecimalsBytes);
        Collections.sort(encodedBigDecimalsBytes, Bytes.BYTES_COMPARATOR);
        assertEquals(bigDecimalList, EncodingTestUtil.toBigDecimal(encodedBigDecimalsBytes));
    }

    @Test
    public void sort_positiveValues_positiveExponents_AND_negativeExponents() {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal(".5"),
                new BigDecimal(".55"),
                new BigDecimal(".555"),
                new BigDecimal(".5555"),
                new BigDecimal(".55555"),
                new BigDecimal(".555555"),
                new BigDecimal(".5555555"),
                new BigDecimal(".55555555"),
                new BigDecimal(".555555555"),
                new BigDecimal("5"),
                new BigDecimal("55"),
                new BigDecimal("555"),
                new BigDecimal("5555"),
                new BigDecimal("55555"),
                new BigDecimal("555555"),
                new BigDecimal("5555555"),
                new BigDecimal("55555555")
        );
        List<byte[]> encodedBigDecimalsBytes = EncodingTestUtil.toBytes(bigDecimalList);
        Collections.shuffle(encodedBigDecimalsBytes);
        Collections.sort(encodedBigDecimalsBytes, Bytes.BYTES_COMPARATOR);
        assertEquals(bigDecimalList, EncodingTestUtil.toBigDecimal(encodedBigDecimalsBytes));
    }

    @Test
    public void sort_negativeValues_positiveExponents_AND_negativeExponents() {
        List<BigDecimal> bigDecimalList = Lists.newArrayList(
                new BigDecimal("-55555555"),
                new BigDecimal("-5555555"),
                new BigDecimal("-555555"),
                new BigDecimal("-55555"),
                new BigDecimal("-5555"),
                new BigDecimal("-555"),
                new BigDecimal("-55"),
                new BigDecimal("-5"),
                new BigDecimal("-.555555555"),
                new BigDecimal("-.55555555"),
                new BigDecimal("-.5555555"),
                new BigDecimal("-.555555"),
                new BigDecimal("-.55555"),
                new BigDecimal("-.5555"),
                new BigDecimal("-.555"),
                new BigDecimal("-.55"),
                new BigDecimal("-.5")
        );
        List<byte[]> encodedBigDecimalsBytes = EncodingTestUtil.toBytes(bigDecimalList);
        Collections.shuffle(encodedBigDecimalsBytes);
        Collections.sort(encodedBigDecimalsBytes, Bytes.BYTES_COMPARATOR);
        assertEquals(bigDecimalList, EncodingTestUtil.toBigDecimal(encodedBigDecimalsBytes));
    }

    @Test
    public void toBigDecimalReturnsNullWhenOrderParameterIsWrong() {
        byte[] bytesDes = BigDecimalEncoding.toBytes(new BigDecimal("42"), true);
        byte[] bytesAsc = BigDecimalEncoding.toBytes(new BigDecimal("42"), false);
        assertNull(BigDecimalEncoding.toBigDecimal(bytesAsc, true));
        assertNull(BigDecimalEncoding.toBigDecimal(bytesDes, false));
    }

}
