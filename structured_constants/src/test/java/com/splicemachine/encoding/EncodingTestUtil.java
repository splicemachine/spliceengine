package com.splicemachine.encoding;

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Utility methods that take numbers, serializer, deserialize, and assert result equals initial number.
 */
public class EncodingTestUtil {

    public static void assertEncodeDecode(BigDecimal bigDecimalIn) {
        byte[] bytesDes = BigDecimalEncoding.toBytes(bigDecimalIn, true);
        byte[] bytesAsc = BigDecimalEncoding.toBytes(bigDecimalIn, false);

        assertByteArrayDoesNotContainZero(bytesDes);
        assertByteArrayDoesNotContainZero(bytesAsc);

        BigDecimal decimalOutDes = BigDecimalEncoding.toBigDecimal(bytesDes, true);
        BigDecimal decimalOutAsc = BigDecimalEncoding.toBigDecimal(bytesAsc, false);

        assertTrue(decimalOutAsc.compareTo(bigDecimalIn) == 0);
        assertTrue(decimalOutDes.compareTo(bigDecimalIn) == 0);
    }

    public static void assertEncodeDecode(double numberIn) {
        byte[] bytesDes = DoubleEncoding.toBytes(numberIn, true);
        byte[] bytesAsc = DoubleEncoding.toBytes(numberIn, false);

        double numberOutDes = DoubleEncoding.toDouble(bytesDes, true);
        double numberOutAsc = DoubleEncoding.toDouble(bytesAsc, false);
        double numberOutDesWrap = DoubleEncoding.toDouble(ByteBuffer.wrap(bytesDes), true);
        double numberOutAscWrap = DoubleEncoding.toDouble(ByteBuffer.wrap(bytesAsc), false);

        assertEquals(numberIn, numberOutAsc, 1e-12);
        assertEquals(numberIn, numberOutDes, 1e-12);
        assertEquals(numberIn, numberOutDesWrap, 1e-12);
        assertEquals(numberIn, numberOutAscWrap, 1e-12);
    }

    public static void assertEncodeDecode(float numberIn) {
        byte[] bytesDes = FloatEncoding.toBytes(numberIn, true);
        byte[] bytesAsc = FloatEncoding.toBytes(numberIn, false);

        double numberOutDes = FloatEncoding.toFloat(bytesDes, true);
        double numberOutAsc = FloatEncoding.toFloat(bytesAsc, false);
        double numberOutDesWrap = FloatEncoding.toFloat(ByteBuffer.wrap(bytesDes), true);
        double numberOutAscWrap = FloatEncoding.toFloat(ByteBuffer.wrap(bytesAsc), false);

        assertEquals(numberIn, numberOutAsc, 1e-12);
        assertEquals(numberIn, numberOutDes, 1e-12);
        assertEquals(numberIn, numberOutDesWrap, 1e-12);
        assertEquals(numberIn, numberOutAscWrap, 1e-12);
    }


    public static List<byte[]> toBytes(List<BigDecimal> bigIns) {
        List<byte[]> bytesList = Lists.newArrayList();
        for (BigDecimal bigDecimal : bigIns) {
            bytesList.add(BigDecimalEncoding.toBytes(bigDecimal, false));
        }
        return bytesList;
    }

    public static List<BigDecimal> toBigDecimal(List<byte[]> bigIns) {
        List<BigDecimal> bigDecimals = Lists.newArrayList();
        for (byte[] bigDecimal : bigIns) {
            bigDecimals.add(BigDecimalEncoding.toBigDecimal(bigDecimal, false));
        }
        return bigDecimals;
    }

    public static void assertByteArrayDoesNotContainZero(byte[] bytes) {
        for(byte b : bytes) {
            assertNotEquals(0 , b);
        }
    }
}
