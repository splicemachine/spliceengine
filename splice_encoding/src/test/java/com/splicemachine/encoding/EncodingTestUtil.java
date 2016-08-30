/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.encoding;

import org.sparkproject.guava.collect.Lists;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Utility methods that take numbers, serialize, deserialize, and assert result equals initial number.
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

    public static void assertEncodeDecode(int numberIn) {
        byte[] bytesDes = ScalarEncoding.writeLong(numberIn,true);
        byte[] bytesAsc = ScalarEncoding.writeLong(numberIn,false);

        int numberOutDes = ScalarEncoding.readInt(bytesDes,true);
        int numberOutAsc = ScalarEncoding.readInt(bytesAsc,false);

        assertEquals(numberIn, numberOutAsc);
        assertEquals(numberIn, numberOutDes);
    }

    public static void assertEncodeDecode(long numberIn) {
        byte[] bytesDes = ScalarEncoding.writeLong(numberIn,true);
        byte[] bytesAsc = ScalarEncoding.writeLong(numberIn,false);

        long numberOutDes = ScalarEncoding.readLong(bytesDes,true);
        long numberOutAsc = ScalarEncoding.readLong(bytesAsc,false);

        assertEquals(numberIn, numberOutAsc);
        assertEquals(numberIn, numberOutDes);
    }

    public static List<byte[]> toBytes(List<BigDecimal> bigIns, boolean desc) {
        List<byte[]> bytesList = Lists.newArrayList();
        for (BigDecimal bigDecimal : bigIns) {
            bytesList.add(BigDecimalEncoding.toBytes(bigDecimal, desc));
        }
        return bytesList;
    }

    public static List<BigDecimal> toBigDecimal(List<byte[]> bigIns, boolean desc) {
        List<BigDecimal> bigDecimals = Lists.newArrayList();
        for (byte[] bigDecimal : bigIns) {
            bigDecimals.add(BigDecimalEncoding.toBigDecimal(bigDecimal, desc));
        }
        return bigDecimals;
    }

    public static void assertByteArrayDoesNotContainZero(byte[] bytes) {
        for(byte b : bytes) {
            assertNotEquals(0 , b);
        }
    }
}
