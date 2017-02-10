/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.encoding;

import org.junit.Test;

import java.math.BigDecimal;

import static com.splicemachine.encoding.EncodingTestUtil.assertEncodeDecode;
import static org.junit.Assert.assertArrayEquals;
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
        assertArrayEquals(new byte[]{127}, BigDecimalEncoding.toBytes(BigDecimal.ZERO, true));
    }

    @Test
    public void testEncodingNumbersAroundSixBitExponentBoundary() {

        assertEncodeDecode(BigDecimal.ONE.divide(BigDecimal.TEN.pow(63)));
        assertEncodeDecode(BigDecimal.ONE.divide(BigDecimal.TEN.pow(64)));
        assertEncodeDecode(BigDecimal.ONE.divide(BigDecimal.TEN.pow(65)));

        assertArrayEquals(new byte[]{48, 62, -33, -2}, BigDecimalEncoding.toBytes(BigDecimal.ONE.divide(BigDecimal.TEN.pow(63)), true));
        assertArrayEquals(new byte[]{-49, -63, 32, 1}, BigDecimalEncoding.toBytes(BigDecimal.ONE.divide(BigDecimal.TEN.pow(63)), false));

        assertArrayEquals(new byte[]{48, 64, -33, -2}, BigDecimalEncoding.toBytes(BigDecimal.ONE.divide(BigDecimal.TEN.pow(65)), true));
        assertArrayEquals(new byte[]{-49, -65, 32, 1}, BigDecimalEncoding.toBytes(BigDecimal.ONE.divide(BigDecimal.TEN.pow(65)), false));

        assertEncodeDecode(BigDecimal.TEN.pow(62));
        assertEncodeDecode(BigDecimal.TEN.pow(63));
        assertEncodeDecode(BigDecimal.TEN.pow(64));

        assertArrayEquals(new byte[]{15, -63, -33, -2}, BigDecimalEncoding.toBytes(BigDecimal.TEN.pow(62), true));
        assertArrayEquals(new byte[]{-16, 62, 32, 1}, BigDecimalEncoding.toBytes(BigDecimal.TEN.pow(62), false));

        assertArrayEquals(new byte[]{15, -65, -33, -2}, BigDecimalEncoding.toBytes(BigDecimal.TEN.pow(64), true));
        assertArrayEquals(new byte[]{-16, 64, 32, 1}, BigDecimalEncoding.toBytes(BigDecimal.TEN.pow(64), false));
    }

    @Test
    public void toBigDecimalReturnsNullWhenOrderParameterIsWrong() {
        byte[] bytesDes = BigDecimalEncoding.toBytes(new BigDecimal("42"), true);
        byte[] bytesAsc = BigDecimalEncoding.toBytes(new BigDecimal("42"), false);
        assertNull(BigDecimalEncoding.toBigDecimal(bytesAsc, true));
        assertNull(BigDecimalEncoding.toBigDecimal(bytesDes, false));
    }

}
