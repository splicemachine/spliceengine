/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.splicemachine.primitives.Bytes;
import org.junit.Assert;
import org.junit.Test;
import java.util.Arrays;
import static org.junit.Assert.assertEquals;

/*
 * Test byte/short/int/long with specific (a.k.a fixed) values.
 */
public class ScalarEncoding_FixedTest {

    private BitFormat bitFormat = new BitFormat(false);

    @Test
    public void testLongBoundaries() {
        EncodingTestUtil.assertEncodeDecode(Long.MIN_VALUE);
        EncodingTestUtil.assertEncodeDecode(0L);
        EncodingTestUtil.assertEncodeDecode(Long.MAX_VALUE);
    }

    @Test
    public void testIntegerBoundaries() {
        EncodingTestUtil.assertEncodeDecode(Integer.MIN_VALUE);
        EncodingTestUtil.assertEncodeDecode(0);
        EncodingTestUtil.assertEncodeDecode(Integer.MAX_VALUE);
    }

    @Test
    public void testLongWithEncodingThatContainsZeroByte() {
        long actual=-9219236770852362184L;
        byte[] bytes = ScalarEncoding.writeLong(actual,false);
//        assertEquals("[4, -128, 14, -79, 0, -91, 32, 40, 56]", Arrays.toString(bytes));
        long orig = ScalarEncoding.readLong(bytes,false);
        assertEquals(actual, orig);
    }

    @Test
    public void testEncodeDecodeInteger() throws Exception {
        int actual = 18278;
//        byte[] data = new byte[]{(byte) 0xE0, (byte) 0x47, (byte) 0x66};
        byte[] data = ScalarEncoding.writeLong(actual,false);
        int decoded = ScalarEncoding.readInt(data,false);
        assertEquals(actual, decoded);
    }

    @Test
    public void testUnsignedComparisonsMakeSense() throws Exception {
        byte[][] dataElements = new byte[256][];
        for (int i = 0; i < 256; i++) {
            dataElements[i] = new byte[]{-47, (byte) i};
            try {
                Encoding.decodeInt(dataElements[i]);
            } catch (Exception e) {
                System.out.println("byte [] " + Arrays.toString(dataElements[i]) + " is not a valid scalar");
            }
        }
    }

    @Test
    public void testLong() {
        long minLong = Long.MAX_VALUE | Long.MIN_VALUE;
        assertEquals("11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111", bitFormat.format(minLong));

        assertEquals("[-1, -8, 0, 0, 0, 0, 0, 1]", Arrays.toString(DoubleEncoding.toBytes(Double.longBitsToDouble(minLong), false)));

        minLong ^= (minLong >>> 11);
        assertEquals("11111111 11100000 00000000 00000000 00000000 00000000 00000000 00000000", bitFormat.format(minLong));
        assertEquals("[0, 32, 0, 0, 0, 0, 0, 0]", Arrays.toString(DoubleEncoding.toBytes(Double.longBitsToDouble(minLong), false)));

        minLong &= (minLong >>> 8);
        minLong |= Long.MIN_VALUE >> 2;
        minLong &= Long.MIN_VALUE >> 2;
        assertEquals("11100000 00000000 00000000 00000000 00000000 00000000 00000000 00000000", bitFormat.format(minLong));
        assertEquals("[32, 0, 0, 0, 0, 0, 0, 0]", Arrays.toString(DoubleEncoding.toBytes(Double.longBitsToDouble(minLong), false)));

        minLong &= (minLong >>> 8);
        minLong |= Long.MIN_VALUE >> 2;
        minLong &= Long.MIN_VALUE >> 2;
        assertEquals("11100000 00000000 00000000 00000000 00000000 00000000 00000000 00000000", bitFormat.format(minLong));
        assertEquals("[32, 0, 0, 0, 0, 0, 0, 0]", Arrays.toString(DoubleEncoding.toBytes(Double.longBitsToDouble(minLong), false)));
    }

    @Test
    public void testInt() {
        int i = Integer.MAX_VALUE | Integer.MIN_VALUE;
        assertEquals("11111111 11111111 11111111 11111111", bitFormat.format(i));
        assertEquals("[-1, -64, 0, 1]", Arrays.toString(FloatEncoding.toBytes(Float.intBitsToFloat(i), false)));
        i ^= (i >>> 9);
        assertEquals("11111111 10000000 00000000 00000000", bitFormat.format(i));
        assertEquals("[0, -128, 0, 0]", Arrays.toString(FloatEncoding.toBytes(Float.intBitsToFloat(i), false)));
    }

    @Test
    public void testCanEncodeAndDecodeSpecificNumbers() throws Exception{
        /*
         * Tests related to DB-3421, to ensure that the numbers themselves encode and decode properly
         */
        long num = -9208636019293794487l;
        byte[] encoded = ScalarEncoding.writeLong(num,false);
        long decoded = ScalarEncoding.readLong(encoded,false);
        Assert.assertEquals("Decoded value does not match actual!",num,decoded);

        num = -9169196554323565708l;
        encoded = ScalarEncoding.writeLong(num,false);
        decoded = ScalarEncoding.readLong(encoded,false);
        Assert.assertEquals("Decoded value does not match actual!",num,decoded);
    }

    @Test
    public void testCanOrderSpecificLargeNegativeNumbers() throws Exception {
        /*
         * Regression test for DB-3421
         */
        byte[] test = ScalarEncoding.writeLong(-9208636019293794487l,false);
        byte[] test2 = ScalarEncoding.writeLong(-9169196554323565708l,false);
        Assert.assertTrue("incorrect sort order!", Bytes.BASE_COMPARATOR.compare(test,test2)<0);
    }

}
