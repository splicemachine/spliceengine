package com.splicemachine.encoding;

import com.splicemachine.encoding.debug.BitFormat;
import org.junit.Assert;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
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
        byte[] bytes = ScalarEncoding.toBytes(-9219236770852362184L, false);
        assertEquals("[4, -128, 14, -79, 0, -91, 32, 40, 56]", Arrays.toString(bytes));
        long orig = ScalarEncoding.toLong(bytes, false);
        assertEquals(-9219236770852362184L, orig);
    }

    @Test
    public void testEncodeDecodeInteger() throws Exception {
        byte[] data = new byte[]{(byte) 0xE0, (byte) 0x47, (byte) 0x66};
        int val = Encoding.decodeInt(data);
        assertEquals(18278, val);
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
    @Ignore("DB-3421")
    public void testEncodeDecodeNegativeBigInt() throws Exception {
        byte[] test = ScalarEncoding.toBytes(-9208636019293794487l,false);
        byte[] test2 = ScalarEncoding.toBytes(-9169196554323565708l,false);
        Assert.assertTrue("encoding incorrect", Bytes.compareTo(test,test2)<0);
    }



}
