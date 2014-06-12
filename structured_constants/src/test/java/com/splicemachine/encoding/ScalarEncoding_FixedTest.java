package com.splicemachine.encoding;

import com.splicemachine.testutil.BitFormat;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/*
 * Test byte/short/int/long with specific (a.k.a fixed) values.
 */
public class ScalarEncoding_FixedTest {

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
        assertEquals("1111111111111111111111111111111111111111111111111111111111111111", BitFormat.pad(minLong));

        assertEquals("[-1, -8, 0, 0, 0, 0, 0, 1]", Arrays.toString(DoubleEncoding.toBytes(Double.longBitsToDouble(minLong), false)));

        minLong ^= (minLong >>> 11);
        assertEquals("1111111111100000000000000000000000000000000000000000000000000000", BitFormat.pad(minLong));
        assertEquals("[0, 32, 0, 0, 0, 0, 0, 0]", Arrays.toString(DoubleEncoding.toBytes(Double.longBitsToDouble(minLong), false)));

        minLong &= (minLong >>> 8);
        minLong |= Long.MIN_VALUE >> 2;
        minLong &= Long.MIN_VALUE >> 2;
        assertEquals("1110000000000000000000000000000000000000000000000000000000000000", BitFormat.pad(minLong));
        assertEquals("[32, 0, 0, 0, 0, 0, 0, 0]", Arrays.toString(DoubleEncoding.toBytes(Double.longBitsToDouble(minLong), false)));

        minLong &= (minLong >>> 8);
        minLong |= Long.MIN_VALUE >> 2;
        minLong &= Long.MIN_VALUE >> 2;
        assertEquals("1110000000000000000000000000000000000000000000000000000000000000", BitFormat.pad(minLong));
        assertEquals("[32, 0, 0, 0, 0, 0, 0, 0]", Arrays.toString(DoubleEncoding.toBytes(Double.longBitsToDouble(minLong), false)));
    }

    @Test
    public void testInt() {
        int i = Integer.MAX_VALUE | Integer.MIN_VALUE;
        assertEquals("11111111111111111111111111111111", BitFormat.pad(i));
        assertEquals("[-1, -64, 0, 1]", Arrays.toString(FloatEncoding.toBytes(Float.intBitsToFloat(i), false)));
        i ^= (i >>> 9);
        assertEquals("11111111100000000000000000000000", BitFormat.pad(i));
        assertEquals("[0, -128, 0, 0]", Arrays.toString(FloatEncoding.toBytes(Float.intBitsToFloat(i), false)));
    }

    @Test
    public void testEncodeDecodeIntegerZero() throws Exception {
        byte[] test = ScalarEncoding.toBytes(0, false);
        int retVal = ScalarEncoding.getInt(test, false);
        assertEquals(0, retVal);
    }

}
