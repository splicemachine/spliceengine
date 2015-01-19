package com.splicemachine.encoding;

import com.splicemachine.encoding.debug.BitFormat;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/*
 * Test DoubleEncoding with specific (aka fixed) values.
 */
public class DoubleEncoding_FixedTest {

    private BitFormat bitFormat = new BitFormat(false);

    /* Note that the encodings of positive and negative zero both contain multiple occurrences of 0x0. */
    @Test
    public void testEncodingOfPositiveAndNegativeZero() throws Exception {
        double zero = 0.0;
        double negativeZero = -0.0;

        // IEEE 754 encoding
        assertEquals("00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000", bitFormat.format(Double.doubleToLongBits(zero)));
        assertEquals("10000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000", bitFormat.format(Double.doubleToLongBits(negativeZero)));

        // splice encoding
        assertEquals("[-128, 0, 0, 0, 0, 0, 0, 1]", Arrays.toString(DoubleEncoding.toBytes(zero, false)));
        assertEquals("[-128, 0, 0, 0, 0, 0, 0, 0]", Arrays.toString(DoubleEncoding.toBytes(negativeZero, false)));
        assertEquals("[127, -1, -1, -1, -1, -1, -1, -2]", Arrays.toString(DoubleEncoding.toBytes(zero, true)));
        assertEquals("[127, -1, -1, -1, -1, -1, -1, -1]", Arrays.toString(DoubleEncoding.toBytes(negativeZero, true)));
    }

}
