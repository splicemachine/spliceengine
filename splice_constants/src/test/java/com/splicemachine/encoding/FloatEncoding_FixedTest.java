package com.splicemachine.encoding;

import com.splicemachine.encoding.debug.BitFormat;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class FloatEncoding_FixedTest {

    private BitFormat bitFormat = new BitFormat(false);

    /* Note that the encodings of positive and negative zero both contain multiple occurrences of 0x0. */
    @Test
    public void testEncodingOfPositiveAndNegativeZero() throws Exception {
        float zero = 0.0f;
        float negativeZero = -0.0f;

        // IEEE 754 encoding
        assertEquals("00000000 00000000 00000000 00000000", bitFormat.format(Float.floatToIntBits(zero)));
        assertEquals("10000000 00000000 00000000 00000000", bitFormat.format(Float.floatToIntBits(negativeZero)));

        // splice encoding
        assertEquals("[-128, 0, 0, 1]", Arrays.toString(FloatEncoding.toBytes(zero, false)));
        assertEquals("[-128, 0, 0, 0]", Arrays.toString(FloatEncoding.toBytes(negativeZero, false)));
        assertEquals("[127, -1, -1, -2]", Arrays.toString(FloatEncoding.toBytes(zero, true)));
        assertEquals("[127, -1, -1, -1]", Arrays.toString(FloatEncoding.toBytes(negativeZero, true)));
    }

}