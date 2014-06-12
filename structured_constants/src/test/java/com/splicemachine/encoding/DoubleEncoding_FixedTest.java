package com.splicemachine.encoding;

import com.splicemachine.testutil.BitFormat;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/*
 * Test DoubleEncoding with specific (aka fixed) values.
 */
public class DoubleEncoding_FixedTest {

    @Test
    public void testCanGetAllZerosDouble() throws Exception {
        double zero = 0.0;

        assertEquals("00000000000000000000000000000000000000000000000000000000000000000", BitFormat.pad(Double.doubleToLongBits(zero)));
        byte[] data = DoubleEncoding.toBytes(zero, false);
        assertEquals("[-128, 0, 0, 0, 0, 0, 0, 1]", Arrays.toString(data));
    }

}
